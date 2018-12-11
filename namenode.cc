#include "namenode.h"
#include "extent_client.h"
#include "lock_client.h"
#include <sys/stat.h>
#include <unistd.h>
#include "threader.h"

using namespace std;

yfs_client::inum n2i(std::string n)
{
    istringstream ist(n);
    unsigned long long finum;
    ist >> finum;
    return finum;
}

string filename(yfs_client::inum inum)
{
    ostringstream ost;
    ost << inum;
    return ost.str();
}

void NameNode::init(const string &extent_dst, const string &lock_dst) {
  ec = new extent_client(extent_dst);
  lc = new lock_client_cache(lock_dst);
  yfs = new yfs_client(extent_dst, lock_dst);

  /* Add your init logic here */
  NewThread(this, &NameNode::CheckLiveness);
}

list<NameNode::LocatedBlock> NameNode::GetBlockLocations(yfs_client::inum ino) {
  list<blockid_t> block_ids;
  list<LocatedBlock> locatedBlocks;

  lc->acquire(ino);

  yfs_client::fileinfo info;
  Getfile(ino,info);
  unsigned int size = info.size;  

  ec->get_block_ids(ino, block_ids);

  int len = block_ids.size();
  for (int i=0;i<len;i++)
  {
    if (size > BLOCK_SIZE)
      locatedBlocks.push_back(NameNode::LocatedBlock(block_ids.front(),i*BLOCK_SIZE,BLOCK_SIZE,GetDatanodes()));
    else
      locatedBlocks.push_back(NameNode::LocatedBlock(block_ids.front(),i*BLOCK_SIZE,size,GetDatanodes()));

    block_ids.pop_front();
    size -= BLOCK_SIZE;
  }

  lc->release(ino);
  return locatedBlocks;
}

bool NameNode::Complete(yfs_client::inum ino, uint32_t new_size) {
  if (ec->complete(ino,new_size) == extent_protocol::OK)
  {
    list<blockid_t> block_ids;
    ec->get_block_ids(ino, block_ids);

    for(list<blockid_t>::iterator it=block_ids.begin();it!=block_ids.end();it++)
      overwrites.push_back(*it);

    if (lc->release(ino) == lock_protocol::OK)
      return true;
  }

  return false;
}

NameNode::LocatedBlock NameNode::AppendBlock(yfs_client::inum ino) {
  blockid_t bid;

  ec->append_block(ino, bid);

  extent_protocol::attr a;
  ec->getattr(ino, a);

  unsigned long long bnum = (a.size+BLOCK_SIZE-1) / BLOCK_SIZE;

  return NameNode::LocatedBlock(bid,bnum+1,BLOCK_SIZE,GetDatanodes());
}

bool NameNode::Rename(yfs_client::inum src_dir_ino, string src_name, yfs_client::inum dst_dir_ino, string dst_name) {
  yfs_client::inum ino_out;
  bool found;
  string buf;

  yfs->lookup(src_dir_ino, src_name.c_str(), found, ino_out);

  ec->get(src_dir_ino, buf);

  string entry = '/'+src_name+'/'+filename(ino_out);
  int first = buf.find(entry);

  buf.replace(first,entry.size(),"");

  ec->put(src_dir_ino, buf);
  
  ec->get(dst_dir_ino, buf);    

  buf.append('/'+dst_name+'/'+filename(ino_out));
  ec->put(dst_dir_ino, buf);

  return true;
}

bool NameNode::Mkdir(yfs_client::inum parent, string name, mode_t mode, yfs_client::inum &ino_out) {
  if (ec->create(extent_protocol::T_DIR,ino_out) == extent_protocol::OK)
  {
    std::string buf;
    ec->get(parent,buf);
    buf.append('/'+std::string(name)+'/'+filename(ino_out));
    ec->put(parent,buf);
    return true;
  }

  return false;
}

bool NameNode::Create(yfs_client::inum parent, string name, mode_t mode, yfs_client::inum &ino_out) {
  if (ec->create(extent_protocol::T_FILE,ino_out) != extent_protocol::OK)
    return false;

  lc->acquire(ino_out);

  std::string buf;
  
  ec->get(parent,buf);

  buf.append('/'+std::string(name)+'/'+filename(ino_out));

  lc->acquire(parent);
  ec->put(parent,buf);

  lc->release(parent);

  return true;
}

bool NameNode::Isfile(yfs_client::inum ino) {
  return yfs->isfile(ino);
}

bool NameNode::Isdir(yfs_client::inum ino) {
  return yfs->isdir(ino);
}

bool NameNode::Getfile(yfs_client::inum ino, yfs_client::fileinfo &info) {
  if (yfs->getfile(ino,info) == 0)
    return true;

  return false;
}

bool NameNode::Getdir(yfs_client::inum ino, yfs_client::dirinfo &info) {
  if (yfs->getdir(ino,info) == 0)
    return true;

  return false;
}

bool NameNode::Readdir(yfs_client::inum ino, std::list<yfs_client::dirent> &dir) {
  if (yfs->readdir(ino,dir) == 0)
    return true;

  return false;
}

bool NameNode::Unlink(yfs_client::inum parent, string name, yfs_client::inum ino) {
  lc->release(parent);
  if (yfs->unlink(parent,name.c_str()) == 0)
  {
    lc->acquire(parent);
    return true;
  }

  return false;
}

void NameNode::DatanodeHeartbeat(DatanodeIDProto id) {
  switch (datanodes.find(id)->second.state)
  {
    case dn_DEAD:
    {
      for(list<blockid_t>::iterator it=overwrites.begin();it!=overwrites.end();it++)
      {
        blockid_t bid = *it;
        ReplicateBlock(bid, master_datanode, id);
      }
      datanodes.find(id)->second.state = dn_NORMAL;
      datanodes.find(id)->second.last = time(NULL); 
      break;  
    }
    case dn_RECOVERY:
    {
      for(list<blockid_t>::iterator it=overwrites.begin();it!=overwrites.end();it++)
      {
        blockid_t bid = *it;
        ReplicateBlock(bid, master_datanode, id);
      }
      datanodes.find(id)->second.state = dn_NORMAL;
      datanodes.find(id)->second.last = time(NULL);
      break;
    }
    case dn_NORMAL:
      datanodes.find(id)->second.last = time(NULL);
  }
}

void NameNode::RegisterDatanode(DatanodeIDProto id) {
  datanodes.insert(std::pair<DatanodeIDProto, State>(id, State(time(NULL),dn_RECOVERY)));
}

list<DatanodeIDProto> NameNode::GetDatanodes() {
  list<DatanodeIDProto> result;

  for(std::map<DatanodeIDProto, State>::iterator it=datanodes.begin();it!=datanodes.end();it++)
    if(it->second.state == dn_NORMAL)
      result.push_back(it->first);

  return result;
}

void  
NameNode::CheckLiveness(){
  while(true)
  {
    for(std::map<DatanodeIDProto, State>::iterator it=datanodes.begin();it!=datanodes.end();it++)
      if (difftime(time(NULL), it->second.last)>=5)
	it->second.state = dn_DEAD;
    sleep(1);   
  }
}
