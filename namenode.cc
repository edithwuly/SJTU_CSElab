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
}

list<NameNode::LocatedBlock> NameNode::GetBlockLocations(yfs_client::inum ino) {
  printf("namenode\tGetBlockLocations\tino:%d\n",ino);fflush(stdout);
  list<blockid_t> block_ids;
  ec->get_block_ids(ino, block_ids);

  list<LocatedBlock> locatedBlocks;

  yfs_client::fileinfo info;
  Getfile(ino,info);
  unsigned long long size = info.size;

  for (unsigned int i=0;i<block_ids.size();i++)
  {
    if (size > BLOCK_SIZE)
      locatedBlocks.push_back(NameNode::LocatedBlock(block_ids.front(),i,BLOCK_SIZE,master_datanode));
    else
      locatedBlocks.push_back(NameNode::LocatedBlock(block_ids.front(),i,size,master_datanode));

    block_ids.pop_front();
    size -= BLOCK_SIZE;
  }

  return locatedBlocks;
}

bool NameNode::Complete(yfs_client::inum ino, uint32_t new_size) {
  printf("namenode\tComplete\tino:%d\n",ino);fflush(stdout);
  if (ec->complete(ino,new_size) == extent_protocol::OK)
  {
    if (lc->release(ino) == lock_protocol::OK)
      return true;
  }

  return false;
}

NameNode::LocatedBlock NameNode::AppendBlock(yfs_client::inum ino) {
  printf("namenode\tAppendBlock\tino:%d\n",ino);fflush(stdout);

  blockid_t bid;

  ec->append_block(ino, bid);

  yfs_client::fileinfo info;
  Getfile(ino,info);
  unsigned long long bnum = (info.size+BLOCK_SIZE-1) / BLOCK_SIZE;

  return NameNode::LocatedBlock(bid,bnum+1,0,master_datanode);
}

bool NameNode::Rename(yfs_client::inum src_dir_ino, string src_name, yfs_client::inum dst_dir_ino, string dst_name) {
  return false;
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
  printf("namenode\tCreate\tparent:%d\tname:%s\n",parent,name.c_str());fflush(stdout);

  if (ec->create(extent_protocol::T_FILE,ino_out) != extent_protocol::OK)
    return false;

  printf("namenode\tacquire lock:%d\n",ino_out);fflush(stdout);
  lc->acquire(ino_out);

  std::string buf;
  
  ec->get(parent,buf);

  buf.append('/'+std::string(name)+'/'+filename(ino_out));

  lc->acquire(parent);
  printf("buf:%s",buf.c_str());fflush(stdout);
  if (ec->put(parent,buf) != extent_protocol::OK)
    printf("1");
  lc->release(parent);

  printf("create return");fflush(stdout);
  return true;
}

bool NameNode::Isfile(yfs_client::inum ino) {
  return yfs->isfile(ino);
}

bool NameNode::Isdir(yfs_client::inum ino) {
  return yfs->isdir(ino);
}

bool NameNode::Getfile(yfs_client::inum ino, yfs_client::fileinfo &info) {
  printf("namenode\tGetfile\tino:%d\n",ino);fflush(stdout);
  lc->release(ino);
  if (yfs->getfile(ino,info) == 0)
  {
    printf("size:%d",info.size);fflush(stdout);
    lc->acquire(ino);
    return true;
  }

  return false;
}

bool NameNode::Getdir(yfs_client::inum ino, yfs_client::dirinfo &info) {
  printf("namenode\tGetdir\tino:%d\n",ino);fflush(stdout);
  if (yfs->getdir(ino,info) == 0)
    return true;

  return false;
}

bool NameNode::Readdir(yfs_client::inum ino, std::list<yfs_client::dirent> &dir) {
  printf("namenode\tReaddir\tino:%d\n",ino);fflush(stdout);

  if (yfs->readdir(ino,dir) == 0)
    return true;

  return false;
}

bool NameNode::Unlink(yfs_client::inum parent, string name, yfs_client::inum ino) {
  if (yfs->unlink(parent,name.c_str()) == 0)
    return true;

  return false;
}

void NameNode::DatanodeHeartbeat(DatanodeIDProto id) {
}

void NameNode::RegisterDatanode(DatanodeIDProto id) {
}

list<DatanodeIDProto> NameNode::GetDatanodes() {
  return list<DatanodeIDProto>();
}
