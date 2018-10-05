// yfs client.  implements FS operations using extent and lock server

#include "yfs_client.h"
#include "extent_client.h"
#include <sstream>
#include <iostream>
#include <stdio.h>
#include <unistd.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>

<<<<<<< HEAD
=======
using namespace std;

yfs_client::yfs_client()
{
    ec = new extent_client();

}

>>>>>>> lab1
yfs_client::yfs_client(std::string extent_dst, std::string lock_dst)
{
  ec = new extent_client(extent_dst);
  lc = new lock_client(lock_dst);
  if (ec->put(1, "") != extent_protocol::OK)
      printf("error init root dir\n"); // XYB: init root dir
}


yfs_client::inum
yfs_client::n2i(std::string n)
{
    std::istringstream ist(n);
    unsigned long long finum;
    ist >> finum;
    return finum;
}

std::string
yfs_client::filename(inum inum)
{
    std::ostringstream ost;
    ost << inum;
    return ost.str();
}

bool
yfs_client::isfile(inum inum)
{
    extent_protocol::attr a;

    if (ec->getattr(inum, a) != extent_protocol::OK) {
        printf("error getting attr\n");
        return false;
    }

    if (a.type == extent_protocol::T_FILE) {
        printf("isfile: %lld is a file\n", inum);
        return true;
    } 
    printf("isfile: %lld is not a file\n", inum);
    return false;
}
/** Your code here for Lab...
 * You may need to add routines such as
 * readlink, issymlink here to implement symbolic link.
 * 
 * */

bool
yfs_client::isdir(inum inum)
{
    extent_protocol::attr a;
    
    if (ec->getattr(inum, a) != extent_protocol::OK) {
        printf("isdir: error getting attr\n");
        return false;
    }

    if (a.type == extent_protocol::T_DIR) {
        printf("isdir: %lld is a directory\n", inum);
        return true;
    } 
    printf("isdir: %lld is not a directory\n", inum);
    return false;
}

int
yfs_client::getfile(inum inum, fileinfo &fin)
{
    int r = OK;

    printf("getfile %016llx\n", inum);
    extent_protocol::attr a;
    if (ec->getattr(inum, a) != extent_protocol::OK) {
        r = IOERR;
        goto release;
    }

    fin.atime = a.atime;
    fin.mtime = a.mtime;
    fin.ctime = a.ctime;
    fin.size = a.size;
    printf("getfile %016llx -> sz %llu\n", inum, fin.size);

release:
    return r;
}

int
yfs_client::getdir(inum inum, dirinfo &din)
{
    int r = OK;

    printf("getdir %016llx\n", inum);
    extent_protocol::attr a;
    if (ec->getattr(inum, a) != extent_protocol::OK) {
        r = IOERR;
        goto release;
    }
    din.atime = a.atime;
    din.mtime = a.mtime;
    din.ctime = a.ctime;

release:
    return r;
}


#define EXT_RPC(xx) do { \
    if ((xx) != extent_protocol::OK) { \
        printf("EXT_RPC Error: %s:%d \n", __FILE__, __LINE__); \
        r = IOERR; \
        goto release; \
    } \
} while (0)

// Only support set size of attr
int
yfs_client::setattr(inum ino, size_t size)
{
    int r = OK;

    /*
     * your code goes here.
     * note: get the content of inode ino, and modify its content
     * according to the size (<, =, or >) content length.
     */
    std::string buf;
    r = ec->get(ino, buf);

    buf.resize(size);

    r = ec->put(ino, buf);

    return r;
}

int
yfs_client::create(inum parent, const char *name, mode_t mode, inum &ino_out)
{
    int r = OK;

    /*
     * your code goes here.
     * note: lookup is what you need to check if file exist;
     * after create file or dir, you must remember to modify the parent infomation.
     */
    bool existed;
    r = lookup(parent,name,existed,ino_out);
    if (existed)
	return r;

    ec->create(extent_protocol::T_FILE,ino_out);
    
    std::string buf;
    ec->get(parent,buf);

    buf.append('/'+std::string(name)+'/'+filename(ino_out));
    ec->put(parent,buf);

    return r;
}

int
yfs_client::mkdir(inum parent, const char *name, mode_t mode, inum &ino_out)
{

    int r = OK;

    /*
     * your code goes here.
     * note: lookup is what you need to check if directory exist;
     * after create file or dir, you must remember to modify the parent infomation.
     */
    bool existed = false;
    r = lookup(parent,name,existed,ino_out);

    if (existed)
	return r;

    ec->create(extent_protocol::T_DIR,ino_out);

    std::string buf;
    ec->get(parent,buf);
    buf.append('/'+std::string(name)+'/'+filename(ino_out));
    ec->put(parent,buf);
    return r;
}

int
yfs_client::lookup(inum parent, const char *name, bool &found, inum &ino_out)
{
    int r = OK;

    /*
     * your code goes here.
     * note: lookup file from parent dir according to name;
     * you should design the format of directory content.
     */

    std::list<dirent> entries;
    readdir(parent,entries);
    found = false;

    for (std::list<dirent>::iterator it=entries.begin();it!=entries.end();it++)
    {
        if (it->name == std::string(name)) 
	{
            found = true;
            ino_out = it->inum;
            r = EXIST;
        }
    }
    return r;
}

int
yfs_client::readdir(inum dir, std::list<dirent> &list)
{
    int r = OK;

    /*
     * your code goes here.
     * note: you should parse the dirctory content using your defined format,
     * and push the dirents to the list.
     */
    std::string buf;
    r = ec->get(dir, buf);
    unsigned int first = 1,last=0;
    struct dirent *entry = new dirent(); 

    while (last<buf.size()) 
    {
        last = buf.find('/',first);
        entry->name = buf.substr(first,last-first);
        first = last+1;

        last = buf.find('/', first);
        entry->inum = n2i(buf.substr(first,last-first));
        first = last+1;

        list.push_back(*entry);
    }
    delete entry;
    return r;
}

int
yfs_client::read(inum ino, size_t size, off_t off, std::string &data)
{
    int r = OK;

    /*
     * your code goes here.
     * note: read using ec->get().
     */
    std::string buf;
    r = ec->get(ino, buf);

    if ((int)buf.size() <= off) 
        data = "";
    else
        data = buf.substr(off, size);

    return r;
}

int
yfs_client::write(inum ino, size_t size, off_t off, const char *data,
        size_t &bytes_written)
{
    int r = OK;

    /*
     * your lab2 code goes here.
     * note: write using ec->put().
     * when off > length of original file, fill the holes with '\0'.
     */
    std::string buf;
    r = ec->get(ino, buf);

    if((int)buf.size()<off || buf.size() < (off+size))
    {
        buf.resize(off,'\0');
        buf.append(std::string(data,size));
    } 
    else
        buf.replace(off, size, std::string(data, size));
    
    r = ec->put(ino, buf);

    bytes_written = size;
    
    return r;
}

int yfs_client::unlink(inum parent,const char *name)
{
    int r = OK;

    /*
     * your code goes here.
     * note: you should remove the file using ec->remove,
     * and update the parent directory content.
     */

    inum ino_out;
    bool existed = true;
    r = lookup(parent,name,existed,ino_out);

    if (!existed)
	return r;

    ec->remove(ino_out);

    std::string buf,entry = '/'+std::string(name)+'/'+filename(ino_out);

    r = ec->get(parent, buf);

    int first = buf.find(entry);

    buf.replace(first,entry.size(),"");

    r = ec->put(parent, buf);

    return r;
}

int 
yfs_client::symlink(inum parent, const char *name, const char *link, inum &ino_out)
{
    int r = OK;
    ec->create(extent_protocol::T_SYM,ino_out);

    r = ec->create(extent_protocol::T_SYM,ino_out);

    std::string buf;
    ec->get(parent,buf);

    buf.append('/'+std::string(name)+'/'+filename(ino_out));
    ec->put(parent,buf);

    r = ec->put(ino_out, std::string(link));
    
    return r;
}

int 
yfs_client::readlink(inum ino, std::string &link)
{
    int r = OK;

    r = ec->get(ino, link);
    return r;
}

