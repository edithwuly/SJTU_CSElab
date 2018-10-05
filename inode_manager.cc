#include "inode_manager.h"
#include <time.h>

// disk layer -----------------------------------------

using namespace std;

disk::disk()
{
  bzero(blocks, sizeof(blocks));
}

void
disk::read_block(blockid_t id, char *buf)
{
  if (id < 0 || id >= BLOCK_NUM || !buf) {
    return;
  }

  memcpy(buf, blocks[id], BLOCK_SIZE);
  return;
}

void
disk::write_block(blockid_t id, const char *buf)
{
  if (id < 0 || id >= BLOCK_NUM || !buf) {
    return;
  }

  memcpy(blocks[id], buf, BLOCK_SIZE);
  return;
}

// block layer -----------------------------------------

//Allocate a free disk block.
blockid_t
block_manager::alloc_block()
{
  /*
   * your code goes here.
   * note: you should mark the corresponding bit in block bitmap when alloc.
   * you need to think about which block you can start to be allocated.
   */
  
  blockid_t id=0;
  char bitmap[BLOCK_SIZE];
  for(int i = BBLOCK(id);i<BBLOCK(BLOCK_NUM);i++)
  {
    d->read_block(i, bitmap);
    for(int j = 0;j<BLOCK_SIZE;j++)
    {
      for(unsigned char mask=0x80;mask>0 && id<BLOCK_NUM;mask=mask>>1)
      {
	if((bitmap[j] & mask) == 0)
	{
          bitmap[j] |= mask;
	  d->write_block(i, bitmap);
          return id;
        }
        id++;
      }
    }
  } 
  return -1;
}

void
block_manager::free_block(uint32_t id)
{
  /* 
   * your lab1 code goes here.
   * note: you should unmark the corresponding bit in the block bitmap when free.
   */
  
  char bitmap[BLOCK_SIZE];
  d->read_block(BBLOCK(id), bitmap);
  unsigned char mask = 1<<(7- (id%BPB)%8);

  bitmap[(id % BPB) / 8] = bitmap[(id % BPB) / 8] ^ mask;
  d->write_block(BBLOCK(id), bitmap);
}

// The layout of disk should be like this:
// |<-sb->|<-free block bitmap->|<-inode table->|<-data->|
block_manager::block_manager()
{
  d = new disk();

  // format the disk
  sb.size = BLOCK_SIZE * BLOCK_NUM;
  sb.nblocks = BLOCK_NUM;
  sb.ninodes = INODE_NUM;

  /* 
   * Something hard to debug will happen 
   * if l didn't initial the disk
   */

  /* Alloc boot block */ 
  alloc_block();

  /* Alloc super block */ 
  alloc_block();

  /* Alloc bitmap */ 
  uint32_t i;
  for (i = 0; i < BLOCK_NUM / BPB; i++) {
    alloc_block();
  }

  /* Alloc inode table */ 
  for (i = 0; i < INODE_NUM / IPB; i++) {
    alloc_block();
  }
}

void
block_manager::read_block(uint32_t id, char *buf)
{
  d->read_block(id, buf);
}

void
block_manager::write_block(uint32_t id, const char *buf)
{
  d->write_block(id, buf);
}

// inode layer -----------------------------------------

inode_manager::inode_manager()
{
  bm = new block_manager();
  uint32_t root_dir = alloc_inode(extent_protocol::T_DIR);
  if (root_dir != 1) {
    printf("\tim: error! alloc first inode %d, should be 1\n", root_dir);
    exit(0);
  }
}

/* Create a new file.
 * Return its inum. */
uint32_t
inode_manager::alloc_inode(uint32_t type)
{
  /* 
   * your lab1 code goes here.
   * note: the normal inode block should begin from the 2nd inode block.
   * the 1st is used for root_dir, see inode_manager::inode_manager().
    
   * if you get some heap memory, do not forget to free it.
   */
  uint32_t inum = 1;
  char buf[BLOCK_SIZE];
  for(uint32_t i=IBLOCK(inum,BLOCK_NUM);i<IBLOCK(INODE_NUM,BLOCK_NUM);i++)
  {
    bm->read_block(i, buf);
    for(int j=0;j<IPB;j++)
    {
      inode_t* ino = (inode_t*)buf+j;
      if(ino->type == 0)
      {
         ino->type = type;
	 ino->size = 0;
         ino->atime = time(NULL);
	 ino->mtime = time(NULL);
         ino->ctime = time(NULL); 
         bm->write_block(i, buf);
         return inum;
      }
      inum++;
    }
  }
  return -1;
}

void
inode_manager::free_inode(uint32_t inum)
{
  /* 
   * your lab1 code goes here.
   * note: you need to check if the inode is already a freed one;
   * if not, clear it, and remember to write back to disk.
   * do not forget to free memory if necessary.
   */

  inode_t* ino = get_inode(inum);

  ino->type = 0;
  put_inode(inum, ino);

  free(ino);
  ino = NULL;
}


/* Return an inode structure by inum, NULL otherwise.
 * Caller should release the memory. */
struct inode* 
inode_manager::get_inode(uint32_t inum)
{
  struct inode *ino, *ino_disk;
  char buf[BLOCK_SIZE];

  printf("\tim: get_inode %d\n", inum);

  if (inum < 0 || inum >= INODE_NUM) {
    printf("\tim: error! inum out of range\n");
    return NULL;
  }

  bm->read_block(IBLOCK(inum, bm->sb.nblocks), buf);
  // printf("%s:%d\n", __FILE__, __LINE__);

  ino_disk = (struct inode*)buf + inum%IPB;
  if (ino_disk->type == 0) {
    printf("\tim: error! inode not exist\n");
    return NULL;
  }

  ino = (struct inode*)malloc(sizeof(struct inode));
  *ino = *ino_disk;

  return ino;
}

void
inode_manager::put_inode(uint32_t inum, struct inode *ino)
{
  char buf[BLOCK_SIZE];
  struct inode *ino_disk;

  printf("\tim: put_inode %d\n", inum);
  if (ino == NULL)
    return;

  bm->read_block(IBLOCK(inum, bm->sb.nblocks), buf);
  ino_disk = (struct inode*)buf + inum%IPB;
  *ino_disk = *ino;
  bm->write_block(IBLOCK(inum, bm->sb.nblocks), buf);
}

#define MIN(a,b) ((a)<(b) ? (a) : (b))

/* Get all the data of a file by inum. 
 * Return alloced data, should be freed by caller. */
void
inode_manager::read_file(uint32_t inum, char **buf_out, int *size)
{
  /*
   * your lab1 code goes here.
   * note: read blocks related to inode number inum,
   * and copy them to buf_out
   */
  inode_t *ino = get_inode(inum);

  if (!ino)
    return;

  int fsize = ino->size;
  *buf_out = (char *)malloc(fsize);

  char block[BLOCK_SIZE];
  int len = 0;

  for (unsigned int i=0; i<NDIRECT && len < fsize;i++) 
  {
    if (len < fsize-BLOCK_SIZE) 
    {
      bm->read_block(ino->blocks[i], *buf_out+len);
      len += BLOCK_SIZE;
    }
    else 
    {
      bm->read_block(ino->blocks[i], block);
      memcpy(*buf_out+len , block, fsize-len);
      len = fsize;
    }
  }

  if (len < fsize) 
  {
    blockid_t indirect[NINDIRECT];
    bm->read_block(ino->blocks[NDIRECT], (char *)indirect);
 
    for (unsigned int i=0; i<NINDIRECT && len<fsize;i++) 
    {
      if (len<fsize-BLOCK_SIZE) 
      {
        bm->read_block(indirect[i],*buf_out+len);
        len += BLOCK_SIZE;
      }
      else 
      {
        bm->read_block(indirect[i], block);
        memcpy(*buf_out+len,block,fsize-len);
        len = fsize;
      }
    }
  }

  unsigned int newtime = (unsigned int)time(NULL);
  ino->atime = newtime;
  ino->mtime = newtime;
  ino->ctime = newtime;

  *size = fsize;
  put_inode(inum, ino);
  free(ino);
  return;
}

/* alloc/free blocks if needed */
void
inode_manager::write_file(uint32_t inum, const char *buf, int size)
{
  /*
   * your lab1 code goes here.
   * note: write buf to blocks of inode inum.
   * you need to consider the situation when the size of buf 
   * is larger or smaller than the size of original inode.
   * you should free some blocks if necessary.
   */

  inode_t *ino = get_inode(inum);

  if (!ino) 
    return;

  unsigned int oldbnum = (ino->size+BLOCK_SIZE-1) / BLOCK_SIZE;
  unsigned int newbnum = (size+BLOCK_SIZE-1) / BLOCK_SIZE;

  if (newbnum < oldbnum) 
  {
    if (newbnum < NDIRECT)
      for (unsigned int i=newbnum;i<MIN(oldbnum, NDIRECT);i++)
        bm->free_block(ino->blocks[i]);

    if (oldbnum > NDIRECT)
    {
      blockid_t indirect[NINDIRECT];
      bm->read_block(ino->blocks[NDIRECT], (char *)indirect);      

      if (newbnum > NDIRECT) 
        for (unsigned int i= newbnum;i<oldbnum;i++)
          bm->free_block(indirect[i-NDIRECT]); 

      else
      {
        for (unsigned int i=NDIRECT;i<oldbnum;i++)
          bm->free_block(indirect[i-NDIRECT]);
        bm->free_block(ino->blocks[NDIRECT]);
      }
    }
  }

  if (newbnum > oldbnum) 
  {
    if (oldbnum < NDIRECT)
      for (unsigned int i=oldbnum;i<MIN(newbnum, NDIRECT);i++)
        ino->blocks[i] = bm->alloc_block();

    if (newbnum > NDIRECT) 
    {
      blockid_t indirect[NINDIRECT];

      if (oldbnum > NDIRECT) 
        for (unsigned int i=oldbnum;i<newbnum;i++) 
          indirect[i-NDIRECT] = bm->alloc_block();

      else 
      {
        ino->blocks[NDIRECT] = bm->alloc_block();        
        for (unsigned int i=NDIRECT;i<newbnum;i++) 
          indirect[i-NDIRECT] = bm->alloc_block();
      }
      bm->write_block(ino->blocks[NDIRECT], (char *)indirect);
    }
  }

  int len = 0;
  char block[BLOCK_SIZE];
  bzero(block, BLOCK_SIZE);

  for (unsigned int i=0; i<NDIRECT && len<size;i++) 
  {
    if (len<size-BLOCK_SIZE)
    {
      bm->write_block(ino->blocks[i],buf+len);
      len += BLOCK_SIZE;
    }
    else
    {
      memcpy(block, buf+len,size-len);
      bm->write_block(ino->blocks[i], block);
      len = size;
    }
  }

  if (newbnum > NDIRECT) 
  {
    blockid_t indirect[NINDIRECT];
    bm->read_block(ino->blocks[NDIRECT], (char *)indirect);
    
    for (unsigned int i=0;i<NINDIRECT && len<size;i++) 
    {
      if (len<size-BLOCK_SIZE)
      {
        bm->write_block(indirect[i],buf+len);
        len += BLOCK_SIZE;
      }
      else
      {
        memcpy(block, buf+len,size-len);
        bm->write_block(indirect[i], block);
        len = size;
      }
    }
  }

  unsigned int newtime = (unsigned int)time(NULL);
  ino->size = size;
  ino->mtime = newtime;
  ino->ctime = newtime;

  put_inode(inum, ino);
  free(ino);
  return;
}

void
inode_manager::getattr(uint32_t inum, extent_protocol::attr &a)
{
  /*
   * your code goes here.
   * note: get the attributes of inode inum.
   * you can refer to "struct attr" in extent_protocol.h
   */
  inode_t *ino = get_inode(inum);
  
  if (!ino)
    return;

  a.type  = ino->type;
  a.atime = ino->atime;
  a.mtime = ino->mtime;
  a.ctime = ino->ctime;
  a.size  = ino->size;

  free(ino);
  return;
}

void
inode_manager::remove_file(uint32_t inum)
{
  /*
   * your lab1 code goes here
   * note: you need to consider about both the data block and inode of the file
   * do not forget to free memory if necessary.
   */
  inode_t *ino = get_inode(inum);
  if (!ino)
    return;

  unsigned int bnum = (ino->size+BLOCK_SIZE-1) / BLOCK_SIZE;

  if (bnum > NDIRECT) 
  { 
    blockid_t indirect[NINDIRECT];
    bm->read_block(ino->blocks[NDIRECT], (char *)indirect);

    for (unsigned int i=0;i<bnum-NDIRECT;i++) 
      bm->free_block(indirect[i]);
    bm->free_block(ino->blocks[NDIRECT]);
  }

  for (unsigned int i=0; i<MIN(bnum, NDIRECT);i++) 
    bm->free_block(ino->blocks[i]);

  free_inode(inum);
  free(ino);

  return;
}
