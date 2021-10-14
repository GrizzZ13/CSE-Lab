#include "inode_manager.h"

// disk layer -----------------------------------------

disk::disk()
{
  bzero(blocks, sizeof(blocks));
}

void disk::read_block(blockid_t id, char *buf)
{
  memcpy(buf, blocks[id], BLOCK_SIZE);
}

void disk::write_block(blockid_t id, const char *buf)
{
  memcpy(blocks[id], buf, BLOCK_SIZE);
}

// block layer -----------------------------------------

// Allocate a free disk block.
blockid_t
block_manager::alloc_block()
{
  /*
   * your code goes here.
   * note: you should mark the corresponding bit in block bitmap when alloc.
   * you need to think about which block you can start to be allocated.
   */
  blockid_t start = IBLOCK(INODE_NUM, BLOCK_NUM) + 1;
  for (blockid_t i = start; i < BLOCK_NUM; i++)
  {
    if (using_blocks[i] == 0)
    {
      using_blocks[i] = 1;
      return i;
    }
  }
  return 0;
}

void block_manager::free_block(uint32_t id)
{
  /* 
   * your code goes here.
   * note: you should unmark the corresponding bit in the block bitmap when free.
   */
  blockid_t start = IBLOCK(INODE_NUM, BLOCK_NUM) + 1;
  if (id < start || id >= BLOCK_NUM)
  {
    return;
  }
  else
  {
    using_blocks[id] = 0;
    return;
  }
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

}

void block_manager::read_block(uint32_t id, char *buf)
{
  d->read_block(id, buf);
}

void block_manager::write_block(uint32_t id, const char *buf)
{
  d->write_block(id, buf);
}

// inode layer -----------------------------------------

inode_manager::inode_manager()
{
  bm = new block_manager();
  uint32_t root_dir = alloc_inode(extent_protocol::T_DIR);
  if (root_dir != 1)
  {
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
   * your code goes here.
   * note: the normal inode block should begin from the 2nd inode block.
   * the 1st is used for root_dir, see inode_manager::inode_manager().
   */
  for (uint32_t i = 1; i <= INODE_NUM; ++i)
  {
    inode_t *inode_ptr = get_inode(i);
    if (inode_ptr == nullptr)
    {
      inode_t inode_tmp;
      inode_tmp.type = type;
      inode_tmp.size = 0;
      inode_tmp.atime = time(0);
      inode_tmp.mtime = time(0);
      inode_tmp.ctime = time(0);
      put_inode(i, &inode_tmp);
      return i;
    }
    else
    {
      free(inode_ptr);
    }
  }
  return 0;
}

void inode_manager::free_inode(uint32_t inum)
{
  /* 
   * your code goes here.
   * note: you need to check if the inode is already a freed one;
   * if not, clear it, and remember to write back to disk.
   */
  if (inum < 1 || inum > INODE_NUM)
  {
    return;
  }
  inode_t *inode_ptr = get_inode(inum);
  if (inode_ptr == nullptr)
  {
    return;
  }
  inode_ptr->type = 0;
  put_inode(inum, inode_ptr);
  free(inode_ptr);
  return;
}

/* Return an inode structure by inum, NULL otherwise.
 * Caller should release the memory. */
struct inode *
inode_manager::get_inode(uint32_t inum)
{
  struct inode *ino, *ino_disk;
  char buf[BLOCK_SIZE];

  printf("\tim: get_inode %d\n", inum);

  if (inum < 0 || inum >= INODE_NUM)
  {
    printf("\tim: inum out of range\n");
    return NULL;
  }

  bm->read_block(IBLOCK(inum, bm->sb.nblocks), buf);
  // printf("%s:%d\n", __FILE__, __LINE__);

  ino_disk = (struct inode *)buf + inum % IPB;
  if (ino_disk->type == 0)
  {
    printf("\tim: inode not exist\n");
    return NULL;
  }

  ino = (struct inode *)malloc(sizeof(struct inode));
  *ino = *ino_disk;

  return ino;
}

void inode_manager::put_inode(uint32_t inum, struct inode *ino)
{
  char buf[BLOCK_SIZE];
  struct inode *ino_disk;

  printf("\tim: put_inode %d\n", inum);
  if (ino == NULL)
    return;

  bm->read_block(IBLOCK(inum, bm->sb.nblocks), buf);
  ino_disk = (struct inode *)buf + inum % IPB;
  *ino_disk = *ino;
  bm->write_block(IBLOCK(inum, bm->sb.nblocks), buf);
}

#define MIN(a, b) ((a) < (b) ? (a) : (b))

/* Get all the data of a file by inum. 
 * Return alloced data, should be freed by caller. */
void inode_manager::read_file(uint32_t inum, char **buf_out, int *size)
{
  /*
   * your code goes here.
   * note: read blocks related to inode number inum,
   * and copy them to buf_out
   */
  inode_t *inode_ptr = get_inode(inum);

  // inode does not exist
  if (inode_ptr == nullptr)
  {
    return;
  }

  // inode exists
  uint32_t block_num = inode_ptr->size / BLOCK_SIZE;
  if (inode_ptr->size % BLOCK_SIZE != 0)
  {
    block_num++;
  }
  char *buf_tmp = new char[BLOCK_SIZE * block_num];

  // direct blocks only
  if (block_num <= NDIRECT)
  {
    for (uint32_t i = 0; i < block_num; i++)
    {
      bm->read_block(inode_ptr->blocks[i], buf_tmp + i * BLOCK_SIZE);
    }
  }
  // indirect blocks
  else
  {
    for (uint32_t i = 0; i < NDIRECT; i++)
    {
      bm->read_block(inode_ptr->blocks[i], buf_tmp + i * BLOCK_SIZE);
    }
    uint32_t indirect_blocks[NINDIRECT];
    bm->read_block(inode_ptr->blocks[NDIRECT], (char *)indirect_blocks);
    for (uint32_t i = 0; i < block_num - NDIRECT; ++i)
    {
      bm->read_block(indirect_blocks[i], buf_tmp + NDIRECT * BLOCK_SIZE + i * BLOCK_SIZE);
    }
  }
  inode_ptr->atime = time(0);
  put_inode(inum, inode_ptr);
  *size = inode_ptr->size;
  *buf_out = buf_tmp;
  free(inode_ptr);
  return;
}

/* alloc/free blocks if needed */
void inode_manager::write_file(uint32_t inum, const char *buf, int size)
{
  /*
   * your code goes here.
   * note: write buf to blocks of inode inum.
   * you need to consider the situation when the size of buf 
   * is larger or smaller than the size of original inode
   */
  inode_t *inode_ptr = get_inode(inum);
  if (inode_ptr == nullptr)
  {
    return;
  }

  uint32_t size_origin = inode_ptr->size;
  uint32_t block_origin = size_origin / BLOCK_SIZE;
  uint32_t block_write = size / BLOCK_SIZE;
  // compute block number
  if (size_origin % BLOCK_SIZE != 0)
  {
    block_origin++;
  }
  if (size % BLOCK_SIZE != 0)
  {
    block_write++;
  }

  // new file size smaller than the old one
  if (block_write <= block_origin)
  {
    // block_write <= block_origin
    if (block_write <= NDIRECT)
    {

      for (uint32_t i = 0; i < block_write; ++i)
      {
        bm->write_block(inode_ptr->blocks[i], buf + i * BLOCK_SIZE);
      }

      if (block_origin <= NDIRECT)
      {
        for (uint32_t i = block_write; i < block_origin; ++i)
        {
          bm->free_block(inode_ptr->blocks[i]);
        }
      }
      else
      {
        uint32_t indirect_blocks[NINDIRECT];
        bm->read_block(inode_ptr->blocks[NDIRECT], (char *)indirect_blocks);
        // free useless direct blocks
        for (uint32_t i = block_write; i < NDIRECT; ++i)
        {
          bm->free_block(inode_ptr->blocks[i]);
        }
        // free useless indirect blocks
        for (uint32_t i = 0; i < block_origin - NDIRECT; ++i)
        {
          bm->free_block(indirect_blocks[i]);
        }
        bm->free_block(inode_ptr->blocks[NDIRECT]);
      }
    }
    // larger than direct blocks' size
    else
    {
      uint32_t indirect_blocks[NINDIRECT];
      bm->read_block(inode_ptr->blocks[NDIRECT], (char *)indirect_blocks);
      for (uint32_t i = 0; i < NDIRECT; ++i)
      {
        bm->write_block(inode_ptr->blocks[i], buf + i * BLOCK_SIZE);
      }
      for (uint32_t i = 0; i < block_write - NDIRECT; ++i)
      {
        bm->write_block(indirect_blocks[i], buf + i * BLOCK_SIZE + NDIRECT * BLOCK_SIZE);
      }
      for (uint32_t i = block_write - NDIRECT; i < block_origin - NDIRECT; ++i)
      {
        bm->free_block(indirect_blocks[i]);
      }
    }
  }
  // new file size larger than the old one
  else
  {
    // block origin < block_write
    if (block_write <= NDIRECT)
    {
      // block_origin < block_write <= NDIRECT
      #ifdef DEBUG
      std::cout << "block_origin < block_write <= NDIRECT" << std::endl;
      #endif
      for (uint32_t i = 0; i < block_origin; ++i)
      {
        bm->write_block(inode_ptr->blocks[i], buf + i * BLOCK_SIZE);
      }
      uint32_t block_num;
      for (uint32_t i = block_origin; i < block_write; ++i)
      {
        block_num = bm->alloc_block();
        inode_ptr->blocks[i] = block_num;
        bm->write_block(block_num, buf + i * BLOCK_SIZE);
      }
    }
    else
    {
      // block_write > NDIRECT
      uint32_t indirect_blocks[NINDIRECT];
      if (block_origin <= NDIRECT)
      {
        // block_origin <= NDIRECT < block_write
        #ifdef DEBUG
        std::cout << "block_origin <= NDIRECT < block_write" << std::endl;
        #endif
        for (uint32_t i = 0; i < block_origin; ++i)
        {
          bm->write_block(inode_ptr->blocks[i], buf + i * BLOCK_SIZE);
        }
        uint32_t block_num;
        for (uint32_t i = block_origin; i < NDIRECT; ++i)
        {
          block_num = bm->alloc_block();
          inode_ptr->blocks[i] = block_num;
          bm->write_block(block_num, buf + i * BLOCK_SIZE);
        }
        inode_ptr->blocks[NDIRECT] = bm->alloc_block();
        for (uint32_t i = 0; i < block_write - NDIRECT; ++i)
        {
          block_num = bm->alloc_block();
          indirect_blocks[i] = block_num;
          bm->write_block(block_num, buf + i * BLOCK_SIZE + NDIRECT * BLOCK_SIZE);
        }
        bm->write_block(inode_ptr->blocks[NDIRECT], (char *)indirect_blocks);
      }
      else
      {
        // NDIRECT < block_origin < block_write
        #ifdef DEBUG
        std::cout << "NDIRECT < block_origin < block_write" << std::endl;
        #endif
        uint32_t block_num;
        uint32_t indirect_blocks[NINDIRECT];
        bm->read_block(inode_ptr->blocks[NDIRECT], (char *)indirect_blocks);
        for (uint32_t i = 0; i < NDIRECT; ++i)
        {
          bm->write_block(inode_ptr->blocks[i], buf + i * BLOCK_SIZE);
        }
        for (uint32_t i = 0; i < block_origin-NDIRECT; ++i)
        {
          bm->write_block(indirect_blocks[i], buf + i * BLOCK_SIZE + NDIRECT * BLOCK_SIZE);
        }
        for (uint32_t i = block_origin-NDIRECT; i < block_write-NDIRECT; ++i)
        {
          block_num = bm->alloc_block();
          indirect_blocks[i] = block_num;
          bm->write_block(block_num, buf + i * BLOCK_SIZE + NDIRECT * BLOCK_SIZE);
        }
        bm->write_block(inode_ptr->blocks[NDIRECT], (char *)indirect_blocks);
      }
    }
  }
  inode_ptr->mtime = time(0);
  inode_ptr->ctime = time(0);
  inode_ptr->atime = time(0);
  inode_ptr->size = size;
  put_inode(inum, inode_ptr);
  free(inode_ptr);
  return;
}

void inode_manager::getattr(uint32_t inum, extent_protocol::attr &a)
{
  /*
   * your code goes here.
   * note: get the attributes of inode inum.
   * you can refer to "struct attr" in extent_protocol.h
   */
  inode_t *inode_ptr = get_inode(inum);
  if (inode_ptr == nullptr)
  {
    return;
  }
  a.type = inode_ptr->type;
  a.size = inode_ptr->size;
  a.atime = inode_ptr->atime;
  a.ctime = inode_ptr->ctime;
  a.mtime = inode_ptr->mtime;
  free(inode_ptr);
  return;
}

void inode_manager::remove_file(uint32_t inum)
{
  /*
   * your code goes here
   * note: you need to consider about both the data block and inode of the file
   */
  inode_t *inode_ptr = get_inode(inum);
  if (inode_ptr == nullptr)
  {
    return;
  }
  // file
  // if(inode_ptr->type==extent_protocol::T_FILE){
  //   uint32_t block_num = inode_ptr->size/BLOCK_SIZE;
  //   if(block_num%BLOCK_SIZE!=0){
  //     block_num++;
  //   }

  //   if(block_num <= NDIRECT){
  //     for(uint32_t i=0;i<block_num;++i){
  //       bm->free_block(inode_ptr->blocks[i]);
  //     }
  //   }
  //   else{
  //     uint32_t indirect_blocks[NINDIRECT];
  //     bm->read_block(inode_ptr->blocks[NDIRECT], (char*)indirect_blocks);
  //     for(uint32_t i=0;i<NDIRECT;++i){
  //       bm->free_block(inode_ptr->blocks[i]);
  //     }
  //     for(uint32_t i=0;i<block_num-NDIRECT;++i){
  //       bm->free_block(indirect_blocks[i]);
  //     }
  //     bm->free_block(inode_ptr->blocks[NDIRECT]);
  //   }
  //   free_inode(inum);
  //   free(inode_ptr);
  // }
  uint32_t block_num = inode_ptr->size / BLOCK_SIZE;
  if (block_num % BLOCK_SIZE != 0)
  {
    block_num++;
  }

  if (block_num <= NDIRECT)
  {
    for (uint32_t i = 0; i < block_num; ++i)
    {
      bm->free_block(inode_ptr->blocks[i]);
    }
  }
  else
  {
    uint32_t indirect_blocks[NINDIRECT];
    bm->read_block(inode_ptr->blocks[NDIRECT], (char *)indirect_blocks);
    for (uint32_t i = 0; i < NDIRECT; ++i)
    {
      bm->free_block(inode_ptr->blocks[i]);
    }
    for (uint32_t i = 0; i < block_num - NDIRECT; ++i)
    {
      bm->free_block(indirect_blocks[i]);
    }
    bm->free_block(inode_ptr->blocks[NDIRECT]);
  }
  free_inode(inum);
  free(inode_ptr);

  return;
}

void
inode_manager::set_time(uint32_t inum){
  inode* inode_ptr = get_inode(inum);
  inode_ptr->ctime = time(0);
  inode_ptr->mtime = time(0);
  inode_ptr->atime = time(0);
}
