// chfs client.  implements FS operations using extent and lock server
#include "chfs_client.h"
#include "extent_client.h"
#include <sstream>
#include <iostream>
#include <stdio.h>
#include <unistd.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>

#define DEBUG

chfs_client::chfs_client()
{
    ec = new extent_client();

}

chfs_client::chfs_client(std::string extent_dst, std::string lock_dst)
{
    ec = new extent_client();
    if (ec->put(1, "") != extent_protocol::OK)
        printf("error init root dir\n"); // XYB: init root dir
}

chfs_client::inum
chfs_client::n2i(std::string n)
{
    std::istringstream ist(n);
    unsigned long long finum;
    ist >> finum;
    return finum;
}

std::string
chfs_client::filename(inum inum)
{
    std::ostringstream ost;
    ost << inum;
    return ost.str();
}

bool
chfs_client::isfile(inum inum)
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
chfs_client::isdir(inum inum)
{
    // Oops! is this still correct when you implement symlink?
    extent_protocol::attr a;
    if (ec->getattr(inum, a) != extent_protocol::OK) {
        return false;
    }
    if (a.type == extent_protocol::T_DIR) {
        return true;
    } 
    return false;
}

bool
chfs_client::issymlink(inum inum)
{
    extent_protocol::attr a;
    if (ec->getattr(inum, a) != extent_protocol::OK) {
        return false;
    }
    if (a.type == extent_protocol::T_SYMLINK) {
        return true;
    } 
    return false;
}

int
chfs_client::getfile(inum inum, fileinfo &fin)
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
chfs_client::getdir(inum inum, dirinfo &din)
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

int
chfs_client::getsymlink(inum inum, symlinkinfo &sin)
{
    int r = OK;

    printf("getsymlink %016llx\n", inum);
    extent_protocol::attr a;
    if (ec->getattr(inum, a) != extent_protocol::OK) {
        printf("get symlink error\n");
        r = IOERR;
        goto release;
    }
    printf("get symlink successfully\n");
    sin.size = a.size;

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
chfs_client::setattr(inum ino, size_t size)
{
    int r = OK;

    /*
     * your code goes here.
     * note: get the content of inode ino, and modify its content
     * according to the size (<, =, or >) content length.
     */
    #ifdef DEBUG
    std::cout << "set attr !!!!!" << std::endl;
    #endif
    if(ino<=0||ino>INODE_NUM){
        return IOERR;
    }
    std::string buf;
    r = ec->get(ino, buf);
    if(r!=extent_protocol::OK){
        return r;
    }
    if(buf.size() < size)
	{
		buf.insert(buf.size(), size-buf.size(), 0);
	}
	else
	{
		buf = buf.substr(0, size);
	}
    r = ec->put(ino, buf);
    return r;
}

int
chfs_client::create(inum parent, const char *name, mode_t mode, inum &ino_out)
{
    int r = OK;

    /*
     * your code goes here.
     * note: lookup is what you need to check if file exist;
     * after create file or dir, you must remember to modify the parent infomation.
     */
    
    // create file
    bool exist;
    std::string buf;
    mydirent entry;
    lookup(parent, name, exist, ino_out);
    if(exist){
        return EXIST;
    }
    r = ec->create(extent_protocol::T_FILE, ino_out);
    if(r!=extent_protocol::OK){
        return r;
    }
    // modify parent which is a directory
    r = ec->get(parent, buf);
    if(r!=extent_protocol::OK){
        return r;
    }
    strcpy(entry.name, name);
    entry.inum = ino_out;
    buf.insert(buf.size(), (char*)(&entry), sizeof(mydirent));
    r = ec->put(parent, buf);
    return r;
}

int
chfs_client::mkdir(inum parent, const char *name, mode_t mode, inum &ino_out)
{
    int r = OK;

    /*
     * your code goes here.
     * note: lookup is what you need to check if directory exist;
     * after create file or dir, you must remember to modify the parent infomation.
     */
    // make directory
    bool exist;
    std::string buf;
    mydirent entry;
    lookup(parent, name, exist, ino_out);
    if(exist){
        return EXIST;
    }
    r = ec->create(extent_protocol::T_DIR, ino_out);
    if(r!=extent_protocol::OK){
        return r;
    }
    r = ec->get(parent, buf);
    if(r!=extent_protocol::OK){
        return r;
    }
    strcpy(entry.name, name);
    entry.inum = ino_out;
    buf.insert(buf.size(), (char*)(&entry), sizeof(mydirent));
    r = ec->put(parent, buf);
    return r;
}

int
chfs_client::lookup(inum parent, const char *name, bool &found, inum &ino_out)
{
    int r = OK;

    /*
     * your code goes here.
     * note: lookup file from parent dir according to name;
     * you should design the format of directory content.
     */
    std::string buf;
    r = ec->get(parent, buf);
    if(r!=extent_protocol::OK){
        return r;
    }
    const char* chars = buf.c_str();
    mydirent entry;
    uint32_t entry_size = sizeof(mydirent);
    for(uint32_t i=0;i<buf.size();i+=entry_size){
        if(!strcmp(chars+i, name)){
            memcpy((void*)(&entry), chars+i, entry_size);
            ino_out = entry.inum;
            found = true;
            return r;
        }
    }
    found = false;
    return r;
}

int
chfs_client::readdir(inum dir, std::list<dirent> &list)
{
    int r = OK;

    /*
     * your code goes here.
     * note: you should parse the dirctory content using your defined format,
     * and push the dirents to the list.
     */
    std::string buf;
    r = ec->get(dir, buf);
    if(r!=extent_protocol::OK){
        return r;
    }
    mydirent entry;
    const char* chars = buf.c_str();
    uint32_t entry_size = sizeof(mydirent);
    for(uint32_t i=0;i<buf.size();i+=entry_size){
        memcpy((void*)(&entry), chars+i, entry_size);
        dirent item;
        item.name.append(entry.name);
        #ifdef DEBUG
        std::cout << "read dir ::: " << item.name << std::endl;
        #endif
        item.inum = entry.inum;
        list.push_back(item);
    }
    return r;
}

int
chfs_client::read(inum ino, size_t size, off_t off, std::string &data)
{
    int r = OK;

    /*
     * your code goes here.
     * note: read using ec->get().
     */
    std::string buf;
    r = ec->get(ino, buf);
    if(r!=extent_protocol::OK){
        return r;
    }
    if(off>=buf.size()){
        #ifdef DEBUG
        std::cout << "read case 1" << std::endl;
        std::cout << "inode" << '\t' << ino << '\t' << size << '\t' << off << '\t' << buf.size() << std::endl;
        #endif
        data = "";
    }
    else{
        if(off+size <= buf.size()){
            #ifdef DEBUG
            std::cout << "read case 2" << std::endl;
            std::cout << "inode" << '\t' << ino << '\t' << size << '\t' << off << '\t' << buf.size() << std::endl;
            #endif
            data = buf.substr(off, size);
        }
        else{
            #ifdef DEBUG
            std::cout << "read case 3" << std::endl;
            std::cout << "inode" << '\t' << ino << '\t' << size << '\t' << off << '\t' << buf.size() << std::endl;
            #endif
            data = buf.substr(off);
        }
    }
    #ifdef DEBUG
    std::cout << "read ::: " << data.size() << " ::: " << data << std::endl;
    #endif
    return r;
}

int
chfs_client::write(inum ino, size_t size, off_t off, const char *data,
        size_t &bytes_written)
{
    int r = OK;

    /*
     * your code goes here.
     * note: write using ec->put().
     * when off > length of original file, fill the holes with '\0'.
     */
    std::string buf;
    std::string dataStr(size, 0);
    uint64_t maxsize = MAXFILE * BLOCK_SIZE;

    for(uint32_t i=0;i<size;++i){
        dataStr[i] = data[i];
    }
    dataStr = dataStr.substr(0, size);
    #ifdef DEBUG
    std::cout << "write ::: " << dataStr.size() << " ::: " << dataStr << std::endl;
    #endif
    bytes_written = 0;
    r = ec->get(ino, buf);
    if(r!=extent_protocol::OK){
        return r;
    }
    if(off < maxsize){
        if(off+size > maxsize){
            #ifdef DEBUG
            std::cout << "write case 1\t" << maxsize << std::endl;
            std::cout << "inode" << '\t' << ino << '\t' << size << '\t' << off << '\t' << buf.size() << std::endl;
            #endif
            dataStr = dataStr.substr(0, maxsize-off);
            size = maxsize - off;
            #ifdef DEBUG
            std::cout << "bytes written\t" << bytes_written << std::endl;
            #endif
        }
        if(off>buf.size()){
            #ifdef DEBUG
            std::cout << "write case 2\t" << maxsize << std::endl;
            std::cout << "inode" << '\t' << ino << '\t' << size << '\t' << off << '\t' << buf.size() << std::endl;
            #endif
            std::string holes(off-buf.size(), 0);
            buf = buf+holes+dataStr;
            bytes_written = dataStr.size();
            #ifdef DEBUG
            std::cout << "bytes written\t" << bytes_written << std::endl;
            #endif
        }
        else{
            #ifdef DEBUG
            std::cout << "write case 3\t" << maxsize << std::endl;
            std::cout << "inode" << '\t' << ino << '\t' << size << '\t' << off << '\t' << buf.size() << std::endl;
            #endif
            buf.replace(off, size, dataStr);
            bytes_written = size;
            #ifdef DEBUG
            std::cout << "bytes written\t" << bytes_written << std::endl;
            #endif
        }
    }
    else{
        #ifdef DEBUG
        std::cout << "write case 4\t" << maxsize << std::endl;
        std::cout << "inode" << '\t' << ino << '\t' << size << '\t' << off << '\t' << buf.size() << std::endl;
        #endif
        std::string holes(maxsize-buf.size(), '\0');
        buf.append(holes);
        bytes_written = maxsize-buf.size();
        #ifdef DEBUG
        std::cout << "bytes written\t" << bytes_written << std::endl;
        #endif
    }
    r = ec->put(ino, buf);
    if(r==OK){
        std::cout << bytes_written << std::endl;
    }
    return r;
}

int chfs_client::unlink(inum parent,const char *name)
{
    int r = OK;
    std::string buf;
    int found = 0;
    uint32_t offset = 0;

    /*
     * your code goes here.
     * note: you should remove the file using ec->remove,
     * and update the parent directory content.
     */
    if((r=ec->get(parent, buf))!=extent_protocol::OK){
        return r;
    }
    mydirent entry;
    const char* chars = buf.c_str();
    uint32_t entry_size = sizeof(mydirent);
    for(uint32_t i=0;i<buf.size();i+=entry_size){
        memcpy((void*)(&entry), chars+i, entry_size);
        if(strcmp(entry.name, name)==0){
            found = entry.inum;
            offset = i;
            break;
        }
    }
    if(found==0){
        return NOENT;
    }
    buf = buf.erase(offset, entry_size);
    if((r=ec->remove(found))!=extent_protocol::OK){
        return r;
    }
    if((r=ec->put(parent, buf))!=extent_protocol::OK){
        return r;
    }
    return r;
}

int
chfs_client::mksymlink(inum parent, const char *name, mode_t mode, inum &ino_out)
{
    int r = OK;

    /*
     * your code goes here.
     * note: lookup is what you need to check if directory exist;
     * after create file or dir, you must remember to modify the parent infomation.
     */
    // make directory
    bool exist;
    std::string buf;
    mydirent entry;
    lookup(parent, name, exist, ino_out);
    if(exist){
        return EXIST;
    }
    r = ec->create(extent_protocol::T_SYMLINK, ino_out);
    if(r!=extent_protocol::OK){
        return r;
    }
    r = ec->get(parent, buf);
    if(r!=extent_protocol::OK){
        return r;
    }
    strcpy(entry.name, name);
    entry.inum = ino_out;
    buf.insert(buf.size(), (char*)(&entry), sizeof(mydirent));
    r = ec->put(parent, buf);
    return r;
}

int chfs_client::readlink(inum ino, std::string &link)
{
    if((ec->get(ino, link))!=extent_protocol::OK){
        return IOERR;
    }
    else{
        return OK;
    }
}

int chfs_client::symlink(const char *link, inum parent, const char *name, inum &ino_out)
{
    int r = OK;
    size_t tmp;
    if((r=mksymlink(parent, name, 0, ino_out))!=OK){
        #ifdef DEBUG
        std::cout << "mksymlink succeed" << std::endl;
        #endif
        return r;
    }
    if((r=write(ino_out, strlen(link), 0, link, tmp))!=OK){
        #ifdef DEBUG
        std::cout << "write succeed" << std::endl;
        #endif
        return r;
    }
    return r;
}
