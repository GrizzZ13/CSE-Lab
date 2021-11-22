#ifndef raft_storage_h
#define raft_storage_h

#include "raft_protocol.h"
#include <fcntl.h>
#include <mutex>
#include <fstream>

template<typename command>
class raft_storage {
public:
    raft_storage(const std::string& file_dir);
    ~raft_storage();
    // Your code here
    void store(int current_term, std::vector<log_entry<command>>& log_entries);
    bool restore(int &current_term, std::vector<log_entry<command>>& log_entries);
private:
    std::mutex mtx;
    std::string dir;
};

template<typename command>
raft_storage<command>::raft_storage(const std::string& dir){
    // Your code here
    this->dir = dir;
}

template<typename command>
raft_storage<command>::~raft_storage() {
   // Your code here
}

template<typename command>
void raft_storage<command>::store(int current_term, std::vector<log_entry<command>>& log_entries) {
   std::ofstream ofs;
   ofs.open(dir+"/log_file.log", std::ofstream::out);
   std::string buf;
   buf += std::to_string(current_term) + "\n";
   char cbuf[8192];
   int size;
   for(const auto& entry : log_entries){
        char cbuf[8192];
        int size = entry.cmd.size();
        entry.cmd.serialize(cbuf, size);
        std::string cmdstr(cbuf, size);
        buf += std::to_string(entry.term) + " " + std::to_string(entry.index)
                + " " + std::to_string(size) + " " + cmdstr + "\n";
   }
   ofs << buf;
   ofs.close();
}

template<typename command>
bool raft_storage<command>::restore(int &current_term, std::vector<log_entry<command>>& log_entries) {
   std::ifstream ifs;
   ifs.open(dir+"/log_file.log");
   if(ifs.is_open()){
       std::string buf;
       getline(ifs, buf);
       current_term = atoi(buf.c_str());
       while(getline(ifs, buf)){
           if(buf == "\n"){
               break;
           }
           int term, index, size;
           char cbuf[8192];
           sscanf(buf.c_str(), "%d %d %d %s", &term, &index, &size, cbuf);
           log_entry<command> entry;
           entry.term = term;
           entry.index = index;
           entry.cmd.deserialize(cbuf, size);
           log_entries.push_back(entry);
       }
       ifs.close();
       return true;
   }
   else{
       return false;
   }
}

#endif // raft_storage_h