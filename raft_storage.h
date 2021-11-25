#ifndef raft_storage_h
#define raft_storage_h

#include "raft_protocol.h"
#include <fcntl.h>
#include <mutex>
#include <fstream>

template <typename command>
class raft_storage
{
public:
    raft_storage(const std::string &file_dir);
    ~raft_storage();
    // Your code here
    void store(int current_term, std::vector<log_entry<command>> &log_entries);
    bool restore(int &current_term, std::vector<log_entry<command>> &log_entries);

    void save_snapshot(int lastIncludedIndex, int lastIncludedTerm, const std::vector<char> &data);
    bool read_snapshot(int &lastIncludedIndex, int &lastIncludedTerm, std::vector<char> &data);

private:
    std::mutex entry_mtx;
    std::mutex snapshot_mtx;
    std::string dir;
};

template <typename command>
raft_storage<command>::raft_storage(const std::string &dir)
{
    // Your code here
    this->dir = dir;
}

template <typename command>
raft_storage<command>::~raft_storage()
{
    // Your code here
}

template <typename command>
void raft_storage<command>::store(int current_term, std::vector<log_entry<command>> &log_entries)
{
    std::unique_lock<std::mutex> lock(entry_mtx);
    std::ofstream ofs;
    ofs.open(dir + "/log_file.log", std::ios::out | std::ios::binary);
    int entry_count = log_entries.size();
    ofs.write((const char *)&current_term, sizeof(int));
    ofs.write((const char *)&entry_count, sizeof(int));
    for (const auto &entry : log_entries)
    {
        char cbuf[2048];
        int size = entry.cmd.size();
        entry.cmd.serialize(cbuf, size);
        ofs.write((const char *)&entry.index, sizeof(int));
        ofs.write((const char *)&entry.term, sizeof(int));
        ofs.write((const char *)&size, sizeof(int));
        ofs.write((const char *)cbuf, size);
    }
    ofs.close();
}

template <typename command>
bool raft_storage<command>::restore(int &current_term, std::vector<log_entry<command>> &log_entries)
{
    std::unique_lock<std::mutex> lock(entry_mtx);
    std::ifstream ifs;
    ifs.open(dir + "/log_file.log", std::ios::in | std::ios::binary);
    if (ifs.is_open())
    {
        int entry_count;
        char cbuf[2048];
        std::vector<log_entry<command>> log_entries_;
        ifs.read((char *)&current_term, sizeof(int));
        ifs.read((char *)&entry_count, sizeof(int));
        for (int i = 0; i < entry_count; ++i)
        {
            int size, term, index;
            ifs.read((char *)&index, sizeof(int));
            ifs.read((char *)&term, sizeof(int));
            ifs.read((char *)&size, sizeof(int));
            ifs.read((char *)cbuf, size);
            log_entry<command> entry;
            entry.term = term;
            entry.index = index;
            entry.cmd.deserialize(cbuf, size);
            log_entries_.push_back(entry);
        }
        ifs.close();
        log_entries.swap(log_entries_);
        return true;
    }
    else
    {
        return false;
    }
}

template <typename command>
void raft_storage<command>::save_snapshot(int lastIncludedIndex, int lastIncludedTerm, const std::vector<char> &data)
{
    std::unique_lock<std::mutex> lock(snapshot_mtx);
    std::ofstream ofs;
    int length = data.size();
    std::string buf(data.begin(), data.end());
    ofs.open(dir + "/snapshot", std::ios::out | std::ios::binary);
    ofs.write((const char *)&lastIncludedIndex, sizeof(int));
    ofs.write((const char *)&lastIncludedTerm, sizeof(int));
    ofs.write((const char *)&length, sizeof(int));
    ofs.write((const char *)buf.c_str(), length);
    ofs.close();
}

template <typename command>
bool raft_storage<command>::read_snapshot(int &lastIncludedIndex, int &lastIncludedTerm, std::vector<char> &data)
{
    std::unique_lock<std::mutex> lock(snapshot_mtx);
    std::ifstream ifs;
    ifs.open(dir + "/snapshot", std::ios::in | std::ios::binary);
    if (ifs.is_open())
    {
        char cbuf[8192];
        int length;
        ifs.read((char *)&lastIncludedIndex, sizeof(int));
        ifs.read((char *)&lastIncludedTerm, sizeof(int));
        ifs.read((char *)&length, sizeof(int));
        ifs.read((char *)cbuf, length);
        std::string buf(cbuf, length);
        std::vector<char> data_(buf.begin(), buf.end());
        data.swap(data_);
        ifs.close();
        return true;
    }
    else
    {
        return false;
    }
}

#endif // raft_storage_h