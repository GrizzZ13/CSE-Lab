#include "raft_state_machine.h"


kv_command::kv_command() : kv_command(CMD_NONE, "", "") { }

kv_command::kv_command(command_type tp, const std::string &key, const std::string &value) : 
    cmd_tp(tp), key(key), value(value), res(std::make_shared<result>())
{
    res->start = std::chrono::system_clock::now();
    res->key = key;
}

kv_command::kv_command(const kv_command &cmd) :
    cmd_tp(cmd.cmd_tp), key(cmd.key), value(cmd.value), res(cmd.res) {}

kv_command::~kv_command() { }

int kv_command::size() const {
    // Your code here:
    int size_ = key.size() + value.size() + 3 * sizeof(int);
    return size_;
}


void kv_command::serialize(char* buf, int size) const {
    // Your code here:
    if(size < this->size()){
        return;
    }
    int array[3];
    array[1] = key.size();
    array[2] = value.size();
    std::string data;
    data.append(key);
    data.append(value);
    switch(cmd_tp){
        case CMD_NONE:
            array[0] = 1;
            break;
        case CMD_GET:
            array[0] = 2;
            break;
        case CMD_PUT:
            array[0] = 3;
            break;
        case CMD_DEL:
            array[0] = 4;
            break;
        default:
            break;
    }
    memcpy(buf, (const void*)array, 12);
    memcpy(buf + 12, (const void*)(data.c_str()), data.size());
    return;
}

void kv_command::deserialize(const char* buf, int size) {
    // Your code here:
    int array[3];
    memcpy(array, (const void*)buf, 12);
    switch(array[0]){
        case 1:
            cmd_tp = CMD_NONE;
            break;
        case 2:
            cmd_tp = CMD_GET;
            break;
        case 3:
            cmd_tp = CMD_PUT;
            break;
        case 4:
            cmd_tp = CMD_DEL;
            break;
        default:
            break;
    }
    std::string key_(buf+12, array[1]);
    std::string value_(buf+12+array[1], array[2]);
    key.swap(key_);
    value.swap(value_);
    return;
}

marshall& operator<<(marshall &m, const kv_command& cmd) {
    // Your code here:
    int size = cmd.size();
    char* cbuf = new char[size];
    cmd.serialize(cbuf, size);
    std::string buf(cbuf, size);
    m << size;
    m << buf;
    delete [] cbuf;
    return m;
}

unmarshall& operator>>(unmarshall &u, kv_command& cmd) {
    // Your code here:
    std::string buf;
    int size;
    u >> size;
    u >> buf;
    cmd.deserialize(buf.c_str(), size);
    return u;
}

kv_state_machine::~kv_state_machine() {

}

void kv_state_machine::apply_log(raft_command &cmd) {
    kv_command &kv_cmd = dynamic_cast<kv_command&>(cmd);
    std::unique_lock<std::mutex> lock(kv_cmd.res->mtx);
    // Your code here:
    switch(kv_cmd.cmd_tp){
        case kv_command::CMD_NONE:
            non(kv_cmd);
            break;
        case kv_command::CMD_GET:
            get(kv_cmd);
            break;
        case kv_command::CMD_PUT:
            put(kv_cmd);
            break;
        case kv_command::CMD_DEL:
            del(kv_cmd);
            break;
        default:
            break;

    }
    kv_cmd.res->done = true;
    kv_cmd.res->cv.notify_all();
    return;
}

std::vector<char> kv_state_machine::snapshot() {
    // Your code here:
    std::unique_lock<std::mutex> lock(mtx);
    std::string buf;
    for(const auto& pair : store){
        int key_len = pair.first.size();
        int value_len = pair.second.size();
        buf.append((const char*)&key_len, 4);
        buf.append(pair.first);
        buf.append((const char*)&value_len, 4);
        buf.append(pair.second);
    }
    std::vector<char> snapshot_(buf.begin(), buf.end());
    return snapshot_;
}

void kv_state_machine::apply_snapshot(const std::vector<char>& snapshot) {
    // Your code here:
    std::unique_lock<std::mutex> lock(mtx);
    std::string buf(snapshot.begin(), snapshot.end());
    char* cbuf = new char[buf.size()];
    memcpy(cbuf, buf.c_str(), buf.size());
    int ptr = 0, size = buf.size();
    int key_len, val_len;
    while(ptr != size){
        memcpy((char*)&key_len, cbuf + ptr, 4);
        std::string key(cbuf + ptr + 4, key_len);
        ptr += key_len + 4;
        memcpy((char*)&val_len, cbuf + ptr, 4);
        std::string val(cbuf + ptr + 4, val_len);
        ptr += val_len + 4;
        store[key] = val;
    }
    delete [] cbuf;
    return;    
}

bool kv_state_machine::contains(const std::string& key){
    auto itr = store.find(key);
    if(itr!=store.end()){
        return true;
    }
    else{
        return false;
    }
}

void kv_state_machine::non(kv_command& kv_cmd) {
    kv_cmd.res->key = "";
    kv_cmd.res->value = "";
    kv_cmd.res->succ = true;
}

void kv_state_machine::get(kv_command& kv_cmd) {
    kv_cmd.res->key = kv_cmd.key;
    if(contains(kv_cmd.key)){
        kv_cmd.res->value = store[kv_cmd.key];
        kv_cmd.res->succ = true;
    }
    else{
        kv_cmd.res->value = "";
        kv_cmd.res->succ = false;
    }
}

void kv_state_machine::put(kv_command& kv_cmd) {
    kv_cmd.res->key = kv_cmd.key;
    if(contains(kv_cmd.key)){
        kv_cmd.res->value = store[kv_cmd.key];
        store[kv_cmd.key] = kv_cmd.value;
        kv_cmd.res->succ = false;
    }
    else{
        kv_cmd.res->value = kv_cmd.value;
        store[kv_cmd.key] = kv_cmd.value;
        kv_cmd.res->succ = true;
    }
}

void kv_state_machine::del(kv_command& kv_cmd) {
    kv_cmd.res->key = kv_cmd.key;
    if(contains(kv_cmd.key)){
        kv_cmd.res->value = store[kv_cmd.key];
        store.erase(kv_cmd.key);
        kv_cmd.res->succ = true;
    }
    else{
        kv_cmd.res->value = "";
        kv_cmd.res->succ = false;
    }
}