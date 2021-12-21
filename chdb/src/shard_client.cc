#include "shard_client.h"


int shard_client::put(chdb_protocol::operation_var var, int &r) {
    // TODO: Your code here
    chdb_log log;
    // a magic number representing null old val
    int old_v = magic_number;
    std::map<int, value_entry> map1 = get_store();
    if(map1.find(var.key)!=map1.end()){
        old_v = map1[var.key].value;
    }
    for(auto &log : logs){
        if(log.tx_id==var.tx_id && log.key==var.key){
            old_v = log.new_v;
        }
    }
    log.tx_id = var.tx_id;
    log.old_v = old_v;
    log.new_v = var.value;
    log.key = var.key;
    logs.push_back(log);

    if(active==false){
        r = 0;
    }
    else{
        r = 1;
    }

    return 0;
}

int shard_client::get(chdb_protocol::operation_var var, int &r) {
    // TODO: Your code here
    int val = 0;
    std::map<int, value_entry> map1 = get_store();
    if(map1.find(var.key)!=map1.end()){
        val = map1[var.key].value;
    }
    for(auto &log : logs){
        if(log.tx_id==var.tx_id && log.key==var.key){
            val = log.new_v;
        }
    }
    r = val;
    return 0;
}

int shard_client::commit(chdb_protocol::commit_var var, int &r) {
    // TODO: Your code here
    for(auto &log : logs) {
        for(auto &map1 : store){
            map1[log.key].value = log.new_v;
        }
    }
    return 0;
}

int shard_client::rollback(chdb_protocol::rollback_var var, int &r) {
    // TODO: Your code here
    size_t size = logs.size();
    for(size_t i = size;i>0;--i){
        if(var.tx_id!=logs[i-1].tx_id){
            continue;
        }
        int old_v = logs[i-1].old_v;
        if(old_v==magic_number){
            for(auto &map1 : store) {
                map1.erase(logs[i-1].key);
            }
        }
        else{
            for(auto &map1 : store) {
                map1[logs[i-1].key].value = logs[i-1].old_v;
            }
        }
    }
    return 0;
}

int shard_client::check_prepare_state(chdb_protocol::check_prepare_state_var var, int &r) {
    // TODO: Your code here
    return 0;
}

int shard_client::prepare(chdb_protocol::prepare_var var, int &r) {
    // TODO: Your code here
    if(active==false){
        r = 0;
    }
    else{
        r = 1;
    }
    return 0;
}

shard_client::~shard_client() {
    delete node;
}