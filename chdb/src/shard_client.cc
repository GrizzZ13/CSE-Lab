#include "shard_client.h"
#include <thread>

#if PART_3

bool shard_client::redo(std::vector<chdb_log> &redo_logs) {
    bool acquired;
    for(auto &var : redo_logs) {
        while(true){
            mtx_map_lock.lock();
            if(mtx_map[var.key].mtx->try_lock()){
                mtx_map[var.key].tx_id = var.tx_id;
                mtx_map_lock.unlock();
                acquired = true;
                break;
            }
            else{
                if(mtx_map[var.key].tx_id > var.tx_id){
                    mtx_map_lock.unlock();
                    std::this_thread::sleep_for(std::chrono::milliseconds(10));
                }
                else{
                    acquired = false;
                    mtx_map_lock.unlock();
                    break;
                }
            }
        }
        if(acquired = false) {
            break;
        }
    }
    if(acquired = false) {
        mtx_map_lock.lock();
        for(auto &var : redo_logs) {
            if(mtx_map[var.key].tx_id == var.tx_id){
                mtx_map[var.key].tx_id = -1;
                mtx_map[var.key].mtx->unlock();
            }
        }
        mtx_map_lock.unlock();
        return false;
    }
    else{
        for(auto &log : redo_logs) {
            logs.push_back(log);
        }
        return true;
    }
}

#endif

int shard_client::put(chdb_protocol::operation_var var, int &r) {
    // TODO: Your code here
    #if PART_3
    {
        mtx_map_lock.lock();
        if(mtx_map.find(var.key)==mtx_map.end()){
            mtx_map[var.key] = my_lock();
        }
        mtx_map_lock.unlock();
    }
    bool ifRedo = false;
    std::vector<chdb_log> redo_logs;
    while(true) {
        if(ifRedo){
            if(!redo(redo_logs)){
                continue;
            }
            else{
                redo_logs.clear();
            }
        }

        mtx_map_lock.lock();
        if(mtx_map[var.key].mtx->try_lock()){
            mtx_map[var.key].tx_id = var.tx_id;
            mtx_map_lock.unlock();

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
        else{
            if(mtx_map[var.key].tx_id > var.tx_id){
                mtx_map_lock.unlock();
                std::this_thread::sleep_for(std::chrono::milliseconds(10));
            }
            else{
                std::vector<chdb_log> new_logs;
                for(auto &log : logs) {
                    if(log.tx_id==var.tx_id){
                        if(mtx_map[log.key].tx_id == log.tx_id){
                            mtx_map[log.key].mtx->unlock();
                            mtx_map[log.key].tx_id = -1;
                        }
                        redo_logs.push_back(log);
                    }
                    else{
                        new_logs.push_back(log);
                    }
                }
                logs.swap(new_logs);
                ifRedo = true;
                mtx_map_lock.unlock();
            }
        }
    }

    #else

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

    #endif
}

int shard_client::get(chdb_protocol::operation_var var, int &r) {
    // TODO: Your code here

    // #if PART_3
    // {
    //     mtx_map_lock.lock();
    //     if(mtx_map.find(var.key)==mtx_map.end()){
    //         mtx_map[var.key] = my_lock();
    //     }
    //     mtx_map_lock.unlock();
    // }
    // bool ifRedo = false;
    // std::vector<chdb_log> redo_logs;
    // while(true) {
    //     if(ifRedo){
    //         if(!redo(redo_logs)){
    //             continue;
    //         }
    //         else{
    //             redo_logs.clear();
    //         }
    //     }

    //     mtx_map_lock.lock();
    //     if(mtx_map[var.key].mtx->try_lock()){
    //         mtx_map[var.key].tx_id = var.tx_id;
    //         mtx_map_lock.unlock();

    //         chdb_log log;
    //         // a magic number representing null old val
    //         int old_v = magic_number;
    //         std::map<int, value_entry> map1 = get_store();
    //         if(map1.find(var.key)!=map1.end()){
    //             old_v = map1[var.key].value;
    //         }
    //         for(auto &log : logs){
    //             if(log.tx_id==var.tx_id && log.key==var.key){
    //                 old_v = log.new_v;
    //             }
    //         }
    //         log.tx_id = var.tx_id;
    //         log.old_v = old_v;
    //         log.new_v = var.value;
    //         log.key = var.key;
    //         logs.push_back(log);

    //         if(active==false){
    //             r = 0;
    //         }
    //         else{
    //             r = 1;
    //         }
    //         return 0;

    //     }
    //     else{
    //         if(mtx_map[var.key].tx_id > var.tx_id){
    //             mtx_map_lock.unlock();
    //             std::this_thread::sleep_for(std::chrono::milliseconds(100));
    //         }
    //         else{
    //             std::vector<chdb_log> new_logs;
    //             for(auto &log : logs) {
    //                 if(log.tx_id==var.tx_id){
    //                     if(mtx_map[log.key].tx_id == log.tx_id){
    //                         mtx_map[log.key].mtx->unlock();
    //                         mtx_map[log.key].tx_id = -1;
    //                     }
    //                     redo_logs.push_back(log);
    //                 }
    //                 else{
    //                     new_logs.push_back(log);
    //                 }
    //             }
    //             logs.swap(new_logs);
    //             ifRedo = true;
    //             mtx_map_lock.unlock();
    //         }
    //     }
    // }

    // #else

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

    // #endif
}

int shard_client::commit(chdb_protocol::commit_var var, int &r) {
    // TODO: Your code here
    #if PART_3
    mtx_map_lock.lock();

    for(auto &log : logs) {
        if(log.tx_id==var.tx_id){
            for(auto &map1 : store){
                map1[log.key].value = log.new_v;
            }
            mtx_map[log.key].mtx->unlock();
            mtx_map[log.key].tx_id = -1;
        }
    }

    mtx_map_lock.unlock();

    return 0;

    #else

    for(auto &log : logs) {
        for(auto &map1 : store){
            map1[log.key].value = log.new_v;
        }
    }
    
    return 0;

    #endif
}

int shard_client::rollback(chdb_protocol::rollback_var var, int &r) {
    // TODO: Your code here
    #if PART_3
    mtx_map_lock.lock();
    size_t size = logs.size();
    for(size_t i = size;i>0;--i){
        if(var.tx_id!=logs[i-1].tx_id){
            continue;
        }
        mtx_map[logs[i-1].key].mtx->unlock();
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
    mtx_map_lock.unlock();

    return 0;

    #else

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

    #endif
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