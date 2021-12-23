#include "tx_region.h"
#include <set>
#include <thread>

#if PART_3
void tx_region::replay() {
    bool cond = true;
    while(cond) {
        cond = false;
        db->latch.lock();
        std::set<int> mtxs;
        for(auto &his : histories) {
            if(mtxs.find(his.key)!=mtxs.end() || db->key_locks[his.key].try_lock()) {
                db->key_locks[his.key].tx_id = this->tx_id;
                mtxs.insert(this->tx_id);
            }
            else if(db->key_locks[his.key].tx_id > this->tx_id) {
                db->latch.unlock();
                db->key_locks[his.key].lock();
                db->latch.lock();
                db->key_locks[his.key].tx_id = this->tx_id;
                mtxs.insert(this->tx_id);
            }
            else{
                cond = true;
                for(auto i :mtxs) {
                    db->key_locks[i].tx_id = -1;
                    db->key_locks[i].unlock();
                }
                std::this_thread::sleep_for(std::chrono::milliseconds(50));
                break;
            }
        }
        db->latch.unlock();
    }

    for(auto &his : histories) {
        if(his.op==1) {
            int r;
            this->db->vserver->execute(
                his.key, 
                chdb_protocol::Put, 
                chdb_protocol::operation_var{.tx_id = tx_id, .key = his.key, .value = his.val}, 
                r
            );
            int offset = db->vserver->dispatch(his.key, db->vserver->shard_num());
            targets[offset] = his.key;
            prepare = prepare & r;
        }
    }
}

bool tx_region::inner_get(const int key, int &r) {
    db->latch.lock();
    bool ret;
    if(db->key_locks[key].try_lock() || db->key_locks[key].tx_id==tx_id) {
        db->key_locks[key].tx_id = tx_id;
        this->db->vserver->execute(
            key, 
            chdb_protocol::Get,
            chdb_protocol::operation_var{.tx_id = tx_id, .key = key, .value = 0},
            r
        );
        ret = true;
    }
    else if(db->key_locks[key].tx_id > tx_id) {
        db->latch.unlock();
        db->key_locks[key].lock();
        db->latch.lock();
        db->key_locks[key].tx_id = tx_id;
        this->db->vserver->execute(
            key, 
            chdb_protocol::Get,
            chdb_protocol::operation_var{.tx_id = tx_id, .key = key, .value = 0},
            r
        );
        ret = true;
    }
    else{
        ret = false;
    }
    if(ret){
        histories.push_back(history(2, key));
    }
    db->latch.unlock();
    return ret;
}

bool tx_region::inner_put(const int key, const int val) {
    db->latch.lock();
    bool ret;
    if(db->key_locks[key].try_lock() || db->key_locks[key].tx_id==tx_id) {
        db->key_locks[key].tx_id = tx_id;
        int r;
        this->db->vserver->execute(
            key, 
            chdb_protocol::Put, 
            chdb_protocol::operation_var{.tx_id = tx_id, .key = key, .value = val}, 
            r
        );
        int offset = db->vserver->dispatch(key, db->vserver->shard_num());
        targets[offset] = key;
        prepare = prepare & r;
        ret = true;
    }
    else if(db->key_locks[key].tx_id > tx_id) {
        db->latch.unlock();
        db->key_locks[key].lock();
        db->latch.lock();
        db->key_locks[key].tx_id = tx_id;
        int r;
        this->db->vserver->execute(
            key, 
            chdb_protocol::Put, 
            chdb_protocol::operation_var{.tx_id = tx_id, .key = key, .value = val}, 
            r
        );
        int offset = db->vserver->dispatch(key, db->vserver->shard_num());
        targets[offset] = key;
        prepare = prepare & r;
        ret = true;
    }
    else{
        ret = false;
    }
    if(ret){
        histories.push_back(history(1, key, val));
    }
    db->latch.unlock();
    return ret;
}

#endif

int tx_region::put(const int key, const int val) {
    #if PART_3

    {
        std::unique_lock<std::mutex> lock(db->latch);
        if(db->key_locks.find(key)==db->key_locks.end()){
            db->key_locks[key] = chdb::key_lock(key);
        }
    }
    bool succ = inner_put(key, val);
    while(!succ) {
        tx_abort();
        std::this_thread::sleep_for(std::chrono::milliseconds(50));
        replay();
        succ = inner_put(key, val);
    }
    return 1;

    #else

    int r;
    this->db->vserver->execute(
        key, 
        chdb_protocol::Put, 
        chdb_protocol::operation_var{.tx_id = tx_id, .key = key, .value = val}, 
        r
    );
    int offset = db->vserver->dispatch(key, db->vserver->shard_num());
    targets[offset] = key;
    prepare = prepare & r;
    return r;

    #endif
}

int tx_region::get(const int key) {
    // TODO: Your code here
    #if PART_3

    {
        std::unique_lock<std::mutex> lock(db->latch);
        if(db->key_locks.find(key)==db->key_locks.end()){
            db->key_locks[key] = chdb::key_lock(key);
        }
    }
    int r;
    bool succ = inner_get(key, r);
    while(!succ) {
        tx_abort();
        std::this_thread::sleep_for(std::chrono::milliseconds(50));
        replay();
        succ = inner_get(key, r);
    }
    return r;

    #else

    int r;
    this->db->vserver->execute(
        key, 
        chdb_protocol::Get,
        chdb_protocol::operation_var{.tx_id = tx_id, .key = key, .value = 0},
        r
    );
    return r;

    #endif

}

int tx_region::tx_can_commit() {
    // TODO: Your code here
    int r;
    for(auto &pair : targets) {
        db->vserver->execute(
            pair.second,
            chdb_protocol::Prepare,
            chdb_protocol::prepare_var{.tx_id = tx_id},
            r
        );
        prepare = prepare & r;
    }
    if(prepare==1){
        return chdb_protocol::prepare_ok;
    }
    else{
        return chdb_protocol::prepare_not_ok;
    }
}

int tx_region::tx_begin() {
    // TODO: Your code here
    #if BIG_LOCK
    db->big_lock.lock();
    #endif
    printf("tx[%d] begin\n", tx_id);
    return 0;
}

int tx_region::tx_commit() {
    // TODO: Your code here
    int r;
    for(auto &pair : targets) {
        db->vserver->execute(
            pair.second,
            chdb_protocol::Commit,
            chdb_protocol::commit_var{.tx_id = tx_id},
            r
        );
    }
    printf("tx[%d] commit\n", tx_id);

    #if BIG_LOCK
    db->big_lock.unlock();
    #endif

    #if PART_3
    std::set<int> keys;
    for(auto &his : histories) {
        keys.insert(his.key);
    }
    db->latch.lock();
    for(auto i : keys) {
        db->key_locks[i].tx_id = -1;
        db->key_locks[i].unlock();
    }
    db->latch.unlock();
    #endif

    return 0;
}

int tx_region::tx_abort() {
    // TODO: Your code here
    int r;
    for(auto &pair : targets) {
        db->vserver->execute(
            pair.second,
            chdb_protocol::Rollback,
            chdb_protocol::rollback_var{.tx_id = tx_id},
            r
        );
    }
    printf("tx[%d] abort\n", tx_id);

    #if BIG_LOCK
    db->big_lock.unlock();
    #endif

    #if PART_3
    std::set<int> keys;
    for(auto &his : histories) {
        keys.insert(his.key);
    }
    db->latch.lock();
    for(auto i : keys) {
        db->key_locks[i].tx_id = -1;
        db->key_locks[i].unlock();
    }
    db->latch.unlock();
    #endif

    return 0;
}
