#include "tx_region.h"
#include <set>
#include <thread>


int tx_region::put(const int key, const int val) {

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

}

int tx_region::get(const int key) {
    // TODO: Your code here

    int r;
    this->db->vserver->execute(
        key, 
        chdb_protocol::Get,
        chdb_protocol::operation_var{.tx_id = tx_id, .key = key, .value = 0},
        r
    );
    return r;

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
    return 0;
}
