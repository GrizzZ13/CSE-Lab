#include "ch_db.h"
#include <thread>

int view_server::execute(unsigned int query_key, unsigned int proc, const chdb_protocol::operation_var &var, int &r) {
    // TODO: Your code here
    #if RAFT_GROUP

    int term, index;
    chdb_command cmd;
    cmd.key = var.key;
    cmd.value = var.value;
    cmd.tx_id = var.tx_id;
    if(proc==chdb_protocol::Put) {
        cmd.cmd_tp = chdb_command::CMD_PUT;
    }
    else if(proc==chdb_protocol::Get) {
        cmd.cmd_tp = chdb_command::CMD_GET;
    }
    else{
        assert(0);
    }
    int leader = raft_group->check_exact_one_leader();
    {
        std::unique_lock<std::mutex> lock(cmd.res->mtx);
        raft_group->nodes[leader]->new_command(cmd, term, index);
        while(!cmd.res->done){
            cmd.res->cv.wait_until(lock, std::chrono::system_clock::now() + std::chrono::milliseconds(1000));
        }
    }

    #endif

    int base_port = this->node->port();
    int shard_offset = this->dispatch(query_key, shard_num());
    return this->node->template call(base_port + shard_offset, proc, var, r);

    
}

int view_server::execute(unsigned int query_key, unsigned int proc, const chdb_protocol::rollback_var &var, int &r) {
    // TODO: Your code here
    int base_port = this->node->port();
    int shard_offset = this->dispatch(query_key, shard_num());

    return this->node->template call(base_port + shard_offset, proc, var, r);
}

int view_server::execute(unsigned int query_key, unsigned int proc, const chdb_protocol::commit_var &var, int &r) {
    // TODO: Your code here
    int base_port = this->node->port();
    int shard_offset = this->dispatch(query_key, shard_num());

    return this->node->template call(base_port + shard_offset, proc, var, r);
}

int view_server::execute(unsigned int query_key, unsigned int proc, const chdb_protocol::prepare_var &var, int &r) {
    // TODO: Your code here
    int base_port = this->node->port();
    int shard_offset = this->dispatch(query_key, shard_num());

    return this->node->template call(base_port + shard_offset, proc, var, r);
}

view_server::~view_server() {
#if RAFT_GROUP
    delete this->raft_group;
#endif
    delete this->node;

}