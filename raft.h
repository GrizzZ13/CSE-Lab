#ifndef raft_h
#define raft_h

#include <atomic>
#include <mutex>
#include <chrono>
#include <thread>
#include <ctime>
#include <algorithm>
#include <thread>
#include <stdarg.h>

#include "rpc.h"
#include "raft_storage.h"
#include "raft_protocol.h"
#include "raft_state_machine.h"

template<typename state_machine, typename command>
class raft {

static_assert(std::is_base_of<raft_state_machine, state_machine>(), "state_machine must inherit from raft_state_machine");
static_assert(std::is_base_of<raft_command, command>(), "command must inherit from raft_command");


friend class thread_pool;

#define RAFT_LOG(fmt, args...) \
    do { \
        auto now = \
        std::chrono::duration_cast<std::chrono::milliseconds>(\
            std::chrono::system_clock::now().time_since_epoch()\
        ).count();\
        printf("[%ld][%s:%d][node %d term %d] " fmt "\n", now, __FILE__, __LINE__, my_id, current_term, ##args); \
    } while(0);

public:
    raft(
        rpcs* rpc_server,
        std::vector<rpcc*> rpc_clients,
        int idx, 
        raft_storage<command>* storage,
        state_machine* state    
    );
    ~raft();

    // start the raft node.
    // Please make sure all of the rpc request handlers have been registered before this method.
    void start();

    // stop the raft node. 
    // Please make sure all of the background threads are joined in this method.
    // Notice: you should check whether is server should be stopped by calling is_stopped(). 
    //         Once it returns true, you should break all of your long-running loops in the background threads.
    void stop();

    // send a new command to the raft nodes.
    // This method returns true if this raft node is the leader that successfully appends the log.
    // If this node is not the leader, returns false. 
    bool new_command(command cmd, int &term, int &index);

    // returns whether this node is the leader, you should also set the current term;
    bool is_leader(int &term);

    // save a snapshot of the state machine and compact the log.
    bool save_snapshot();

private:
    std::mutex mtx;                     // A big lock to protect the whole data structure
    ThrPool* thread_pool;
    raft_storage<command>* storage;              // To persist the raft log
    state_machine* state;  // The state machine that applies the raft log, e.g. a kv store

    rpcs* rpc_server;               // RPC server to recieve and handle the RPC requests
    std::vector<rpcc*> rpc_clients; // RPC clients of all raft nodes including this node
    int my_id;                     // The index of this node in rpc_clients, start from 0

    std::atomic_bool stopped;

    enum raft_role {
        follower,
        candidate,
        leader
    };
    raft_role role;
    int current_term;

    std::thread* background_election;
    std::thread* background_ping;
    std::thread* background_commit;
    std::thread* background_apply;

    // Your code here:
    int granted;

    std::chrono::system_clock::time_point last_received_rpc;
    std::chrono::system_clock::time_point last_election;
    std::chrono::system_clock::time_point last_ping_sent;

    std::chrono::milliseconds follower_election_timeout;
    std::chrono::milliseconds candidate_election_timeout;
    std::chrono::milliseconds ping_sent_timeout;

    int last_log_term;
    int last_log_index;
    int commit_index;
    int last_applied;
    std::vector<log_entry<command>> log_entries;
    std::vector<int> next_index;
    std::vector<int> match_index;


private:
    // RPC handlers
    int request_vote(request_vote_args arg, request_vote_reply& reply);

    int append_entries(append_entries_args<command> arg, append_entries_reply& reply);

    int install_snapshot(install_snapshot_args arg, install_snapshot_reply& reply);

    // RPC helpers
    void send_request_vote(int target, request_vote_args arg);
    void handle_request_vote_reply(int target, const request_vote_args& arg, const request_vote_reply& reply);

    void send_append_entries(int target, append_entries_args<command> arg);
    void handle_append_entries_reply(int target, const append_entries_args<command>& arg, const append_entries_reply& reply);

    void send_install_snapshot(int target, install_snapshot_args arg);
    void handle_install_snapshot_reply(int target, const install_snapshot_args& arg, const install_snapshot_reply& reply);


private:
    bool is_stopped();
    int num_nodes() {return rpc_clients.size();}

    // background workers    
    void run_background_ping();
    void run_background_election();
    void run_background_commit();
    void run_background_apply();

    // Your code here:
    int next_log_index();
    int majority(const std::vector<int>& vec);
    log_entry<command> last_log_entry();
    void start_election();
    void ping();

};

template<typename state_machine, typename command>
raft<state_machine, command>::raft(rpcs* server, std::vector<rpcc*> clients, int idx, raft_storage<command> *storage, state_machine *state) :
    storage(storage),
    state(state),   
    rpc_server(server),
    rpc_clients(clients),
    my_id(idx),
    stopped(false),
    role(follower),
    current_term(0),
    background_election(nullptr),
    background_ping(nullptr),
    background_commit(nullptr),
    background_apply(nullptr)
{
    thread_pool = new ThrPool(32);

    // Register the rpcs.
    rpc_server->reg(raft_rpc_opcodes::op_request_vote, this, &raft::request_vote);
    rpc_server->reg(raft_rpc_opcodes::op_append_entries, this, &raft::append_entries);
    rpc_server->reg(raft_rpc_opcodes::op_install_snapshot, this, &raft::install_snapshot);

    // Your code here: 
    // Do the initialization
    granted = 0;

    last_received_rpc = std::chrono::system_clock::now();
    last_election =  std::chrono::system_clock::now();
    last_ping_sent =  std::chrono::system_clock::now();

    follower_election_timeout = std::chrono::milliseconds((rand()%(500-300)) + 300);
    candidate_election_timeout = std::chrono::milliseconds(1000);
    ping_sent_timeout = std::chrono::milliseconds(200);

    last_log_term = 0;
    last_log_index = 0;
    last_applied = 0;
    commit_index = 0;

    command cmd;
    log_entries.push_back(log_entry<command>(0, 0, cmd));
    next_index = std::vector<int>(num_nodes(), 1);
    match_index = std::vector<int>(num_nodes(), 0);
}

template<typename state_machine, typename command>
raft<state_machine, command>::~raft() {
    if (background_ping) {
        delete background_ping;
    }
    if (background_election) {
        delete background_election;
    }
    if (background_commit) {
        delete background_commit;
    }
    if (background_apply) {
        delete background_apply;
    }
    delete thread_pool;    
}

/******************************************************************

                        Public Interfaces

*******************************************************************/

template<typename state_machine, typename command>
void raft<state_machine, command>::stop() {
    stopped.store(true);
    background_ping->join();
    background_election->join();
    background_commit->join();
    background_apply->join();
    thread_pool->destroy();
}

template<typename state_machine, typename command>
bool raft<state_machine, command>::is_stopped() {
    return stopped.load();
}

template<typename state_machine, typename command>
bool raft<state_machine, command>::is_leader(int &term) {
    term = current_term;
    return role == leader;
}

template<typename state_machine, typename command>
void raft<state_machine, command>::start() {
    // Your code here:
    
    RAFT_LOG("start");
    this->background_election = new std::thread(&raft::run_background_election, this);
    this->background_ping = new std::thread(&raft::run_background_ping, this);
    this->background_commit = new std::thread(&raft::run_background_commit, this);
    this->background_apply = new std::thread(&raft::run_background_apply, this);
}

template<typename state_machine, typename command>
bool raft<state_machine, command>::new_command(command cmd, int &term, int &index) {
    // Your code here:
    std::unique_lock<std::mutex> lock(mtx);
    if(!is_leader(current_term)){
        return false;
    }
    
    RAFT_LOG("new command %d", my_id);
    index = next_log_index();
    term = current_term;
    log_entry<command> entry(term, index, cmd);
    log_entries.push_back(entry);
    next_index[my_id] = index + 1;
    match_index[my_id] = index;
    return true;
}

template<typename state_machine, typename command>
bool raft<state_machine, command>::save_snapshot() {
    // Your code here:
    return true;
}



/******************************************************************

                         RPC Related

*******************************************************************/
template<typename state_machine, typename command>
int raft<state_machine, command>::request_vote(request_vote_args args, request_vote_reply& reply) {
    // Your code here:
    std::unique_lock<std::mutex> lock(mtx);
    last_received_rpc = std::chrono::system_clock::now();
    if(args.term < current_term){
        reply.term = current_term;
        reply.voteGranted = false;
    }
    // args.term >= current_term
    else{
        if(args.term > current_term){
            current_term = args.term;
            role = follower;
            reply.term = args.term;
            if(last_log_term>args.lastLogTerm || (last_log_term==args.lastLogTerm && last_log_index>args.lastLogIndex)){
                reply.voteGranted = false;
            }
            else{
                reply.voteGranted = true;
            }
        }
        else{
            reply.term = current_term;
            reply.voteGranted = false;
        }
    }
    return 0;
}


template<typename state_machine, typename command>
void raft<state_machine, command>::handle_request_vote_reply(int target, const request_vote_args& arg, const request_vote_reply& reply) {
    // Your code here:
    std::unique_lock<std::mutex> lock(mtx);
    if(reply.term > current_term){
        current_term = reply.term;
        role = follower;
    }
    else if(reply.voteGranted==true && role==candidate){
        granted++;
        int size = num_nodes();
        if(granted >= size/2 + 1){
            role = leader;
            RAFT_LOG("leader %d", my_id);
            // ensure that only those commands who come in this term will be commited
            // but those who came in earlier and haven't been commited will not be committd immediately
            for(int i = 0;i < size;++i){
                next_index[i] = next_log_index();
            }
        }
    }
}


template<typename state_machine, typename command>
int raft<state_machine, command>::append_entries(append_entries_args<command> arg, append_entries_reply& reply) {
    // Your code here:
    last_received_rpc = std::chrono::system_clock::now();
    // heartbeat with empty body
    if(arg.log_entries.size()==0){
        if(arg.term >= current_term){
            role = follower;
            current_term = arg.term;
            int last_new_index = log_entries[log_entries.size()-1].index;
            if(arg.leaderCommit > commit_index){
                std::unique_lock<std::mutex> lock(mtx);
                commit_index = arg.leaderCommit > last_new_index ? last_new_index : arg.leaderCommit;
            }
            reply.term = arg.term;
            reply.success = true;
            reply.match_index = last_new_index;
        }
        else{
            reply.term = current_term;
            reply.success = false;
        }
    }
    else{
        // real append-entries request
        if(arg.term < current_term){
            reply.term = current_term;
            reply.success = false;
        }
        else{
            std::unique_lock<std::mutex> lock(mtx);
            // if(arg.log_entries.size() > 1){
            //     int i = 1;
            //     for(const auto &ent : arg.log_entries){
            //         RAFT_LOG("new [%d] value : %d , term : %d", i, ent.cmd.value, ent.term);
            //         ++i;
            //     }
            //     for(const auto &ent : log_entries){
            //         RAFT_LOG("old [%d] value : %d , term : %d", i, ent.cmd.value, ent.term);
            //         ++i;
            //     }
            // }
            // arg.term >= current_term
            current_term = arg.term;
            role = follower;
            if(log_entries.size() <= arg.prevLogIndex){
                reply.term = arg.term;
                reply.success = false;
            }
            else{
                if(log_entries[arg.prevLogIndex].term != arg.prevLogTerm){
                    reply.term = arg.term;
                    reply.success = false;
                    auto itr_1 = log_entries.begin() + arg.prevLogIndex;
                    auto itr_2 = log_entries.end();
                    log_entries.erase(itr_1, itr_2);
                }
                else{
                    auto itr_1 = log_entries.begin() + arg.prevLogIndex + 1;
                    auto itr_2 = log_entries.end();
                    auto itr_3 = arg.log_entries.begin();
                    auto itr_4 = arg.log_entries.end();
                    while(itr_1!=itr_2 && itr_3!=itr_4){
                        if((*itr_1).term != (*itr_3).term){
                            break;
                        }
                        if((*itr_1).index != (*itr_3).index){
                            break;
                        }
                        ++itr_1;
                        ++itr_3;
                    }
                    if(itr_1!=itr_2){
                        log_entries.erase(itr_1, itr_2);
                    }
                    if(itr_3!=itr_4){
                        log_entries.insert(log_entries.end(), itr_3, itr_4);
                    }
                    int last_new_index = log_entries[log_entries.size()-1].index;
                    if(arg.leaderCommit > commit_index){
                        commit_index = arg.leaderCommit > last_new_index ? last_new_index : arg.leaderCommit;
                    }
                    reply.term = arg.term;
                    reply.success = true;
                    reply.match_index = last_new_index;
                    RAFT_LOG("server %d commit index %d", my_id, commit_index);
                }
            }
        }
    }
    return 0;
}

template<typename state_machine, typename command>
void raft<state_machine, command>::handle_append_entries_reply(int target, const append_entries_args<command>& arg, const append_entries_reply& reply) {
    // Your code here:
    std::unique_lock<std::mutex> lock(mtx);
    if(reply.term > current_term){
        current_term = reply.term;
        role = follower;
    }
    else if(role == leader){
        if(reply.success == true){
            next_index[target] = reply.match_index + 1;
            match_index[target] = reply.match_index;
            int N = majority(match_index);
            if(N > commit_index && log_entries[N].term == current_term){
                commit_index = N;
                RAFT_LOG("leader : commit index %d", commit_index);
            }
        }
        else{
            next_index[target]--;
        }
    }
    return;
}


template<typename state_machine, typename command>
int raft<state_machine, command>::install_snapshot(install_snapshot_args args, install_snapshot_reply& reply) {
    // Your code here:
    return 0;
}


template<typename state_machine, typename command>
void raft<state_machine, command>::handle_install_snapshot_reply(int target, const install_snapshot_args& arg, const install_snapshot_reply& reply) {
    // Your code here:
    return;
}

template<typename state_machine, typename command>
void raft<state_machine, command>::send_request_vote(int target, request_vote_args arg) {
    request_vote_reply reply;
    if (rpc_clients[target]->call(raft_rpc_opcodes::op_request_vote, arg, reply) == 0) {
        handle_request_vote_reply(target, arg, reply);
    } else {
        // RPC fails
    }
}

template<typename state_machine, typename command>
void raft<state_machine, command>::send_append_entries(int target, append_entries_args<command> arg) {
    append_entries_reply reply;
    if (rpc_clients[target]->call(raft_rpc_opcodes::op_append_entries, arg, reply) == 0) {
        handle_append_entries_reply(target, arg, reply);
    } else {
        // RPC fails
    }
}

template<typename state_machine, typename command>
void raft<state_machine, command>::send_install_snapshot(int target, install_snapshot_args arg) {
    install_snapshot_reply reply;
    if (rpc_clients[target]->call(raft_rpc_opcodes::op_install_snapshot, arg, reply) == 0) {
        handle_install_snapshot_reply(target, arg, reply);
    } else {
        // RPC fails
    }
}

/******************************************************************

                        Background Workers

*******************************************************************/

template<typename state_machine, typename command>
void raft<state_machine, command>::run_background_election() {
    // Check the liveness of the leader.
    // Work for followers and candidates.

    // Hints: You should record the time you received the last RPC.
    //        And in this function, you can compare the current time with it.
    //        For example:
    //        if (current_time - last_received_RPC_time > timeout) start_election();
    //        Actually, the timeout should be different between the follower (e.g. 300-500ms) and the candidate (e.g. 1s).

    RAFT_LOG("run background election");
    while (true) {
        if (is_stopped()) return;
        // Your code here:
        std::chrono::system_clock::time_point current_time = std::chrono::system_clock::now();
        if(role==follower && current_time - last_received_rpc > follower_election_timeout){
            start_election();
        }
        else if(role==candidate && current_time - last_election > candidate_election_timeout){
            start_election();
        }

        std::this_thread::sleep_for(std::chrono::milliseconds(10));
    }    
    

    return;
}

template<typename state_machine, typename command>
void raft<state_machine, command>::run_background_commit() {
    // Send logs/snapshots to the follower.
    // Only work for the leader.

    // Hints: You should check the leader's last log index and the follower's next log index.        
    
    while (true) {
        if (is_stopped()) return;
        // Your code here:
        if(is_leader(current_term)){
            int size = num_nodes();
            log_entry<command> last_entry = last_log_entry();
            for(int i = 0;i < size;++i){
                if(i!=my_id){
                    if(last_entry.index >= next_index[i]){
                        auto itr_1 = log_entries.begin() + next_index[i];
                        auto itr_2 = log_entries.end();
                        std::vector<log_entry<command>> entries(itr_1, itr_2);
                        log_entry<command> prev_entry = log_entries[next_index[i]-1];
                        append_entries_args<command> arg(current_term, my_id, prev_entry.index, prev_entry.term, commit_index, entries);
                        thread_pool->addObjJob(this, &raft::send_append_entries, i, arg);
                    }
                }
            }
        }
        
        std::this_thread::sleep_for(std::chrono::milliseconds(10));
    }    
    
    return;
}

template<typename state_machine, typename command>
void raft<state_machine, command>::run_background_apply() {
    // Apply committed logs the state machine
    // Work for all the nodes.

    // Hints: You should check the commit index and the apply index.
    //        Update the apply index and apply the log if commit_index > apply_index

    
    while (true) {
        if (is_stopped()) return;
        // Your code here:
        if(commit_index > last_applied){
            while(last_applied < commit_index){
                last_applied++;
                state->apply_log(log_entries[last_applied].cmd);
                if(role==leader){
                    RAFT_LOG("leader   %d applied log %d with value %d", my_id, last_applied, log_entries[last_applied].cmd.value);
                }
                else{
                    RAFT_LOG("follower %d applied log %d with value %d", my_id, last_applied, log_entries[last_applied].cmd.value);
                }
            }
        }
        
        std::this_thread::sleep_for(std::chrono::milliseconds(10));
    }    
    return;
}

template<typename state_machine, typename command>
void raft<state_machine, command>::run_background_ping() {
    // Send empty append_entries RPC to the followers.

    // Only work for the leader.
    
    while (true) {
        if (is_stopped()) return;
        // Your code here:
        std::chrono::system_clock::time_point current_time = std::chrono::system_clock::now();
        if(role==leader && current_time - last_ping_sent > ping_sent_timeout){
            ping();
        }

        
        std::this_thread::sleep_for(std::chrono::milliseconds(10)); // Change the timeout here!
    }    
    return;
}


/******************************************************************

                        Other functions

*******************************************************************/

template<typename state_machine, typename command>
int raft<state_machine, command>::next_log_index() {
    return log_entries.size();
}

template<typename state_machine, typename command>
log_entry<command> raft<state_machine, command>::last_log_entry() {
    return log_entries.back();
}

template<typename state_machine, typename command>
void raft<state_machine, command>::start_election() {
    last_election = std::chrono::system_clock::now();
    current_term++;
    role = candidate;
    granted = 1;
    int client_count = num_nodes();
    request_vote_args arg(current_term, my_id, last_log_index, last_log_term);
    for (int i = 0; i < client_count; i++){
        if(i!=my_id){
            thread_pool->addObjJob(this, &raft::send_request_vote, i, arg);
        }
    }
}

template<typename state_machine, typename command>
void raft<state_machine, command>::ping() {
    last_ping_sent = std::chrono::system_clock::now();
    int client_count = num_nodes();
    append_entries_args<command> arg(current_term, my_id, 0, 0, commit_index);
    for (int i = 0; i < client_count; i++){
        if(i!=my_id){
            thread_pool->addObjJob(this, &raft::send_append_entries, i, arg);
        }
    }
}

template<typename state_machine, typename command>
int raft<state_machine, command>::majority(const std::vector<int>& vec) {
    int x, cx = 0;
    for(const auto & i : vec){
        if(cx == 0){
            x = i;
            cx = 1;
        }
        else if(x == i){
            ++cx;
        }
        else{
            --cx;
        }
    }
    int count = 0;
    for(const auto & i : vec){
        if(x == i){
            count++;
        }
    }
    if(count >= vec.size()/2 + 1){
        return x;
    }
    else{
        return -1;
    }
}

#endif // raft_h