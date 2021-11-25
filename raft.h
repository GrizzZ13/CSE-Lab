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

// #define ENABLE_LOG
// #define OUTPUT_RESTORE

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
    int last_included_term;
    int last_included_index;
    int commit_index;
    int last_applied;
    std::vector<log_entry<command>> log_entries;
    std::vector<char> data;
    std::vector<int> next_index;
    std::vector<int> match_index;

    int pingcnt;


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
    void start_election();
    void ping();
    void try_commit();

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
    std::unique_lock<std::mutex> lock(mtx);
    #ifdef ENABLE_LOG
    RAFT_LOG("init raft server");
    #endif
    pingcnt = 0;
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

    follower_election_timeout = std::chrono::milliseconds((rand() % 200) + 300);
    candidate_election_timeout = std::chrono::milliseconds((rand() % 200) + 900);
    ping_sent_timeout = std::chrono::milliseconds(150);

    last_log_term = 0;
    last_log_index = 0;
    last_included_term = -1;
    last_included_index = -1;
    last_applied = 0;
    commit_index = 0;
    next_index = std::vector<int>(num_nodes(), 1);
    match_index = std::vector<int>(num_nodes(), 0);
    command cmd;
    log_entries.push_back(log_entry<command>(0, 0, cmd));

    if(storage->restore(current_term, log_entries)){
        if(!log_entries.empty()){
            last_log_index = log_entries[log_entries.size()-1].index;
            last_log_term = log_entries[log_entries.size()-1].term;
        }
    }
    if(storage->read_snapshot(last_included_index, last_included_term, data)){
        state->apply_snapshot(data);
        commit_index = last_included_index;
        last_applied = last_included_index;
        last_log_term = last_log_term == 0 ? last_included_term : last_log_term;
        last_log_index = last_log_index == 0 ? last_included_index : last_log_index;
    }
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
    
    #ifdef ENABLE_LOG
    RAFT_LOG("new command %d", my_id);
    #endif
    index = next_log_index();
    term = current_term;
    log_entry<command> entry(term, index, cmd);
    log_entries.push_back(entry);
    last_log_term = entry.term;
    last_log_index = entry.index;
    next_index[my_id] = index + 1;
    match_index[my_id] = index;
    storage->store(current_term, log_entries);
    try_commit();
    return true;
}

template<typename state_machine, typename command>
bool raft<state_machine, command>::save_snapshot() {
    // Your code here:
    std::unique_lock<std::mutex> lock(mtx);
    #ifdef ENABLE_LOG
    RAFT_LOG("save log");
    #endif
    auto itr = log_entries.begin();
    for(;itr!=log_entries.end();++itr){
        if((*itr).index > last_applied){
            break;
        }
    }
    if(itr==log_entries.begin()){
        return false;
    }
    else{
        auto last_included = itr-1;
        last_included_term = (*last_included).term;
        last_included_index = (*last_included).index;
        data = state->snapshot();
        log_entries.erase(log_entries.begin(), itr);
        storage->save_snapshot(last_included_index, last_included_term, data);
        storage->store(current_term, log_entries);
        #ifdef ENABLE_LOG
        RAFT_LOG("state machine length %d", state->store.size());
        #endif
        return true;
    }
}



/******************************************************************

                         RPC Related

*******************************************************************/
template<typename state_machine, typename command>
int raft<state_machine, command>::request_vote(request_vote_args args, request_vote_reply& reply) {
    // Your code here:
    std::unique_lock<std::mutex> lock(mtx);
    #ifdef ENABLE_LOG
    RAFT_LOG("current term %d last log index %d last log term %d ping count %d", current_term, last_log_index, last_log_term, pingcnt);
    #endif
    last_received_rpc = std::chrono::system_clock::now();
    follower_election_timeout = std::chrono::milliseconds((rand() % 200) + 300);
    if(args.term < current_term){
        reply.term = current_term;
        reply.voteGranted = false;
    }
    // args.term >= current_term
    else{
        if(args.term > current_term){
            current_term = args.term;
            storage->store(current_term, log_entries);
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
        storage->store(current_term, log_entries);
        role = follower;
    }
    else if(reply.voteGranted==true && role==candidate){
        granted++;
        int size = num_nodes();
        if(granted >= size/2 + 1){
            role = leader;
            #ifdef ENABLE_LOG
            RAFT_LOG("leader %d", my_id);
            #endif
            // ensure that only those commands who come in this term will be commited
            // but those who came earlier and haven't been commited will not be committd immediately
            for(int i = 0;i < size;++i){
                next_index[i] = next_log_index();
                match_index[i] = 0;
            }
        }
    }
}


template<typename state_machine, typename command>
int raft<state_machine, command>::append_entries(append_entries_args<command> arg, append_entries_reply& reply) {
    // Your code here:
    std::unique_lock<std::mutex> lock(mtx);
    last_received_rpc = std::chrono::system_clock::now();
    follower_election_timeout = std::chrono::milliseconds((rand() % 200) + 300);
    // heartbeat with empty body
    if(arg.log_entries.size()==0){
        if(arg.term >= current_term){
            role = follower;
            if(current_term < arg.term){
                current_term = arg.term;
                storage->store(current_term, log_entries);
            }
            if(arg.leaderCommit > commit_index){
                if(next_log_index() <= arg.leaderCommit || last_log_term != arg.prevLogTerm || last_log_index != arg.prevLogIndex){
                    commit_index = commit_index;
                }
                else{
                    commit_index = arg.leaderCommit > last_log_index ? last_log_index : arg.leaderCommit;
                }
            }
            reply.term = arg.term;
            reply.success = true;
            reply.match_index = commit_index;
            pingcnt++;
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
            bool modified = false;
            if(current_term < arg.term){
                current_term = arg.term;
                role = follower;
                modified = true;
            }
            if(next_log_index() <= arg.prevLogIndex){
                reply.term = arg.term;
                reply.success = false;
            }
            else if(arg.prevLogIndex <= last_included_index){
                auto itr_1  = arg.log_entries.begin();
                for(;itr_1!=log_entries.end();++itr_1){
                    if((*itr_1).index==last_included_index+1){
                        break;
                    }
                }
                log_entries.clear();
                log_entries.insert(log_entries.end(), itr_1, arg.log_entries.end());
                if(log_entries.size()==0){
                    last_log_index = last_included_index;
                    last_log_term = last_included_term;
                }
                else{
                    last_log_index = log_entries[log_entries.size()-1].index;
                    last_log_term = log_entries[log_entries.size()-1].term;
                }
                reply.term = arg.term;
                reply.success = true;
                reply.match_index = last_log_index;
                modified = true;
            }
            else{
                auto itr_0 = log_entries.begin();
                for(;itr_0!=log_entries.end();++itr_0){
                    if((*itr_0).index == arg.prevLogIndex){
                        break;
                    }
                }

                if(itr_0 == log_entries.end() ||  (*itr_0).term != arg.prevLogTerm){
                    reply.term = arg.term;
                    reply.success = false;
                    auto itr_1 = itr_0;
                    auto itr_2 = log_entries.end();
                    if(itr_1!=itr_2){
                        log_entries.erase(itr_1, itr_2);
                        modified = true;
                    }
                }
                else{
                    #ifdef ENABLE_LOG
                    RAFT_LOG("commit index %d log size %d arg last log %d", commit_index, log_entries.size(), (*(arg.log_entries.end()-1)).index);
                    #endif
                    auto itr_1 = itr_0 + 1;
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
                        modified = true;
                    }
                    if(itr_3!=itr_4){
                        log_entries.insert(log_entries.end(), itr_3, itr_4);
                        modified = true;
                    }
                    last_log_index = log_entries[log_entries.size()-1].index;
                    if(arg.leaderCommit > commit_index){
                        commit_index = arg.leaderCommit > last_log_index ? last_log_index : arg.leaderCommit;
                    }
                    reply.term = arg.term;
                    reply.success = true;
                    reply.match_index = last_log_index;
                }
                #ifdef ENABLE_LOG
                RAFT_LOG("server %d commit index %d", my_id, commit_index);
                #endif
                last_log_term = log_entries[log_entries.size()-1].term;
                last_log_index = log_entries[log_entries.size()-1].index;
            }
            if(modified){
                storage->store(current_term, log_entries);
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
        storage->store(current_term, log_entries);
        role = follower;
    }
    else if(role == leader){
        if(reply.success == true){
            next_index[target] = reply.match_index + 1;
            match_index[target] = reply.match_index;
            std::vector<int> temp = match_index;
            std::sort (temp.begin(), temp.end());
            #ifdef ENABLE_LOG
            // std::string buf;
            // for(auto i : match_index){
            //     buf += std::to_string(i) + " ";
            // }
            // RAFT_LOG("match array %s", buf.c_str());
            #endif
            int N = temp[(temp.size()-1)/2];
            if(N > commit_index && log_entries[N-last_included_index-1].term == current_term){
                commit_index = N;
                #ifdef ENABLE_LOG
                RAFT_LOG("leader : commit index %d", commit_index);
                #endif
            }
        }
        else{
            #ifdef ENABLE_LOG
            RAFT_LOG("append-entries failed");
            #endif
            if(next_index[target]>1){
                next_index[target]--;
            }
        }
    }
    return;
}


template<typename state_machine, typename command>
int raft<state_machine, command>::install_snapshot(install_snapshot_args args, install_snapshot_reply& reply) {
    // Your code here:
    std::unique_lock<std::mutex> lock(mtx);
    if(args.term >= current_term){
        current_term = args.term;
        reply.term = current_term;
        auto itr = log_entries.begin();
        for(;itr!=log_entries.end();++itr){
            if((*itr).index > args.lastIncludedIndex){
                break;
            }
        }
        log_entries.erase(log_entries.begin(), itr);
        std::vector<char> data_(args.data.begin(), args.data.end());
        data.swap(data_);
        last_included_index = args.lastIncludedIndex;
        last_included_term = args.lastIncludedTerm;
        commit_index = last_included_index;
        last_applied = last_included_index;
        if(log_entries.empty()){
            last_log_index = last_included_index;
            last_log_term = last_included_term;
        }
        else{
            last_log_index = log_entries[log_entries.size()-1].index;
            last_log_term = log_entries[log_entries.size()-1].term;
        }
        state->apply_snapshot(data);
        storage->save_snapshot(last_included_index, last_included_term, data);
        storage->store(current_term, log_entries);
    }
    else{
        reply.term = current_term;
    }
    return 0;
}


template<typename state_machine, typename command>
void raft<state_machine, command>::handle_install_snapshot_reply(int target, const install_snapshot_args& arg, const install_snapshot_reply& reply) {
    // Your code here:
    std::unique_lock<std::mutex> lock(mtx);
    if(reply.term > current_term){
        current_term = reply.term;
        storage->store(current_term, log_entries);
        role = follower;
    }
    else{
        next_index[target] = arg.lastIncludedIndex + 1;
        match_index[target] = arg.lastIncludedIndex;
    }
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

    while (true) {
        if (is_stopped()) return;
        // Your code here:
        {
            std::unique_lock<std::mutex> lock(mtx);
            std::chrono::system_clock::time_point current_time = std::chrono::system_clock::now();
            if(role==follower && current_time - last_received_rpc > follower_election_timeout){
                start_election();
            }
            else if(role==candidate && current_time - last_election > candidate_election_timeout){
                start_election();
                candidate_election_timeout = std::chrono::milliseconds((rand() % 200) + 900);
            }
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
        {
            std::unique_lock<std::mutex> lock(mtx);
            try_commit();
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
        {
            std::unique_lock<std::mutex> lock(mtx);
            if(commit_index > last_applied){
                #ifdef ENABLE_LOG
                // int i = 0;
                // for(auto &entry : log_entries){
                //     i++;
                //     RAFT_LOG("index %d value %d", i, entry.cmd.value);
                // }
                #endif
                while(last_applied < commit_index){
                    last_applied++;
                    state->apply_log(log_entries[last_applied - last_included_index - 1].cmd);
                    #ifdef ENABLE_LOG
                    RAFT_LOG("apply log to index %d last included index %d", last_applied, last_included_index);
                    #endif
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
        {
            std::unique_lock<std::mutex> lock(mtx);
            std::chrono::system_clock::time_point current_time = std::chrono::system_clock::now();
            if(role==leader && current_time - last_ping_sent > ping_sent_timeout){
                ping();
            }
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
    return last_log_index + 1;
}

template<typename state_machine, typename command>
void raft<state_machine, command>::start_election() {
    last_election = std::chrono::system_clock::now();
    current_term++;
    storage->store(current_term, log_entries);
    role = candidate;
    granted = 1;
    #ifdef ENABLE_LOG
    RAFT_LOG("start election : last log term %d last log index %d commit index %d", log_entries[log_entries.size()-1].term, log_entries[log_entries.size()-1].index, commit_index);
    #endif
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
    append_entries_args<command> arg(current_term, my_id, last_log_index, last_log_term, commit_index);
    for (int i = 0; i < client_count; i++){
        if(i!=my_id){
            thread_pool->addObjJob(this, &raft::send_append_entries, i, arg);
        }
    }
}

template<typename state_machine, typename command>
void raft<state_machine, command>::try_commit() {
    if(is_leader(current_term)){
        int size = num_nodes();
        for(int i = 0;i < size;++i){
            if(i!=my_id){
                if(last_log_index >= next_index[i] && next_index[i] > last_included_index){
                    auto itr_1 = log_entries.begin() + next_index[i] - last_included_index - 1;
                    auto itr_2 = log_entries.end();
                    std::vector<log_entry<command>> entries(itr_1, itr_2);
                    int prev_log_index, prev_log_term;
                    if(next_index[i] == last_included_index + 1){
                        prev_log_index = last_included_index;
                        prev_log_term = last_included_term;
                    }
                    else{
                        prev_log_index = (*(itr_1-1)).index;
                        prev_log_term = (*(itr_1-1)).term;
                    }
                    append_entries_args<command> arg(current_term, my_id, prev_log_index, prev_log_term, commit_index, entries);
                    thread_pool->addObjJob(this, &raft::send_append_entries, i, arg);
                }
                else if(last_log_index >= next_index[i] && next_index[i] <= last_included_index){
                    #ifdef ENABLE_LOG
                    RAFT_LOG("send install log to server %d", i);
                    #endif
                    std::string data_(data.begin(), data.end());
                    install_snapshot_args arg(current_term, my_id, last_included_index, last_included_term, data_);
                    thread_pool->addObjJob(this, &raft::send_install_snapshot, i, arg);
                }
            }
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