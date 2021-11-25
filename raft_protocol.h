#ifndef raft_protocol_h
#define raft_protocol_h

#include "rpc.h"
#include "raft_state_machine.h"

#define MAX_BUF_LEN 8192

enum raft_rpc_opcodes {
    op_request_vote = 0x1212,
    op_append_entries = 0x3434,
    op_install_snapshot = 0x5656
};

enum raft_rpc_status {
   OK,
   RETRY,
   RPCERR,
   NOENT,
   IOERR
};

class request_vote_args {
public:
    // Your code here
    int term;
    int candidateId;
    int lastLogIndex;
    int lastLogTerm;

    request_vote_args(){}
    request_vote_args(int term_, int candidateId_, int lastLogIndex_, int lastLogTerm_){
        term = term_;
        candidateId = candidateId_;
        lastLogIndex = lastLogIndex_;
        lastLogTerm = lastLogTerm_;
    }
};

marshall& operator<<(marshall &m, const request_vote_args& args);
unmarshall& operator>>(unmarshall &u, request_vote_args& args);


class request_vote_reply {
public:
    // Your code here
    int term;
    bool voteGranted;

    request_vote_reply(){}
    request_vote_reply(int term_, bool voteGranted_){
        term = term_;
        voteGranted = voteGranted_;
    }
};

marshall& operator<<(marshall &m, const request_vote_reply& reply);
unmarshall& operator>>(unmarshall &u, request_vote_reply& reply);

template<typename command>
class log_entry {
public:
    // Your code here
    int term;
    int index;
    command cmd;

    log_entry(){}
    log_entry(int term_, int index_, const command& cmd_){
        term = term_;
        index = index_;
        cmd = cmd_;
    }
};

template<typename command>
marshall& operator<<(marshall &m, const log_entry<command>& entry) {
    // Your code here
    m << entry.term;
    m << entry.index;
    char buf[MAX_BUF_LEN];
    int size = entry.cmd.size();
    entry.cmd.serialize(buf, size);
    std::string str(buf, size);
    m << size;
    m << str;
    return m;
}

template<typename command>
unmarshall& operator>>(unmarshall &u, log_entry<command>& entry) {
    // Your code here
    u >> entry.term;
    u >> entry.index;
    int size;
    std::string str;
    u >> size;
    u >> str;
    entry.cmd.deserialize(str.c_str(), size);
    return u;
}

template<typename command>
class append_entries_args {
public:
    // Your code here
    int term;
    int leaderId;
    int prevLogIndex;
    int prevLogTerm;
    int leaderCommit;
    bool commit;
    std::vector<log_entry<command>> log_entries;

    append_entries_args(){}
    append_entries_args(int term_, int leaderId_, int prevLogIndex_,
                        int prevLogTerm_, int leaderCommit_){
        term = term_;
        leaderId = leaderId_;
        prevLogIndex = prevLogIndex_;
        prevLogTerm = prevLogTerm_;
        leaderCommit = leaderCommit_;
        commit = false;
    }
    append_entries_args(int term_, int leaderId_, int prevLogIndex_, 
                        int prevLogTerm_, int leaderCommit_, 
                        const std::vector<log_entry<command>>& log_entries_){
        term = term_;
        leaderId = leaderId_;
        prevLogIndex = prevLogIndex_;
        prevLogTerm = prevLogTerm_;
        leaderCommit = leaderCommit_;
        log_entries = log_entries_;
        commit = false;
    }
    append_entries_args(int term_, int leaderId_, int prevLogIndex_, 
                        int prevLogTerm_, int leaderCommit_, 
                        const log_entry<command>& log_entry_){
        term = term_;
        leaderId = leaderId_;
        prevLogIndex = prevLogIndex_;
        prevLogTerm = prevLogTerm_;
        leaderCommit = leaderCommit_;
        log_entries.push_back(log_entry_);
        commit = false;
    }
};

template<typename command>
marshall& operator<<(marshall &m, const append_entries_args<command>& args) {
    // Your code here
    m << args.term;
    m << args.leaderId;
    m << args.prevLogIndex;
    m << args.prevLogTerm;
    m << args.leaderCommit;
    m << args.commit;
    int size = args.log_entries.size();
    m << size;
    for(int i = 0;i < size;++i){
        m << args.log_entries[i];
    }
    return m;
}

template<typename command>
unmarshall& operator>>(unmarshall &u, append_entries_args<command>& args) {
    // Your code here
    u >> args.term;
    u >> args.leaderId;
    u >> args.prevLogIndex;
    u >> args.prevLogTerm;
    u >> args.leaderCommit;
    u >> args.commit;
    int size;
    u >> size;
    for(int i = 0;i < size;++i){
        log_entry<command> entry;
        u >> entry;
        args.log_entries.push_back(entry);
    }
    return u;
}

class append_entries_reply {
public:
    // Your code here
    int term;
    bool success;
    int match_index;
    bool append;

    append_entries_reply(){}
    append_entries_reply(int term_, bool success_, int match_index_){
        term = term_;
        success = success_;
        match_index = match_index_;
        append = false;
    }
};

marshall& operator<<(marshall &m, const append_entries_reply& reply);
unmarshall& operator>>(unmarshall &m, append_entries_reply& reply);


class install_snapshot_args {
public:
    // Your code here
    int term;
    int leaderId;
    int lastIncludedIndex;
    int lastIncludedTerm;
    std::string data;

    install_snapshot_args(){}
    install_snapshot_args(int term_, int leaderId_, int lastIncludedIndex_, int lastIncludedTerm_, const std::string& data_){
        term = term_;
        leaderId = leaderId_;
        lastIncludedIndex = lastIncludedIndex_;
        lastIncludedTerm = lastIncludedTerm_;
        data = data_;
    }
};

marshall& operator<<(marshall &m, const install_snapshot_args& args);
unmarshall& operator>>(unmarshall &m, install_snapshot_args& args);


class install_snapshot_reply {
public:
    // Your code here
    int term;

    install_snapshot_reply(){}
    install_snapshot_reply(int term_){
        term = term_;
    }
};

marshall& operator<<(marshall &m, const install_snapshot_reply& reply);
unmarshall& operator>>(unmarshall &m, install_snapshot_reply& reply);


#endif // raft_protocol_h