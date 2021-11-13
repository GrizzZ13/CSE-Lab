#include "raft_protocol.h"

marshall& operator<<(marshall &m, const request_vote_args& args) {
    // Your code here
    m << args.term;
    m << args.candidateId;
    m << args.lastLogIndex;
    m << args.lastLogTerm;
    return m;

}
unmarshall& operator>>(unmarshall &u, request_vote_args& args) {
    // Your code here
    u >> args.term;
    u >> args.candidateId;
    u >> args.lastLogIndex;
    u >> args.lastLogTerm;
    return u;
}

marshall& operator<<(marshall &m, const request_vote_reply& reply) {
    // Your code here
    m << reply.term;
    m << reply.voteGranted;
    return m;
}

unmarshall& operator>>(unmarshall &u, request_vote_reply& reply) {
    // Your code here
    u >> reply.term;
    u >> reply.voteGranted;
    return u;
}

marshall& operator<<(marshall &m, const append_entries_reply& reply) {
    // Your code here
    m << reply.term;
    m << reply.success;
    m << reply.match_index;
    return m;
}
unmarshall& operator>>(unmarshall &u, append_entries_reply& reply) {
    // Your code here
    u >> reply.term;
    u >> reply.success;
    u >> reply.match_index;
    return u;
}

marshall& operator<<(marshall &m, const install_snapshot_args& args) {
    // Your code here

    return m;
}

unmarshall& operator>>(unmarshall &u, install_snapshot_args& args) {
    // Your code here

    return u; 
}

marshall& operator<<(marshall &m, const install_snapshot_reply& reply) {
    // Your code here

    return m;
}

unmarshall& operator>>(unmarshall &u, install_snapshot_reply& reply) {
    // Your code here

    return u;
}