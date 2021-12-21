#include "chdb_state_machine.h"

chdb_command::chdb_command() {
    // TODO: Your code here
    cmd_tp = CMD_NONE;
    key = 0x7fffffff;
    value = 0x7fffffff;
    tx_id = -1;
    res = std::make_shared<result>();
}

chdb_command::chdb_command(command_type tp, const int &key, const int &value, const int &tx_id)
        : cmd_tp(tp), key(key), value(value), tx_id(tx_id) {
    // TODO: Your code here
    res = std::make_shared<result>();
}

chdb_command::chdb_command(const chdb_command &cmd) :
        cmd_tp(cmd.cmd_tp), key(cmd.key), value(cmd.value), tx_id(cmd.tx_id), res(cmd.res) {
    // TODO: Your code here
}


void chdb_command::serialize(char *buf, int size) const {
    // TODO: Your code here
    int array[4];
    array[0] = this->key;
    array[1] = this->value;
    array[2] = this->tx_id;
    switch (cmd_tp){
        case chdb_command::CMD_NONE:
            array[3] = 1;
            break;
        case chdb_command::CMD_GET:
            array[3] = 2;
            break;
        case chdb_command::CMD_PUT:
            array[3] = 3;
            break;
        default:
            assert(0);
            break;
    }
    memcpy(buf, (const void*)array, 16);
}

void chdb_command::deserialize(const char *buf, int size) {
    // TODO: Your code here
    int array[4];
    memcpy(array, (const void*)buf, 16);
    key = array[0];
    value = array[1];
    tx_id = array[2];
    switch (array[3]){
        case 1:
            cmd_tp = CMD_NONE;
            break;
        case 2:
            cmd_tp = CMD_GET;
            break;
        case 3:
            cmd_tp = CMD_PUT;
            break;
        default:
            assert(0);
            break;
    }
}

marshall &operator<<(marshall &m, const chdb_command &cmd) {
    // TODO: Your code here
    m << cmd.key;
    m << cmd.value;
    m << cmd.tx_id;
    switch (cmd.cmd_tp){
        case chdb_command::CMD_NONE:
            m << 1;
            break;
        case chdb_command::CMD_GET:
            m << 2;
            break;
        case chdb_command::CMD_PUT:
            m << 3;
            break;
        default:
            break;
    }
    return m;
}

unmarshall &operator>>(unmarshall &u, chdb_command &cmd) {
    // TODO: Your code here
    u >> cmd.key;
    u >> cmd.value;
    u >> cmd.tx_id;
    int c;
    u >> c;
    switch (c){
        case 1:
            cmd.cmd_tp = chdb_command::CMD_NONE;
            break;
        case 2:
            cmd.cmd_tp = chdb_command::CMD_GET;
            break;
        case 3:
            cmd.cmd_tp = chdb_command::CMD_PUT;
            break;
        default:
            break;
    }
    return u;
}

void chdb_state_machine::apply_log(raft_command &cmd) {
    // TODO: Your code here
    chdb_command &db_cmd = dynamic_cast<chdb_command&>(cmd);
    std::unique_lock<std::mutex> lock(db_cmd.res->mtx);
    db_cmd.res->key = db_cmd.key;
    db_cmd.res->value = db_cmd.value;
    db_cmd.res->tx_id = db_cmd.tx_id;
    db_cmd.res->tp = db_cmd.cmd_tp;
    db_cmd.res->done = true;
    db_cmd.res->cv.notify_all();
}