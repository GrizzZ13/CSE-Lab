#include <string>
#include <vector>
#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <arpa/inet.h>
#include <string>
#include <vector>
#include <mutex>

#include "mr_protocol.h"
#include "rpc.h"

#define DEBUG

using namespace std;

struct Task {
	int taskType;     // should be either Mapper or Reducer
	bool isAssigned;  // has been assigned to a worker
	bool isCompleted; // has been finised by a worker
	int index;        // index to the file
};

class Coordinator {
public:
	Coordinator(const vector<string> &files, int nReduce);
	mr_protocol::status askTask(int, mr_protocol::AskTaskResponse &reply);
	mr_protocol::status submitTask(int taskType, int index, bool &success);
	bool isFinishedMap();
	bool isFinishedReduce();
	bool Done();

private:
	vector<string> files;
	vector<Task> mapTasks;
	vector<Task> reduceTasks;

	mutex mtx;

	long completedMapCount;
	long completedReduceCount;
	bool isFinished;
	
	string getFile(int index);
};


// Your code here -- RPC handlers for the worker to call.

mr_protocol::status Coordinator::askTask(int, mr_protocol::AskTaskResponse &reply) {
	// Lab2 : Your code goes here.
	reply.taskType = mr_tasktype::NONE;
	if(!isFinishedMap()){
		// map unfinished
		this->mtx.lock();
		for(auto &task : mapTasks){
			if(!task.isAssigned){
				task.isAssigned = true;
				reply.taskType = mr_tasktype::MAP;
				reply.index = task.index;
				reply.filename = files[task.index];
				break;
			}
		}
		this->mtx.unlock();
		return mr_protocol::OK;
	}
	#ifdef DEBUG
	cout << "Got request. Map finished." << endl;
	#endif
	// map finished
	if(!isFinishedReduce()){
		// map finished, reduce unfinished
		this->mtx.lock();
		for(auto &task : reduceTasks){
			if(!task.isAssigned){
				task.isAssigned = true;
				reply.taskType = mr_tasktype::REDUCE;
				reply.index = task.index;
				break;
			}
		}
		this->mtx.unlock();
		return mr_protocol::OK;
	}
	// map finished, reduce finished
	return mr_protocol::OK;
}

mr_protocol::status Coordinator::submitTask(int taskType, int index, bool &success) {
	#ifdef DEBUG
	cout << "Got submit request" << endl;
	#endif
	// Lab2 : Your code goes here.
	if(taskType==mr_tasktype::MAP){
		this->mtx.lock();
		completedMapCount++;
		mapTasks[index].isCompleted = true;
		this->mtx.unlock();
	}
	else if(taskType==mr_tasktype::REDUCE){
		this->mtx.lock();
		completedReduceCount++;
		reduceTasks[index].isCompleted = true;
		if (this->completedReduceCount >= long(this->reduceTasks.size())) {
			isFinished = true;
		}
		this->mtx.unlock();
	}
	return mr_protocol::OK;
}

string Coordinator::getFile(int index) {
	this->mtx.lock();
	string file = this->files[index];
	this->mtx.unlock();
	return file;
}

bool Coordinator::isFinishedMap() {
	bool isFinished = false;
	this->mtx.lock();
	if (this->completedMapCount >= long(this->mapTasks.size())) {
		isFinished = true;
	}
	this->mtx.unlock();
	return isFinished;
}

bool Coordinator::isFinishedReduce() {
	bool isFinished = false;
	this->mtx.lock();
	if (this->completedReduceCount >= long(this->reduceTasks.size())) {
		isFinished = true;
	}
	this->mtx.unlock();
	return isFinished;
}

//
// mr_coordinator calls Done() periodically to find out
// if the entire job has finished.
//
bool Coordinator::Done() {
	bool r = false;
	this->mtx.lock();
	r = this->isFinished;
	this->mtx.unlock();
	return r;
}

//
// create a Coordinator.
// nReduce is the number of reduce tasks to use.
//
Coordinator::Coordinator(const vector<string> &files, int nReduce)
{
	this->files = files;
	this->isFinished = false;
	this->completedMapCount = 0;
	this->completedReduceCount = 0;

	int filesize = files.size();
	for (int i = 0; i < filesize; i++) {
		#ifdef DEBUG
		cout << files[i] << endl;
		#endif
		this->mapTasks.push_back(Task{mr_tasktype::MAP, false, false, i});
	}
	for (int i = 0; i < nReduce; i++) {
		this->reduceTasks.push_back(Task{mr_tasktype::REDUCE, false, false, i});
	}
}

int main(int argc, char *argv[])
{
	int count = 0;

	if(argc < 3){
		fprintf(stderr, "Usage: %s <port-listen> <inputfiles>...\n", argv[0]);
		exit(1);
	}
	char *port_listen = argv[1];
	
	setvbuf(stdout, NULL, _IONBF, 0);

	char *count_env = getenv("RPC_COUNT");
	if(count_env != NULL){
		count = atoi(count_env);
	}

	vector<string> files;
	char **p = &argv[2];
	while (*p) {
		files.push_back(string(*p));
		++p;
	}

	rpcs server(atoi(port_listen), count);

	Coordinator c(files, REDUCER_COUNT);
	
	//
	// Lab2: Your code here.
	// Hints: Register "askTask" and "submitTask" as RPC handlers here
	// 
	server.reg(mr_protocol::asktask, &c, &Coordinator::askTask);
	server.reg(mr_protocol::submittask, &c, &Coordinator::submitTask);

	while(!c.Done()) {
		sleep(1);
	}

	return 0;
}


