package mr

import (
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"sync"
	"time"
)

type Master struct {
	// Your definitions here.
	mu           sync.Mutex
	files        []string
	nReduce      int
	timeout      int64
	mapFinish    bool
	reduceFinish bool
	mapTasks     []int64
	reduceTasks  []int64
}

// Your code here -- RPC handlers for the worker to call.

//
// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
//
func (m *Master) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
}

func (m *Master) GetTask(args *TaskArgs, reply *TaskReply) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.mapFinish { // reduce
		for index, value := range m.reduceTasks {
			now := time.Now().Unix()
			if value == 0 || (value > 0 && now-value >= m.timeout) {
				m.reduceTasks[index] = now
				reply.Task = "reduce"
				reply.TaskId = index
				reply.Nmap = len(m.files)
				reply.Nreduce = m.nReduce
				return nil
			}
		}
	} else {
		for index, value := range m.mapTasks {
			now := time.Now().Unix()
			if value == 0 || (value > 0 && now-value >= m.timeout) {
				m.mapTasks[index] = now
				reply.Task = "map"
				reply.TaskId = index
				reply.File = m.files[index]
				reply.Nmap = len(m.files)
				reply.Nreduce = m.nReduce
				return nil
			}
		}
	}

	reply.Task = "None"

	return nil
}

func (m *Master) Finish(args *FinishArgs, reply *FinishReply) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if args.Task == "map" {
		if m.mapTasks[args.TaskId] < 0 {
			return nil
		}
		m.mapTasks[args.TaskId] = -1
		// for index, value := range args.TempFileName {
		// 	err := os.Rename(value, fmt.Sprintf("./mr-%d-%d", args.TaskId, index))
		// 	if err != nil {
		// 		m.mapTasks[args.TaskId] = 0
		// 		log.Fatal(err)
		// 	}
		// }
		m.mapFinish = true
		for _, value := range m.mapTasks {
			if value != -1 {
				m.mapFinish = false
				break
			}
		}
	} else {
		if m.reduceTasks[args.TaskId] < 0 {
			return nil
		}
		m.reduceTasks[args.TaskId] = -1
		// for _, value := range args.TempFileName {
		// 	err := os.Rename(value, fmt.Sprintf("./mr-out-%d", args.TaskId))
		// 	if err != nil {
		// 		m.reduceTasks[args.TaskId] = 0
		// 		log.Fatal(err)
		// 	}
		// }
		m.reduceFinish = true
		for _, value := range m.reduceTasks {
			if value != -1 {
				m.reduceFinish = false
				break
			}
		}
	}
	return nil
}

//
// start a thread that listens for RPCs from worker.go
//
func (m *Master) server() {
	rpc.Register(m)
	rpc.HandleHTTP()
	//l, e := net.Listen("tcp", ":1234")
	sockname := masterSock()
	os.Remove(sockname)
	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatal("listen error:", e)
	}
	go http.Serve(l, nil)
}

//
// main/mrmaster.go calls Done() periodically to find out
// if the entire job has finished.
//
func (m *Master) Done() bool {
	ret := false
	// Your code here.
	m.mu.Lock()
	defer m.mu.Unlock()
	ret = m.reduceFinish

	return ret
}

//
// create a Master.
// main/mrmaster.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeMaster(files []string, nReduce int) *Master {
	m := Master{
		files:        files,
		nReduce:      nReduce,
		timeout:      10, // default to be 10 in this lab
		mapFinish:    false,
		reduceFinish: false,
		mapTasks:     make([]int64, len(files)), // 0: task not assigned, -1: finished, > 0: time when assigned
		reduceTasks:  make([]int64, nReduce),
	}

	for index := range files {
		m.mapTasks[index] = 0
		m.reduceTasks[index] = 0
	}

	m.server()
	return &m
}
