package mr

import (
	"encoding/json"
	"fmt"
	"hash/fnv"
	"io/ioutil"
	"log"
	"net/rpc"
	"os"
	"sort"
	"time"
)

type ByKey []KeyValue

// for sorting by key.
func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

//
// Map functions return a slice of KeyValue.
//
type KeyValue struct {
	Key   string
	Value string
}

//
// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
//
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

//
// main/mrworker.go calls this function.
//
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	// Your worker implementation here.

	// uncomment to send the Example RPC to the master.
	// CallExample()
	for {
		task := CallTask()
		if task.Task == "None" {
			time.Sleep(time.Second)
			continue
		}
		if task.Task == "map" {
			MapF(mapf, task.TaskId, task.Nreduce, task.File)
		} else {
			RedF(reducef, task.Nmap, task.TaskId)
		}
	}
}

func MapF(mapf func(string, string) []KeyValue, mapId int, nReduce int, filename string) {
	intermediate := make([][]KeyValue, nReduce)

	file, err := os.Open(filename)
	if err != nil {
		fmt.Println(err.Error())
		return
	}
	content, err := ioutil.ReadAll(file)
	if err != nil {
		fmt.Println(err.Error())
		return
	}
	file.Close()
	kva := mapf(filename, string(content))
	for _, v := range kva {
		index := ihash(v.Key) % nReduce
		intermediate[index] = append(intermediate[index], v)
	}
	for i := 0; i < nReduce; i++ {
		f, err := ioutil.TempFile("./", "atomic-")
		defer func() {
			if err != nil {
				f.Close()
			}
			os.Remove(f.Name())
		}()
		if err != nil {
			fmt.Println(err.Error())
			return
		}
		enc := json.NewEncoder(f)
		for _, kv := range intermediate[i] {
			err := enc.Encode(&kv)
			if err != nil {
				fmt.Println(err.Error())
				return
			}
		}
		if err := f.Close(); err != nil {
			fmt.Println(err.Error())
			return
		}
		os.Rename(f.Name(), fmt.Sprintf("mr-%d-%d", mapId, i))
		CallFinish("map", mapId)
	}
}

func RedF(reducef func(string, []string) string, nMap int, reduceId int) {
	kva := []KeyValue{}
	for i := 0; i < nMap; i++ {
		file, err := os.Open(fmt.Sprintf("mr-%d-%d", i, reduceId))
		if err != nil {
			if os.IsExist(err) {
				fmt.Println(err.Error())
				return
			}
			continue
		}
		dec := json.NewDecoder(file)
		for {
			var kv KeyValue
			if err := dec.Decode(&kv); err != nil {
				break
			}
			kva = append(kva, kv)
		}
	}
	sort.Sort(ByKey(kva))
	f, err := ioutil.TempFile("./", "atomic-")
	defer func() {
		if err != nil {
			f.Close()
		}
		os.Remove(f.Name())
	}()
	if err != nil {
		fmt.Println(err.Error())
		return
	}

	i := 0
	for i < len(kva) {
		j := i + 1
		for j < len(kva) && kva[j].Key == kva[i].Key {
			j++
		}
		values := []string{}
		for k := i; k < j; k++ {
			values = append(values, kva[k].Value)
		}
		output := reducef(kva[i].Key, values)

		// this is the correct format for each line of Reduce output.
		fmt.Fprintf(f, "%v %v\n", kva[i].Key, output)
		i = j
	}

	if err := f.Close(); err != nil {
		fmt.Println(err.Error())
		return
	}
	err = os.Rename(f.Name(), fmt.Sprintf("mr-out-%d", reduceId))
	CallFinish("reduce", reduceId)
}

//
// example function to show how to make an RPC call to the master.
//
// the RPC argument and reply types are defined in rpc.go.
//

func CallTask() *TaskReply {
	args := TaskArgs{}
	reply := TaskReply{
		Task: "None",
	}
	mark := call("Master.GetTask", &args, &reply)
	if !mark {
		return nil
	}
	return &reply
}

func CallFinish(Task string, TaskId int) {
	args := FinishArgs{
		Task:   Task,
		TaskId: TaskId,
	}
	reply := FinishReply{}
	call("Master.Finish", &args, &reply)
}

func CallExample() {

	// declare an argument structure.
	args := ExampleArgs{}

	// fill in the argument(s).
	args.X = 99

	// declare a reply structure.
	reply := ExampleReply{}

	// send the RPC request, wait for the reply.
	call("Master.Example", &args, &reply)

	// reply.Y should be 100.
	fmt.Printf("reply.Y %v\n", reply.Y)
}

//
// send an RPC request to the master, wait for the response.
// usually returns true.
// returns false if something goes wrong.
//
func call(rpcname string, args interface{}, reply interface{}) bool {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	sockname := masterSock()
	c, err := rpc.DialHTTP("unix", sockname)
	if err != nil {
		log.Fatal("dialing:", err)
	}
	defer c.Close()

	err = c.Call(rpcname, args, reply)
	if err == nil {
		return true
	}

	fmt.Println(err)
	return false
}
