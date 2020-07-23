package mr

import (
	"encoding/json"
	"errors"
	"fmt"
	"io/ioutil"
	"os"
)
import "log"
import "net/rpc"
import "hash/fnv"

const (
	GetTaskRpcName    = "Master.GetTask"
	SubmitTaskRpcName = "Master.SubmitTask"
	RegisterRpcName   = "Master.Register"
)

//
// Map functions return a slice of KeyValue.
//
type KeyValue struct {
	Key   string
	Value string
}

type worker struct {
	Id         int
	MapFunc    func(string, string) []KeyValue
	ReduceFunc func(string, []string) string
}

func (w *worker) Start() {
	for {
		task := w.GetTask()
		w.Exec(task)
	}
}

func (w *worker) Exec(task *Task) {
	var err error
	if task.Type == MapTask {
		err = w.ExecMap(task)
	} else {
		err = w.ExecReduce(task)
	}
	log.Println("worker -> ", "submit task :", task.Id)
	w.SubmitTask(task, err == nil)
}

func (w *worker) ExecMap(task *Task) error {
	log.Printf("start map task : %v", task.Id)
	bytes, err := ioutil.ReadFile(task.FileName)
	if err != nil {
		return err
	}
	keyValues := w.MapFunc(task.FileName, string(bytes))
	reduces := make([][]KeyValue, task.NReduce)
	for _, kv := range keyValues {
		p := ihash(kv.Key) % task.NReduce
		reduces[p] = append(reduces[p], kv)
	}
	for i, reduce := range reduces {
		midFileName := fmt.Sprintf("mr-%d-%d", task.Id, i)
		file, err := os.Create(midFileName)
		if err != nil {
			return err
		}
		encoder := json.NewEncoder(file)
		for _, keyValue := range reduce {
			err = encoder.Encode(&keyValue)
			if err != nil {
				return err
			}
		}
	}
	log.Printf("end map task : %v", task.Id)
	return nil
}

func (w *worker) ExecReduce(task *Task) error {
	log.Printf("start reduce task : %v", task.Id)
	kvsMap := make(map[string][]string)
	for i := 0; i < task.NMap; i++ {
		midFileName := fmt.Sprintf("mr-%d-%d", i, task.Id)
		file, err := os.Open(midFileName)
		if err != nil {
			return err
		}
		decoder := json.NewDecoder(file)
		for {
			var kv KeyValue
			if err = decoder.Decode(&kv); err != nil {
				break
			}
			if _, ok := kvsMap[kv.Key]; !ok {
				kvsMap[kv.Key] = make([]string, 0, 10)
			}
			kvsMap[kv.Key] = append(kvsMap[kv.Key], kv.Value)
		}
	}
	outputFileName := fmt.Sprintf("mr-out-%d", task.Id)
	file, err := os.Create(outputFileName)
	if err != nil {
		return err
	}
	for key, values := range kvsMap {
		result := w.ReduceFunc(key, values)
		fmt.Fprintf(file, "%v %v\n", key, result)
	}
	log.Printf("end reduce task : %v", task.Id)
	return nil
}

func (w *worker) Register() error {
	args := RegisterArgs{}
	reply := RegisterReply{}
	result := call(RegisterRpcName, &args, &reply)
	if !result {
		return errors.New("register fail")
	}
	w.Id = reply.WorkerId
	log.Printf("register worker success :%v", w.Id)
	return nil
}

func (w *worker) GetTask() *Task {
	args := GetTaskArgs{WorkerId: w.Id}
	reply := GetTaskReply{}
	result := call(GetTaskRpcName, &args, &reply)
	if !result {
		panic("get task fail, exit")
	}
	return reply.Task
}

func (w *worker) SubmitTask(task *Task, isSuccess bool) {
	args := SubmitTaskArgs{
		TaskId:    task.Id,
		TaskType:  task.Type,
		WorkerId:  w.Id,
		IsSuccess: isSuccess,
	}
	reply := SubmitTaskReply{}
	result := call(SubmitTaskRpcName, &args, &reply)
	if !result {
		panic("submit task fail, exit")
	}
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
	newWorker := worker{
		Id:         0,
		MapFunc:    mapf,
		ReduceFunc: reducef,
	}
	err := newWorker.Register()
	if err != nil {
		panic(err.Error())
	}
	newWorker.Start()
	log.Printf("worker %v finish", newWorker.Id)
}

//
// example function to show how to make an RPC call to the master.
//
// the RPC argument and reply types are defined in rpc.go.
//
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
