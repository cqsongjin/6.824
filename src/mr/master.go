package mr

import (
	"log"
	"sync"
	"time"
)
import "net"
import "os"
import "net/rpc"
import "net/http"

type Stage int
type TaskType int
type State int

const (
	MapStage Stage = iota
	ReduceStage
)

const (
	MapTask TaskType = iota
	ReduceTask
)

const (
	TaskStateInit State = iota
	TaskStateWaiting
	TaskStateWorking
	TaskStateTimeOut
	TaskStateDone
	TaskStateFail
)

type Master struct {
	id          int
	nMap        int
	nReduce     int
	files       []string
	tasks       chan Task
	stage       Stage
	mapState    []TaskState
	reduceState []TaskState
	mutex       sync.Mutex
	done        bool
	workerCount int
}

type Task struct {
	Id       int
	NMap     int
	NReduce  int
	Type     TaskType
	FileName string
}

type TaskState struct {
	TaskId    int
	WorkerId  int
	State     State
	StartTime time.Time
}

func (m *Master) Register(args *RegisterArgs, reply *RegisterReply) error {
	m.mutex.Lock()
	defer m.mutex.Unlock()
	m.workerCount++
	reply.WorkerId = m.workerCount
	return nil
}

func (m *Master) GetTask(args *GetTaskArgs, reply *GetTaskReply) error {
	log.Println("call get task")
	task := <-m.tasks
	log.Println("call get task test")
	m.mutex.Lock()
	defer m.mutex.Unlock()
	if task.Type == MapTask {
		m.mapState[task.Id].WorkerId = args.WorkerId
		m.mapState[task.Id].State = TaskStateWorking
		m.mapState[task.Id].StartTime = time.Now()
	} else {
		m.reduceState[task.Id].WorkerId = args.WorkerId
		m.reduceState[task.Id].State = TaskStateWorking
		m.reduceState[task.Id].StartTime = time.Now()
	}
	reply.Task = &task
	return nil
}

func (m *Master) SubmitTask(args *SubmitTaskArgs, reply *SubmitTaskReply) error {
	m.mutex.Lock()
	defer m.mutex.Unlock()
	log.Println("submit task :", args)
	if args.TaskType == MapTask {
		taskState := &m.mapState[args.TaskId]
		if args.WorkerId == taskState.WorkerId {
			if args.IsSuccess {
				taskState.State = TaskStateDone
			} else {
				taskState.State = TaskStateFail
			}
		} else {
			log.Println("the workerId is not equals, ignore the submit")
		}
	} else {
		taskState := &m.reduceState[args.TaskId]
		if args.WorkerId == taskState.WorkerId {
			if args.IsSuccess {
				taskState.State = TaskStateDone
			} else {
				taskState.State = TaskStateFail
			}
		} else {
			log.Println("the workerId is not equals, ignore the submit")
		}
	}
	go m.Scan()
	log.Println("submit task success:", args)
	return nil
}

func (m *Master) Scan() {
	m.mutex.Lock()
	defer m.mutex.Unlock()
	log.Printf("start scan:")
	log.Println(m.mapState)
	log.Println(m.reduceState)
	log.Println(len(m.tasks))
	mapDone := true
	reduceDone := true
	if m.stage == MapStage {
		for i, _ := range m.mapState {
			log.Println("check the map :", i)
			switch m.mapState[i].State {
			case TaskStateInit:
				mapDone = false
				task := CreateTask(i, m)
				m.tasks <- task
				m.mapState[i].State = TaskStateWaiting
			case TaskStateWaiting:
				mapDone = false
			case TaskStateWorking:
				mapDone = false
				if (time.Now().Unix() - m.mapState[i].StartTime.Unix()) > 10 {
					log.Printf("task %v timeout", i)
					task := CreateTask(i, m)
					m.tasks <- task
					m.mapState[i].State = TaskStateWaiting
				}
			case TaskStateTimeOut:
				mapDone = false
				task := CreateTask(i, m)
				m.tasks <- task
				m.mapState[i].State = TaskStateWaiting
			case TaskStateDone:

			case TaskStateFail:
				mapDone = false
				task := CreateTask(i, m)
				m.tasks <- task
				m.mapState[i].State = TaskStateWaiting
			}
		}
		if mapDone {
			m.stage = ReduceStage
		}
	} else {
		for i, _ := range m.reduceState {
			log.Println("check the reduce :", i)
			switch m.reduceState[i].State {
			case TaskStateInit:
				reduceDone = false
				task := CreateTask(i, m)
				m.tasks <- task
				m.reduceState[i].State = TaskStateWaiting
				m.reduceState[i].StartTime = time.Now()
			case TaskStateWaiting:
				reduceDone = false
			case TaskStateWorking:
				reduceDone = false
				if (time.Now().Unix() - m.reduceState[i].StartTime.Unix()) > 10 {
					log.Printf("task %v timeout", i)
					task := CreateTask(i, m)
					m.tasks <- task
					m.reduceState[i].State = TaskStateWaiting
				}
			case TaskStateTimeOut:
				reduceDone = false
				task := CreateTask(i, m)
				m.tasks <- task
				m.reduceState[i].State = TaskStateWaiting
			case TaskStateDone:

			case TaskStateFail:
				reduceDone = false
				task := CreateTask(i, m)
				m.tasks <- task
				m.reduceState[i].State = TaskStateWaiting
			}
		}
		if reduceDone {
			m.done = true
		}
	}
	log.Println("end the scan ")
}

func (m *Master) ScanDuration() {
	for {
		log.Println("scan duration start")
		if !m.Done() {
			go m.Scan()
			time.Sleep(time.Second)
		}
		log.Println("scan duration end")
	}

}

//
// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
//
func (m *Master) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
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
	m.mutex.Lock()
	defer m.mutex.Unlock()
	return m.done
}

//
// create a Master.
// main/mrmaster.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeMaster(files []string, nReduce int) *Master {
	m := Master{
		id:          os.Getuid(),
		nMap:        len(files),
		nReduce:     nReduce,
		files:       files,
		tasks:       make(chan Task, len(files)+nReduce),
		stage:       MapStage,
		mapState:    make([]TaskState, len(files)),
		reduceState: make([]TaskState, nReduce),
		mutex:       sync.Mutex{},
		done:        false,
	}
	log.Println("master start")
	m.server()
	go m.ScanDuration()
	return &m
}

func CreateTask(i int, m *Master) Task {
	if m.stage == MapStage {
		return Task{
			Id:       i,
			NMap:     m.nMap,
			NReduce:  m.nReduce,
			Type:     MapTask,
			FileName: m.files[i],
		}
	} else {
		return Task{
			Id:       i,
			NMap:     m.nMap,
			NReduce:  m.nReduce,
			Type:     ReduceTask,
			FileName: "",
		}
	}

}
