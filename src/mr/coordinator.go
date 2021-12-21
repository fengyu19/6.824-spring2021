// https://mr-dai.github.io/mit-6824-lab1/

package mr

import (
	"fmt"
	"log"
	"math"
	"sync"
	"time"
)
import "net"
import "os"
import "net/rpc"
import "net/http"


type Master struct {
	// Your definitions here.
	lock 			sync.Mutex
	stage 			string // map or reduce
	nMap 			int
	nReduce			int
	tasks			map[string]Task
	availableTasks	chan Task
}

// Your code here -- RPC handlers for the worker to call.

func (m *Master) ApplyForTask(args *ApplyForTaskArgs, reply *ApplyForTaskReply) error {
	if args.LastTaskType != "" {
		// previous task completed
		m.lock.Lock()
		lastTaskID := GenTaskID(args.LastTaskType, args.LastTaskIndex)
		if task, exists := m.tasks[lastTaskID]; exists && task.WorkerID == args.WorkerID {
			log.Printf("Mark %s task %d as finished on worker %s\n",
				task.Type, task.Index, args.WorkerID)

			if args.LastTaskType == MAP {
				for ri := 0; ri < m.nReduce; ri++ {
					err := os.Rename(
						tmpMapOutFile(args.WorkerID, args.LastTaskIndex, ri),
						finalMapOutFile(args.LastTaskIndex, ri))
					if err != nil {
						log.Fatalf("Failed to mark map output file `%s` as final: %e",
							tmpMapOutFile(args.WorkerID, args.LastTaskIndex, ri), err)
					}
				}
			} else if args.LastTaskType == REDUCE {
				err := os.Rename(
					tmpReduceOutFile(args.WorkerID, args.LastTaskIndex),
					finalReduceOutFile(args.LastTaskIndex))
				if err != nil {
					log.Fatalf("Failed to mark reduce output file `%s` as final: %e",
						tmpReduceOutFile(args.WorkerID, args.LastTaskIndex), err)
				}
			}
			delete(m.tasks, lastTaskID)
			if len(m.tasks) == 0 {
				m.transit()
			}
		}
		m.lock.Unlock()
	}

	task, ok := <- m.availableTasks
	if !ok {
		log.Printf("Failed to get the task")
		return nil
	}

	m.lock.Lock()
	defer m.lock.Unlock()
	log.Printf("Assgin %s task %d to worker %s\n", task.Type, task.Index, args.WorkerID)

	task.WorkerID = args.WorkerID
	task.Deadline = time.Now().Add(10 * time.Second)
	m.tasks[GenTaskID(task.Type, task.Index)] = task
	reply.TaskType = task.Type
	reply.TaskIndex = task.Index
	reply.MapInputFile = task.MapInputFile
	reply.MapNum = m.nMap
	reply.ReduceNum = m.nReduce
	return nil
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

func (m *Master) transit() {
	if m.stage == MAP {
		log.Printf("All MAP tasks finished, Transit to REDUCE stage\n")
		m.stage = REDUCE

		for i := 0; i < m.nReduce; i++ {
			task := Task{
				Type: REDUCE,
				Index: i,
			}
			m.tasks[GenTaskID(task.Type, task.Index)] = task
			m.availableTasks <- task
		}
	} else if m.stage == REDUCE {
		log.Printf("All REDUCE tasks finished, Prepare to exit\n")
		close(m.availableTasks)
		m.stage = ""
	}
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
	// Your code here.
	m.lock.Lock()
	defer m.lock.Unlock()
	return m.stage == ""
}

//
// create a Master.
// main/mrmaster.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeCoordinator(files []string, nReduce int) *Master {
	//m := Master{}

	// Your code here.
	m := Master{
		stage: 				MAP,
		nMap: 				len(files),
		nReduce: 			nReduce,
		tasks: 				make(map[string]Task),
		availableTasks:		make(chan Task, int(math.Max(float64(len(files)), float64(nReduce)))),
	}
	fmt.Printf(m.stage)
	for i, file := range files {
		task := Task{
			Type: MAP,
			Index: i,
			MapInputFile: file,
		}
		m.tasks[GenTaskID(task.Type, task.Index)] = task
		m.availableTasks <- task
	}

	log.Printf("Master start\n")
	m.server()

	go func() {
		for {
			time.Sleep(500 * time.Millisecond)

			m.lock.Lock()
			for _, task := range m.tasks {
				if task.WorkerID != "" && time.Now().After(task.Deadline) {
					log.Printf("Found time-out %s task %d previously running on worker %s. Prepare to re-assgin",
						task.Type, task.Index, task.WorkerID)
					task.WorkerID = ""
					m.availableTasks <- task
				}
			}
			m.lock.Unlock()
		}
	}()

	return &m
}

func GenTaskID(t string, index int) string  {
	return fmt.Sprintf("%s-%d", t, index)
}