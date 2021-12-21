package mr

//
// RPC definitions.
//
// remember to capitalize all names.
//

import (
	"fmt"
	"os"
	"time"
)
import "strconv"

const (
	MAP = "MAP"
	REDUCE = "REDUCE"
)

//
// example to show how to declare the arguments
// and reply for an RPC.
//

type Task struct {
	Type 			string
	Index 			int
	MapInputFile	string
	WorkerID		string
	Deadline		time.Time
}

type ApplyForTaskArgs struct {
	WorkerID 		string

	LastTaskType 	string
	LastTaskIndex 	int
}

type ApplyForTaskReply struct {
	TaskType 		string
	TaskIndex 		int
	MapInputFile 	string
	MapNum 			int
	ReduceNum 		int
}

type ExampleArgs struct {
	X int
}

type ExampleReply struct {
	Y int
}

// Add your RPC definitions here.


// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the master.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func masterSock() string {
	s := "/var/tmp/824-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}

func tmpMapOutFile(worker string, mapIndex int, reduceIndex int) string {
	return fmt.Sprintf("tmp-worker-%s-%d-%d", worker, mapIndex, reduceIndex)
}

func finalMapOutFile(mapIndex int, reduceIndex int) string {
	return fmt.Sprintf("mr-%d-%d", mapIndex, reduceIndex)
}

func tmpReduceOutFile(worker string, reduceIndex int) string {
	return fmt.Sprintf("tmp-worker-%s-out-%d", worker, reduceIndex)
}

func finalReduceOutFile(reduceIndex int) string {
	return fmt.Sprintf("mr-out-%d", reduceIndex)
}