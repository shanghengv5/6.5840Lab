package mr

//
// RPC definitions.
//
// remember to capitalize all names.
//

import (
	"os"
	"strconv"
)

//
// example to show how to declare the arguments
// and reply for an RPC.
//

type ExampleArgs struct {
	X int
}

type ExampleReply struct {
	Y int
}

// Add your RPC definitions here.
type Empty struct {
}

type JobType int

const (
	mapJob JobType = iota
	intermediateJob
	reduceJob
)

const (
	idle WorkStatus = iota
	work
	done
)

type WorkArgs struct {
	JobType JobType
}

type WorkReply struct {
	Files   []string
	NReduce int
	JobNum  int
}

type WorkDoneArgs struct {
	JobType JobType
	JobNum  int
}

type WorkDoneReply struct {
	StepDone bool
}

// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the coordinator.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func coordinatorSock() string {
	s := "/var/tmp/5840-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}
