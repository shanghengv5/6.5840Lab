package mr

import (
	"fmt"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"sync"
)

type WorkStatus int

type Coordinator struct {
	// Your definitions here.
	files   []string
	nReduce int

	jobTypeStatus map[JobType]WorkStatus
	jobs          map[int]WorkStatus
	reduceJobs    map[int]WorkStatus

	mu sync.Mutex

	Retry map[JobType]map[int]int
}

// Worker ask job to do
func (c *Coordinator) Work(args *WorkArgs, reply *WorkReply) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	// if worker call work,we think it still alive

	if args.JobType == mapJob {
		for n, b := range c.jobs {
			if b == idle {
				reply.Files = append(reply.Files, c.files[n])
				reply.NReduce = c.nReduce
				reply.JobNum = n
				c.Retry[args.JobType][n] = 0
				c.jobs[n] = work
				return nil
			}
		}
	} else if args.JobType == reduceJob {
		reply.JobNum = c.nReduce
		reply.NReduce = c.nReduce
		for n, b := range c.reduceJobs {
			if b == idle {
				c.reduceJobs[n] = work
				reply.JobNum = n
				c.Retry[args.JobType][n] = 0
				return nil
			}
		}
	}

	return nil
}

func (c *Coordinator) Log() {
	fmt.Println("Retrys:", c.Retry)

}

// Worker reply coordinator one job is done
func (c *Coordinator) WorkDone(args *WorkDoneArgs, reply *WorkDoneReply) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	// if worker call  done work,we think it still alive

	if args.JobType == mapJob {
		if c.jobs[args.JobNum] != done {
			c.jobs[args.JobNum] = done
		}

		// Finish all map job
		isDone := true
		for _, b := range c.jobs {
			if b != done {
				isDone = false
			}
		}
		if isDone {
			c.jobTypeStatus[mapJob] = done
			c.jobTypeStatus[reduceJob] = work
			reply.StepDone = true
		}
	} else if args.JobType == reduceJob && c.jobTypeStatus[mapJob] == done {
		if c.reduceJobs[args.JobNum] != done {
			c.reduceJobs[args.JobNum] = done
		}

		isDone := true
		for _, b := range c.reduceJobs {
			if b != done {
				isDone = false
			}
		}

		if isDone {
			c.jobTypeStatus[reduceJob] = done
			reply.StepDone = true
		}
		// all done
		c.Done()
	}

	return nil
}

// start a thread that listens for RPCs from worker.go
func (c *Coordinator) server() {
	rpc.Register(c)
	rpc.HandleHTTP()
	//l, e := net.Listen("tcp", ":1234")
	sockname := coordinatorSock()
	os.Remove(sockname)
	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatal("listen error:", e)
	}
	go http.Serve(l, nil)
}

// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
func (c *Coordinator) Done() bool {
	ret := false
	// Your code here.
	if c.jobTypeStatus[mapJob] == done && c.jobTypeStatus[reduceJob] == done {
		return true
	}
	c.Log()
	//reset
	for job, b := range c.jobs {
		if b == work {
			c.Retry[mapJob][job]++
			if c.Retry[mapJob][job] > 10 {
				c.jobs[job] = idle
				c.Retry[mapJob][job] = 0
			}
		}
	}
	for job, b := range c.reduceJobs {
		if b == work {
			c.Retry[reduceJob][job]++
			if c.Retry[reduceJob][job] > 10 {
				c.reduceJobs[job] = idle
				c.Retry[reduceJob][job] = 0
			}
		}
	}

	return ret
}

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{
		jobs:          make(map[int]WorkStatus),
		jobTypeStatus: make(map[JobType]WorkStatus),
		reduceJobs:    make(map[int]WorkStatus),
		Retry:         make(map[JobType]map[int]int),
	}

	c.files = files
	c.nReduce = nReduce
	// init
	for i := 0; i < len(c.files); i++ {
		c.jobs[i] = idle
	}

	for i := 0; i < nReduce; i++ {
		c.reduceJobs[i] = idle
	}
	c.Retry[mapJob] = make(map[int]int)
	c.Retry[reduceJob] = make(map[int]int)
	c.jobTypeStatus[mapJob] = work
	c.jobTypeStatus[intermediateJob] = idle
	c.jobTypeStatus[reduceJob] = idle

	c.server()
	return &c
}
