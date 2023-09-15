package mr

import (
	"encoding/json"
	"errors"
	"fmt"
	"hash/fnv"
	"io"
	"log"
	"net/rpc"
	"os"
	"regexp"
	"sort"
)

// Map functions return a slice of KeyValue.
type KeyValue struct {
	Key   string
	Value string
}

type ByKey []KeyValue

func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

// main/mrworker.go calls this function.
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	// Your worker implementation here.
	// Ask a map job
	for err := mapWork(mapf); err != nil; err = mapWork(mapf) {

	}
	// Ask a reduce job
	for err := reduceWork(reducef); err != nil; err = reduceWork(reducef) {

	}
}

func reduceWork(reducef func(string, []string) string) error {
	reply := WorkReply{}
	arg := WorkArgs{JobType: reduceJob}
	ok := call("Coordinator.Work", &arg, &reply)
	if ok && reply.JobNum != reply.NReduce {
		kva := []KeyValue{}
		jobNum := reply.JobNum
		// Find intermediate
		curDir, err := os.Getwd()
		if err != nil {
			return err
		}
		files, err := os.ReadDir(curDir)
		if err != nil {
			return err
		}
		regStr := fmt.Sprintf(`^mr-%d-.+$`, jobNum)
		regex := regexp.MustCompile(regStr)

		for _, file := range files {
			if regex.MatchString(file.Name()) {
				f, err := os.Open(file.Name())
				if err != nil {
					return err
				}
				dec := json.NewDecoder(f)
				for {
					var kv KeyValue
					if err := dec.Decode(&kv); err != nil {
						break
					}
					kva = append(kva, kv)
				}
			}
		}
		// sort Intermediate
		sort.Sort(ByKey(kva))
		path := fmt.Sprintf("mr-out-%d", jobNum)

		tmpFile, err := os.Create(path)
		if err != nil {
			return fmt.Errorf("create tmp file failed!\n")
		}
		for i := 0; i < len(kva); {
			j := i + 1
			for j < len(kva) && kva[i].Key == kva[j].Key {
				j++
			}
			values := []string{}
			for k := i; k < j; k++ {
				values = append(values, kva[k].Value)
			}
			output := reducef(kva[i].Key, values)
			_, err = fmt.Fprintf(tmpFile, "%v %v\n", kva[i].Key, output)
			if err != nil {
				return err
			}
			i = j
		}

		if err = tmpFile.Close(); err != nil {
			return err
		}
		doneReply := WorkDoneReply{}
		doneArgs := WorkDoneArgs{JobType: reduceJob, JobNum: reply.JobNum}

		ok = call("Coordinator.WorkDone", &doneArgs, &doneReply)

		if ok && doneReply.StepDone {
			return nil
		}
	}

	return errors.New("need a new reduce work")
}

// A map work
func mapWork(mapf func(string, string) []KeyValue) error {
	reply := WorkReply{}
	arg := WorkArgs{JobType: mapJob}
	ok := call("Coordinator.Work", &arg, &reply)
	if ok {
		// Map work
		intermediate := make([][]KeyValue, reply.NReduce)

		for _, fileName := range reply.Files {
			file, err := os.Open(fileName)
			if err != nil {
				return err
			}
			contents, err := io.ReadAll(file)
			if err != nil {
				return err
			}
			kv := mapf(fileName, string(contents))
			// Create intermediate
			for _, kv := range kv {
				k := ihash(kv.Key) % reply.NReduce
				intermediate[k] = append(intermediate[k], kv)
			}

		}
		// save intermediate
		for idx, kv := range intermediate {
			path := fmt.Sprintf("mr-%d-%d", idx, reply.JobNum)

			tmpFile, err := os.Create(path)
			if err != nil {
				return err
			}
			enc := json.NewEncoder(tmpFile)
			if err != nil {
				return err
			}
			for _, kv := range kv {
				err = enc.Encode(&kv)
				if err != nil {
					return err
				}
			}
			err = tmpFile.Close()
			if err != nil {
				return err
			}
		}

		doneReply := WorkDoneReply{}
		doneArgs := WorkDoneArgs{JobType: mapJob, JobNum: reply.JobNum}
		ok = call("Coordinator.WorkDone", &doneArgs, &doneReply)

		if ok && doneReply.StepDone {
			return nil
		}
	}

	return errors.New("need a new map work")
}

// send an RPC request to the coordinator, wait for the response.
// usually returns true.
// returns false if something goes wrong.
func call(rpcname string, args interface{}, reply interface{}) bool {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	sockname := coordinatorSock()
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
