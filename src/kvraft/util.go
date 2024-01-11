package kvraft

import (
	"fmt"
	"log"
	"os"
	"strconv"
	"time"
)

// Debugging
const Debug = 0b0010

type logTopic string

const (
	dClient  logTopic = "CLNT"
	dRequest logTopic = "REQ"
	dServer  logTopic = "KVSERVER"
	dApply   logTopic = "Applier"
	dRespond logTopic = "RESP"
)

var debugStart time.Time
var debugVerbosity int

func init() {
	debugVerbosity = getVerbosity()
	debugStart = time.Now()

	log.SetFlags(log.Flags() &^ (log.Ldate | log.Ltime))
}

func DPrintf(topic logTopic, format string, a ...interface{}) {
	if getVerbosity()&Debug > 0 {
		time := time.Since(debugStart).Microseconds()
		time /= 100
		prefix := fmt.Sprintf("%06d %v ", time, string(topic))
		format = prefix + format
		log.Printf(format, a...)
	}
}

func getVerbosity() int {
	v := os.Getenv("VERBOSE")
	level := 0
	if v != "" {
		var err error
		level, err = strconv.Atoi(v)
		if err != nil {
			log.Fatalf("Invalid verbosity %v", v)
		}
	}
	return level
}
