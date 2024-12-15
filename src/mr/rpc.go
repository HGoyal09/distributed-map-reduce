package mr

import (
	"os"
)
import "strconv"

type JobArgs struct{}

type JobMapReply struct {
	FileName string
	MapId    int
	NReduce  int
	Finish   bool
}

type JobReduceReply struct {
	Files    []string
	ReduceId int
}

type JobReply struct {
	ReplyType   string
	MapReply    JobMapReply
	ReduceReply JobReduceReply
}

type JobMapFinishArgs struct {
	OfilePrefix       string
	ProcessedFilename string
}

type JobReduceFinishArgs struct {
	ReduceId int
	Filename string
}

type JobFinishReply struct{}

func coordinatorSock() string {
	s := "/var/tmp/5840-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}
