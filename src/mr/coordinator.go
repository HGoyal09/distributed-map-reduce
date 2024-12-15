package mr

import (
	"fmt"
	"log"
	"sync"
)
import "net"
import "os"
import "net/rpc"
import "net/http"
import "time"

type FileInfo struct {
	completed   bool
	ofilePrefix string
	startTime   time.Time
}

type MapData struct {
	files    []string
	fileInfo map[string]FileInfo
	mapId    int //always keeps increasing
	finish   bool
}

type ReduceStatus struct {
	startTime time.Time
	finished  bool
	fileName  string
}

type ReduceData struct {
	files       []string
	reduceState map[int]ReduceStatus // finish up till nReduce
}

type Coordinator struct {
	mapData    MapData
	reduceData ReduceData
	nReduce    int
	doneChan   chan bool
	mu         sync.Mutex
}

func (c *Coordinator) MapFinish(args *JobMapFinishArgs, reply *JobFinishReply) error {
	fmt.Println("MapFinish called")

	processFile := args.ProcessedFilename
	ofilePrefix := args.OfilePrefix

	c.mu.Lock()
	defer c.mu.Unlock()

	c.mapData.fileInfo[processFile] = FileInfo{completed: true, ofilePrefix: ofilePrefix}

	fmt.Println(c)

	return nil
}

func (c *Coordinator) ReduceFinish(args *JobReduceFinishArgs, reply *JobFinishReply) error {
	fmt.Println("ReduceFinish called")

	reduceId := args.ReduceId
	filename := args.Filename

	c.mu.Lock()
	defer c.mu.Unlock()

	reduceState, ok := c.reduceData.reduceState[reduceId]
	if ok {
		reduceState.finished = true
		reduceState.fileName = filename
		c.reduceData.reduceState[reduceId] = reduceState
	}

	fmt.Printf("Finished reducing id: %v", c.reduceData.reduceState[reduceId].finished)

	return nil
}

func (c *Coordinator) FetchJob(args *JobArgs, reply *JobReply) error {
	fmt.Printf("Fetch job..., id: %v, nReduce: %v\n", c.mapData.mapId, c.nReduce)

	var fileName string
	var isAllCompleted bool

	c.mu.Lock()
	defer c.mu.Unlock()

	for {
		fileName, isAllCompleted = getCurrentFile(c)
		reply.ReplyType = "Map"
		if isAllCompleted {
			fmt.Printf("Notifying done. IsAllCompleted: %v\n", isAllCompleted)
			reply.MapReply.Finish = true
			break
		}

		if fileName != "" {
			fmt.Printf("Using file. fileName: %v, isAllCompleted: %v\n", fileName, isAllCompleted)
			reply.MapReply.FileName = fileName
			reply.MapReply.MapId = c.mapData.mapId
			reply.MapReply.NReduce = c.nReduce
			reply.MapReply.Finish = isAllCompleted
			c.mapData.mapId++
			fmt.Printf("Returning map job: %v\n", reply.MapReply)
			return nil
		}
		fmt.Printf("Waiting for file. fileName: %v, isAllCompleted: %v\n", fileName, isAllCompleted)
		time.Sleep(time.Second)
	}

	if isAllCompleted {
		reply.ReplyType = "Reduce"

		if c.reduceData.files == nil {
			c.reduceData.files = []string{}
			for _, fileInfo := range c.mapData.fileInfo {
				if fileInfo.completed {
					c.reduceData.files = append(c.reduceData.files, fileInfo.ofilePrefix)
				}
			}
		}

		if c.reduceData.reduceState == nil {
			c.reduceData.reduceState = make(map[int]ReduceStatus)
		}

		for i := 0; i < c.nReduce; i++ {
			reduceState, ok := c.reduceData.reduceState[i]
			if !ok || (!reduceState.finished && time.Since(reduceState.startTime) > time.Second*10) {
				fmt.Printf("Returning reduce job: %v, time: %v, current: %v\n", i, reduceState.startTime, time.Now())
				reduceState.startTime = time.Now()
				c.reduceData.reduceState[i] = reduceState
				reply.ReduceReply.ReduceId = i
				reply.ReduceReply.Files = c.reduceData.files
				return nil
			}
		}

		fmt.Println("All reduce jobs completed")
	}

	reply.ReplyType = "Finished"
	c.doneChan <- true

	return nil
}

func getCurrentFile(c *Coordinator) (string, bool) {
	isAllCompleted := true
	for _, filename := range c.mapData.files {
		fileInfo, mappingFound := c.mapData.fileInfo[filename]
		if !mappingFound || (!fileInfo.completed && time.Since(fileInfo.startTime) > time.Second*10) {
			fileInfo.startTime = time.Now()
			c.mapData.fileInfo[filename] = fileInfo
			fmt.Printf("Returning file name: %v, startTime: %v, completed: %v\n", filename, fileInfo.startTime, fileInfo.completed)
			return filename, false
		}
		if !fileInfo.completed {
			isAllCompleted = false
		}
	}
	fmt.Printf("No file to run. isAllCompleted: %v\n", isAllCompleted)
	return "", isAllCompleted
}

func (c *Coordinator) server() {
	rpc.Register(c)
	rpc.HandleHTTP()
	sockname := coordinatorSock()
	os.Remove(sockname)
	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatal("listen error:", e)
	}
	go http.Serve(l, nil)
}

func (c *Coordinator) Done() bool {
	return <-c.doneChan
}

func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{nReduce: nReduce, doneChan: make(chan bool)}

	c.mapData = MapData{files: files, fileInfo: make(map[string]FileInfo), mapId: 0, finish: false}

	c.server()
	return &c
}
