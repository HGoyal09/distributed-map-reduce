package mr

import (
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

// FetchJob Fetch the next job for the worker
func (c *Coordinator) FetchJob(args *JobArgs, reply *JobReply) error {
	log.Printf("Fetch job..., id: %v, nReduce: %v\n", c.mapData.mapId, c.nReduce)

	c.mu.Lock()
	defer c.mu.Unlock()

	// Attempt to fetch a map job
	if mapJob, mapJobAvailable := c.fetchMapJob(); mapJobAvailable {
		*reply = mapJob
		return nil
	}

	// Attempt to fetch a reduce job
	if reduceJob, reduceJobAvailable := c.fetchReduceJob(); reduceJobAvailable {
		*reply = reduceJob
		return nil
	}

	// If no jobs are available, mark as finished
	reply.ReplyType = Finished
	c.doneChan <- true
	log.Println("All jobs (Map and Reduce) are completed")
	return nil
}

// MapFinish Mark the map job as completed
func (c *Coordinator) MapFinish(args *JobMapFinishArgs, _ *JobFinishReply) error {
	log.Println("MapFinish called")

	processFile := args.ProcessedFilename
	ofilePrefix := args.OfilePrefix

	c.mu.Lock()
	defer c.mu.Unlock()

	c.mapData.fileInfo[processFile] = FileInfo{completed: true, ofilePrefix: ofilePrefix}

	return nil
}

// ReduceFinish Mark the reduce job as completed
func (c *Coordinator) ReduceFinish(args *JobReduceFinishArgs, _ *JobFinishReply) error {
	log.Println("ReduceFinish called")

	reduceId := args.ReduceId
	filename := args.Filename

	c.mu.Lock()
	defer c.mu.Unlock()

	c.reduceData.reduceState[reduceId] = ReduceStatus{finished: true, fileName: filename}
	log.Printf("Finished reducing id: %v", c.reduceData.reduceState[reduceId].finished)

	return nil
}

// Fetch the next map job. Return empty if not available
func (c *Coordinator) fetchMapJob() (JobReply, bool) {
	for {
		fileName := c.getCurrentFile()
		if fileName == "" {
			if c.allJobsCompleted() {
				log.Println("All Map jobs are completed")
				return JobReply{}, false
			}
			time.Sleep(time.Second) // Wait and retry
			continue
		}

		// Assign a map job
		mapJob := JobReply{
			ReplyType: Map,
			MapReply: JobMapReply{
				FileName: fileName,
				MapId:    c.mapData.mapId,
				NReduce:  c.nReduce,
				Finish:   false,
			},
		}

		c.mapData.mapId++
		log.Printf("Assigned map job: %v\n", mapJob.MapReply)
		return mapJob, true
	}
}

// Fetch the next reduce job. Return empty if not available
func (c *Coordinator) fetchReduceJob() (JobReply, bool) {
	if c.reduceData.files == nil {
		c.initializeReduceData()
	}

	for i := 0; i < c.nReduce; i++ {
		reduceState := c.getReduceState(i)
		if !reduceState.finished && time.Since(reduceState.startTime) > time.Second*10 {
			// Assign a reduce job
			reduceState.startTime = time.Now()
			c.reduceData.reduceState[i] = reduceState

			reduceJob := JobReply{
				ReplyType: Reduce,
				ReduceReply: JobReduceReply{
					ReduceId: i,
					Files:    c.reduceData.files,
				},
			}
			return reduceJob, true
		}
	}

	return JobReply{}, false
}

// Initialize reduce data based on completed map jobs
func (c *Coordinator) initializeReduceData() {
	c.reduceData.files = []string{}
	for _, fileInfo := range c.mapData.fileInfo {
		if fileInfo.completed {
			c.reduceData.files = append(c.reduceData.files, fileInfo.ofilePrefix)
		}
	}
	c.reduceData.reduceState = make(map[int]ReduceStatus)
}

func (c *Coordinator) getReduceState(reduceId int) ReduceStatus {
	state, exists := c.reduceData.reduceState[reduceId]
	if !exists {
		state = ReduceStatus{finished: false}
	}
	return state
}

func (c *Coordinator) allJobsCompleted() bool {
	for _, fileInfo := range c.mapData.fileInfo {
		if !fileInfo.completed {
			return false
		}
	}
	return true
}

func (c *Coordinator) getCurrentFile() string {
	for _, filename := range c.mapData.files {
		if c.handlePendingFile(filename) {
			log.Printf("Mapping files: %v", filename)
			return filename
		}
	}
	log.Printf("No file to run")
	return ""
}

// get file to map
func (c *Coordinator) handlePendingFile(filename string) bool {
	fileInfo, exists := c.mapData.fileInfo[filename]
	if !exists || (!fileInfo.completed && time.Since(fileInfo.startTime) > time.Second*10) {
		fileInfo.startTime = time.Now()
		c.mapData.fileInfo[filename] = fileInfo
		return true
	}
	return false
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
