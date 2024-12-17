package mr

import (
	"fmt"
	"io/ioutil"
	"os"
	"sort"
	"strings"
)
import "log"
import "net/rpc"
import "hash/fnv"

type KeyValue struct {
	Key   string
	Value string
}

type ByKey []KeyValue

func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

// Worker function to perform map and reduce operations
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	for {
		jobReply := fetchJob()

		switch jobReply.ReplyType {
		case Map:
			processMapJob(jobReply.MapReply, mapf)
		case Reduce:
			processReduceJob(jobReply.ReduceReply, reducef)
		case Finished:
			fmt.Println("All jobs finished")
			return
		default:
			fmt.Println("No job found")
		}
	}
}

// Process the map job
func processMapJob(mapReply JobMapReply, mapf func(string, string) []KeyValue) {
	if mapReply.Finish {
		fmt.Printf("Map job finished: %v\n", mapReply.Finish)
	}

	filename, content := getFile(mapReply.FileName)
	fmt.Printf("Mapping file: %v\n", filename)

	// Perform the map operation
	kva := mapf(filename, string(content))
	sort.Sort(ByKey(kva))

	ofilePrefix := fmt.Sprintf("mr-out-%d", mapReply.MapId)
	storeKva(ofilePrefix, kva, mapReply.NReduce)

	fmt.Printf("Finished mapping file: %v\n", filename)

	notifyMapFinish(ofilePrefix, filename)
}

// Process the reduce job
func processReduceJob(reduceReply JobReduceReply, reducef func(string, []string) string) {
	fmt.Printf("Reduce job running for: %v\n", reduceReply.ReduceId)

	intermediate := loadIntermediateData(reduceReply.Files, reduceReply.ReduceId)
	keys := sortedKeys(intermediate)

	oname := fmt.Sprintf("mr-out-%d", reduceReply.ReduceId)
	ofile, err := os.Create(oname)
	if err != nil {
		log.Fatalf("Cannot create %v", oname)
	}
	defer ofile.Close()

	for _, key := range keys {
		output := reducef(key, intermediate[key])
		fmt.Fprintf(ofile, "%v %v\n", key, output)
	}

	notifyReduceFinish(reduceReply.ReduceId, oname)
}

// Load intermediate data to reduce from the files
func loadIntermediateData(files []string, reduceId int) map[string][]string {
	intermediate := make(map[string][]string)

	for _, filePrefix := range files {
		filename := fmt.Sprintf("%v-%d", filePrefix, reduceId)
		_, content := getFile(filename)

		log.Printf("Reducing file: %s\n", filename)
		lines := strings.Split(string(content), "\n")

		for _, line := range lines {
			if line == "" {
				continue
			}
			kv := strings.SplitN(line, " ", 2)
			key := kv[0]
			var value []string = strings.Fields(strings.Trim(kv[1], "[]"))

			for _, v := range value {
				intermediate[key] = append(intermediate[key], v)
			}
		}
	}

	return intermediate
}

// Sort the keys
func sortedKeys(intermediate map[string][]string) []string {
	var keys []string
	for key := range intermediate {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	return keys
}

// Notify the coordinator that the map job has finished
func notifyMapFinish(ofilePrefix, filename string) {
	mapFinishArgs := JobMapFinishArgs{OfilePrefix: ofilePrefix, ProcessedFilename: filename}

	log.Printf("Map task finished for file: %v\n", filename)

	call("Coordinator.MapFinish", &mapFinishArgs, nil)
}

// Notify the coordinator that the reduce job has finished
func notifyReduceFinish(reduceId int, filename string) {
	reduceFinishArgs := JobReduceFinishArgs{ReduceId: reduceId, Filename: filename}
	var reduceFinishReply JobFinishReply
	fmt.Printf("Reduce task finished for file: %v\n", filename)

	call("Coordinator.ReduceFinish", &reduceFinishArgs, &reduceFinishReply)
}

// Get the file content
func getFile(filename string) (string, []byte) {
	file, err := os.Open(filename)
	if err != nil {
		log.Printf("cannot open %v", filename)
		return "", []byte{}
	}
	content, err := ioutil.ReadAll(file)

	if err != nil {
		log.Printf("cannot read %v", filename)
		return "", []byte{}
	}
	file.Close()

	fmt.Printf("Successfully got file: %v\n", filename)

	return filename, content
}

func storeKva(ofilePrefix string, kva []KeyValue, nReduce int) {
	i := 0
	files := make(map[int]*os.File)
	for i < len(kva) {
		j := i + 1
		for j < len(kva) && kva[j].Key == kva[i].Key {
			j++
		}
		values := []string{}
		for k := i; k < j; k++ {
			values = append(values, kva[k].Value)
		}

		currentKey := kva[i].Key
		workerNum := ihash(currentKey) % nReduce

		oname := fmt.Sprintf("%v-%d", ofilePrefix, workerNum)

		if files[workerNum] == nil {
			file, err := os.Create(oname)
			if err != nil {
				fmt.Printf("Error creating file %s: %v\n", oname, err)
			}
			files[workerNum] = file
			defer file.Close()
		}

		ofile := files[workerNum]

		fmt.Fprintf(ofile, "%v %v\n", currentKey, values)

		i = j
	}
}

// Fetch the job from the coordinator
func fetchJob() JobReply {
	var jobRepl JobReply
	var args JobArgs

	ok := call("Coordinator.FetchJob", &args, &jobRepl)

	if ok {
		return jobRepl
	}
	// throw error here
	return jobRepl
}

func call(rpcname string, args interface{}, reply interface{}) bool {
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
