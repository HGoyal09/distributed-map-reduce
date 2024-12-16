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

		//jobReply := fetchJob()
		//
		//switch jobReply.ReplyType {
		//case Map:
		//	mapReply := jobReply.MapReply
		//	if mapReply.Finish {
		//		fmt.Printf("Map job finished: %v\n", mapReply.Finish)
		//		break
		//	}
		//	filename, content := getFile(mapReply.FileName)
		//
		//	fmt.Printf("Mapping file: %v\n", filename)
		//
		//	kva := mapf(filename, string(content))
		//	sort.Sort(ByKey(kva))
		//
		//	fmt.Println(mapReply.MapId, mapReply.NReduce)
		//
		//	ofilePrefix := fmt.Sprintf("mr-out-%d", mapReply.MapId)
		//
		//	storeKva(ofilePrefix, kva, mapReply.NReduce)
		//
		//	fmt.Printf("Finished mapping file: %v\n", filename)
		//
		//	notifyFinish(ofilePrefix, filename)
		//case Reduce:
		//	reduceReply := jobReply.ReduceReply
		//	fmt.Printf("Reduce job running for: %v\n", reduceReply.ReduceId)
		//
		//	intermediate := make(map[string][]string)
		//
		//	for _, filePrefix := range reduceReply.Files {
		//		filename := fmt.Sprintf("%v-%d", filePrefix, reduceReply.ReduceId)
		//		_, content := getFile(filename)
		//
		//		fmt.Printf("Reducing file: %s\n", filename)
		//		lines := strings.Split(string(content), "\n")
		//
		//		for _, line := range lines {
		//			if line == "" {
		//				continue
		//			}
		//			kv := strings.SplitN(line, " ", 2)
		//			key := kv[0]
		//			value := kv[1]
		//
		//			if _, ok := intermediate[key]; !ok {
		//				intermediate[key] = []string{}
		//			}
		//			intermediate[key] = append(intermediate[key], value)
		//		}
		//	}
		//
		//	// Sort the keys
		//	var keys []string
		//	for key := range intermediate {
		//		keys = append(keys, key)
		//	}
		//	sort.Strings(keys)
		//
		//	// Create the output file
		//	oname := fmt.Sprintf("mr-out-%d", reduceReply.ReduceId)
		//	ofile, err := os.Create(oname)
		//	if err != nil {
		//		log.Fatalf("cannot create %v", oname)
		//	}
		//	defer ofile.Close()
		//
		//	// Process each key and write the result to the output file
		//	for _, key := range keys {
		//		output := reducef(key, intermediate[key])
		//		fmt.Fprintf(ofile, "%v %v\n", key, output)
		//	}
		//
		//	notifyReduceFinish(reduceReply.ReduceId, oname)
		//case Finished:
		//	fmt.Println("All jobs finished")
		//	return
		//default:
		//	fmt.Println("No job found")
		//}
	}
}

func processMapJob(mapReply JobMapReply, mapf func(string, string) []KeyValue) {
	if mapReply.Finish {
		fmt.Printf("Map job finished: %v\n", mapReply.Finish)
		return
	}

	filename, content := getFile(mapReply.FileName)
	fmt.Printf("Mapping file: %v\n", filename)

	// Perform the map operation
	kva := mapf(filename, string(content))
	sort.Sort(ByKey(kva))

	ofilePrefix := fmt.Sprintf("mr-out-%d", mapReply.MapId)
	storeKva(ofilePrefix, kva, mapReply.NReduce)

	fmt.Printf("Finished mapping file: %v\n", filename)
	notifyFinish(ofilePrefix, filename)
}

func processReduceJob(reduceReply JobReduceReply, reducef func(string, []string) string) {
	fmt.Printf("Reduce job running for: %v\n", reduceReply.ReduceId)

	intermediate := loadIntermediateData(reduceReply)

	// Sort the keys
	keys := sortedKeys(intermediate)

	// Create the output file
	oname := fmt.Sprintf("mr-out-%d", reduceReply.ReduceId)
	ofile, err := os.Create(oname)
	if err != nil {
		log.Fatalf("Cannot create %v", oname)
	}
	defer ofile.Close()

	// Process each key and write the result
	for _, key := range keys {
		output := reducef(key, intermediate[key])
		fmt.Fprintf(ofile, "%v %v\n", key, output)
	}

	notifyReduceFinish(reduceReply.ReduceId, oname)
}

func loadIntermediateData(reduceReply JobReduceReply) map[string][]string {
	intermediate := make(map[string][]string)

	for _, filePrefix := range reduceReply.Files {
		filename := fmt.Sprintf("%v-%d", filePrefix, reduceReply.ReduceId)
		_, content := getFile(filename)

		fmt.Printf("Reducing file: %s\n", filename)
		lines := strings.Split(string(content), "\n")

		for _, line := range lines {
			if line == "" {
				continue
			}
			kv := strings.SplitN(line, " ", 2)
			key := kv[0]
			value := kv[1]
			intermediate[key] = append(intermediate[key], value)
		}
	}

	return intermediate
}

func sortedKeys(intermediate map[string][]string) []string {
	var keys []string
	for key := range intermediate {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	return keys
}

func notifyFinish(ofilePrefix, filename string) {
	mapFinishArgs := JobMapFinishArgs{OfilePrefix: ofilePrefix, ProcessedFilename: filename}

	log.Printf("Map task finished for file: %v\n", filename)

	call("Coordinator.MapFinish", &mapFinishArgs, nil)
}

func notifyReduceFinish(reduceId int, filename string) {
	reduceFinishArgs := JobReduceFinishArgs{ReduceId: reduceId, Filename: filename}
	var reduceFinishReply JobFinishReply
	fmt.Printf("Reduce task finished for file: %v\n", filename)

	call("Coordinator.ReduceFinish", &reduceFinishArgs, &reduceFinishReply)
}

func getFile(filename string) (string, []byte) {
	file, err := os.Open(filename)
	if err != nil {
		log.Fatalf("cannot open %v", filename)
	}
	content, err := ioutil.ReadAll(file)

	if err != nil {
		log.Fatalf("cannot read %v", filename)
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

func fetchJob() JobReply {
	var jobRepl JobReply
	var args JobArgs

	ok := call("Coordinator.FetchJob", &args, &jobRepl)

	fmt.Println(ok, jobRepl.MapReply.FileName)

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
