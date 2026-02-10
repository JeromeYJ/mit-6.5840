package mr

import (
	"encoding/json"
	"fmt"
	"hash/fnv"
	"io/ioutil"
	"log"
	"net/rpc"
	"os"
	"sort"
	"strconv"
	"strings"
)

// Map functions return a slice of KeyValue.
type KeyValue struct {
	Key   string
	Value string
}

// for sorting by key
type ByKey []KeyValue

// for sorting by key.
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

var (
	coordSockName string // socket for coordinator
	nReduceTask   int    // reduce task 数量
)

// main/mrworker.go calls this function.
func Worker(sockname string, mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	coordSockName = sockname

	// Your worker implementation here.
	for {
		ok, reply := CallAskForTask()
		if ok {
			if nReduceTask == 0 {
				nReduceTask = reply.NReduce
			}

			switch reply.Type {
			case 0:
				// 若为map task
				ExecuteMap(reply, mapf)

				ok1, _ := CallReportTaskDone(reply.Type, reply.ID)
				if !ok1 {
					log.Fatal("call ReportTaskDone() failed!\n")
				}
			case 1:
				// 若为reduce task
				ExecuteReduce(reply, reducef)

				ok1, _ := CallReportTaskDone(reply.Type, reply.ID)
				if !ok1 {
					log.Fatal("call ReportTaskDone() failed!\n")
				}
			case 2:
				// 若已经全部完成
				// log.Printf("all tasks done!")
				return
			}
		} else {
			log.Fatal("call AskForTask() failed!\n")
		}
	}

	// uncomment to send the Example RPC to the coordinator.
	// CallExample()

}

func CallAskForTask() (bool, *TaskReply) {
	args := new(TaskArgs)
	// reply结构体内值使用默认赋值，否则RPC系统给出错误结果
	reply := TaskReply{}

	// 向coordinator请求task
	ok := call("Coordinator.AskForTask", args, &reply)

	return ok, &reply
}

func CallReportTaskDone(taskType int, taskID int) (bool, *ReportReply) {
	// log.Printf("Finished %v task %v.\n", func() string {
	// 	if taskType == 0 {
	// 		return "map"
	// 	} else {
	// 		return "reduce"
	// 	}
	// }(), taskID)

	args := ReportArgs{taskType, taskID}
	// reply结构体内值使用默认赋值，否则RPC系统给出错误结果
	reply := new(ReportReply)

	// 向coordinator请求task
	ok := call("Coordinator.ReportTaskDone", args, reply)

	return ok, reply
}

func ExecuteMap(reply *TaskReply, mapf func(string, string) []KeyValue) {
	// log.Printf("Assigned map task %v.\n", reply.ID)

	filename := reply.FileName
	file, err := os.Open(filename)
	if err != nil {
		log.Fatalf("cannot open %v", filename)
	}
	content, err := ioutil.ReadAll(file)
	if err != nil {
		log.Fatalf("cannot read %v", filename)
	}
	file.Close()
	kva := mapf(filename, string(content))

	// sort.Sort(ByHash(kva))
	Partition(kva, reply.ID)
}

func ExecuteReduce(reply *TaskReply, reducef func(string, []string) string) {
	// log.Printf("Assigned reduce task %v.\n", reply.ID)

	// 将对应intermediate files的内容合并
	intermediate := MergeKva(reply.ID)

	sort.Sort(ByKey(intermediate))
	oname := "mr-out-" + strconv.Itoa(reply.ID)
	ofile, _ := os.Create(oname)

	for i := 0; i < len(intermediate); {
		j := i + 1
		for j < len(intermediate) && intermediate[j].Key == intermediate[i].Key {
			j++
		}
		value := []string{}
		for k := i; k < j; k++ {
			value = append(value, intermediate[k].Value)
		}
		output := reducef(intermediate[i].Key, value)

		fmt.Fprintf(ofile, "%v %v\n", intermediate[i].Key, output)
		i = j
	}

	ofile.Close()
}

// 分区函数
// 用于将mapf生成的intermedate key/value pairs分到对应 intermediate file
func Partition(kva []KeyValue, ID int) {
	intermediateFiles := make([]*os.File, nReduceTask)

	// Create intermediate files
	for i := 0; i < nReduceTask; i++ {
		// named "mr-X-Y"
		intermediateFiles[i], _ = os.Create("mr-" + strconv.Itoa(ID) + "-" + strconv.Itoa(i))
	}

	intermediateKva := make([][]KeyValue, nReduceTask)
	for _, kv := range kva {
		id := ihash(kv.Key) % nReduceTask
		intermediateKva[id] = append(intermediateKva[id], kv)
	}

	for i, kva := range intermediateKva {
		enc := json.NewEncoder(intermediateFiles[i])
		for _, kv := range kva {
			err := enc.Encode(&kv)
			if err != nil {
				log.Fatalf(err.Error())
			}
		}
	}
}

// 检查文件名是否符合 mr-X-Y 格式并提取Y值
func isMRFileWithY(filename string, targetY int) (bool, int) {
	parts := strings.Split(filename, "-")
	if len(parts) != 3 || parts[0] != "mr" {
		return false, -1
	}

	// 验证X和Y都是整数
	_, err1 := strconv.Atoi(parts[1])
	y, err2 := strconv.Atoi(parts[2])

	if err1 != nil || err2 != nil {
		return false, -1
	}

	// 检查Y是否等于目标值
	return y == targetY, y
}

// 获取当前目录下所有Y值为targetY的mr文件
func GetMRFilesByY(targetY int) ([]string, error) {
	var mrFiles []string

	// 读取当前目录
	entries, err := os.ReadDir(".")

	// log.Printf("%v", entries)
	if err != nil {
		return nil, fmt.Errorf("failed to read directory: %v", err)
	}

	for _, entry := range entries {
		if entry.IsDir() {
			continue
		}

		filename := entry.Name()
		if ok, _ := isMRFileWithY(filename, targetY); ok {
			mrFiles = append(mrFiles, filename)
			// fmt.Printf("Found: %s (Y=%d)\n", filename, actualY)
		}
	}

	return mrFiles, nil
}

// 合并reduce编号id的intermediate files
func MergeKva(id int) []KeyValue {
	filesName, err := GetMRFilesByY(id)
	// log.Printf("intermediate filename: %v", filesName)

	if err != nil {
		fmt.Printf("Error: %v\n", err)
		return nil
	}
	kva := []KeyValue{}

	for _, filename := range filesName {
		file, err := os.Open(filename)
		if err != nil {
			log.Fatalf("cannot open %v", filename)
		}
		dec := json.NewDecoder(file)
		for {
			var kv KeyValue
			if err := dec.Decode(&kv); err != nil {
				break
			}
			kva = append(kva, kv)
		}
	}
	return kva
}

// example function to show how to make an RPC call to the coordinator.
//
// the RPC argument and reply types are defined in rpc.go.
func CallExample() {

	// declare an argument structure.
	args := ExampleArgs{}

	// fill in the argument(s).
	args.X = 99

	// declare a reply structure.
	reply := ExampleReply{}

	// send the RPC request, wait for the reply.
	// the "Coordinator.Example" tells the
	// receiving server that we'd like to call
	// the Example() method of struct Coordinator.
	ok := call("Coordinator.Example", &args, &reply)
	if ok {
		// reply.Y should be 100.
		fmt.Printf("reply.Y %v\n", reply.Y)
	} else {
		fmt.Printf("call failed!\n")
	}
}

// send an RPC request to the coordinator, wait for the response.
// usually returns true.
// returns false if something goes wrong.
func call(rpcname string, args interface{}, reply interface{}) bool {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	c, err := rpc.DialHTTP("unix", coordSockName)
	if err != nil {
		log.Fatal("dialing:", err)
	}
	defer c.Close()

	if err := c.Call(rpcname, args, reply); err == nil {
		return true
	}
	log.Printf("%d: call failed err %v", os.Getpid(), err)
	return false
}
