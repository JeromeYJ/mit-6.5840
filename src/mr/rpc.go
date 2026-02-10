package mr

//
// RPC definitions.
//
// remember to capitalize all names.
//

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
type TaskArgs int

type TaskReply struct {
	// task类型，0为map，1为reduce，2为所有任务已完成
	Type int
	// map/reduce task编号
	ID int
	// map task对应的文件名。若为reduce task则为空字符串
	FileName string
	// reduce task数量
	NReduce int
}

type ReportArgs struct {
	// 完成的task类型，0为map，1为reduce
	Type int
	// 完成的task id
	ID int
}

type ReportReply int
