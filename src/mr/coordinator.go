package mr

import (
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"sync"
	"time"
)

type Coordinator struct {
	// Your definitions here.

	// 待处理的文件名
	files []string

	nReduceTask int

	// 判断目前处于map/reduce阶段
	reducePhase bool

	done bool

	// 已完成的map/reduce task数量
	finishedMapTasks    int
	finishedReduceTasks int

	// 记录未分配的task，不同phase下存储map or reduce task id
	// 这里使用channel，结构上视作list，且天然支持并发
	unassignedMapTasks    chan int
	unassignedReduceTasks chan int

	// 记录已经分配的task的状态. 不同phase下存储map or reduce Task
	// 使用哈希表利于指定完成task id的task的删除
	assignedTasks map[int]Task

	// 线程安全相关的锁等
	mu   sync.Mutex
	cond sync.Cond
}

type Task struct {
	id        int
	startTime time.Time
}

// Your code here -- RPC handlers for the worker to call.
// worker申请 map/reduce task的 RPC 方法
func (c *Coordinator) AskForTask(args *TaskArgs, reply *TaskReply) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	if !c.reducePhase {
		for len(c.unassignedMapTasks) == 0 && !c.reducePhase {
			c.cond.Wait()
		}

		if c.reducePhase {
			// assigned a reduce task
			reply.Type = 1
			reply.ID = <-c.unassignedReduceTasks
			reply.NReduce = c.nReduceTask
			c.assignedTasks[reply.ID] = Task{reply.ID, time.Now()}
			return nil
		}

		// assigned a map task
		reply.Type = 0
		reply.ID = <-c.unassignedMapTasks
		reply.FileName = c.files[reply.ID]
		reply.NReduce = c.nReduceTask
		c.assignedTasks[reply.ID] = Task{reply.ID, time.Now()}

	} else {
		for len(c.unassignedReduceTasks) == 0 && !c.done {
			c.cond.Wait()
		}

		if c.done {
			reply.Type = 2
			return nil
		}

		// assigned a reduce task
		reply.Type = 1
		reply.ID = <-c.unassignedReduceTasks
		reply.NReduce = c.nReduceTask
		c.assignedTasks[reply.ID] = Task{reply.ID, time.Now()}
	}

	return nil
}

// worker 报告当前task完成的 RPC 方法
func (c *Coordinator) ReportTaskDone(args *ReportArgs, reply *ReportReply) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	if args.Type == 0 {
		delete(c.assignedTasks, args.ID)
		c.finishedMapTasks++

		// 如果所有map task都完成，则切换到reduce phase
		if !c.reducePhase && c.finishedMapTasks == len(c.files) {
			c.reducePhase = true

			// 开始往unassignedReduceTasks channel中加入元素
			// 开始reduce phase
			for i := 0; i < c.nReduceTask; i++ {
				c.unassignedReduceTasks <- i
			}
			// 唤醒正在等待的 AskForTask 线程
			c.cond.Broadcast()

			// log.Printf("should be reduce phase")
		}
	} else {
		delete(c.assignedTasks, args.ID)
		c.finishedReduceTasks++
		if c.finishedReduceTasks == c.nReduceTask {
			c.done = true
			// 唤醒正在等待的 AskForTask 线程
			c.cond.Broadcast()

			// log.Printf("all task done!")
		}
	}

	return nil
}

// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
func (c *Coordinator) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
}

// start a thread that listens for RPCs from worker.go
func (c *Coordinator) server(sockname string) {
	rpc.Register(c)
	rpc.HandleHTTP()
	os.Remove(sockname)
	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatalf("listen error %s: %v", sockname, e)
	}
	go http.Serve(l, nil)
}

// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
func (c *Coordinator) Done() bool {

	// Your code here.
	c.mu.Lock()
	defer c.mu.Unlock()

	return c.done
}

// 检查各worker是否"存活"
func (c *Coordinator) CheckAlive() {
	for {
		// check per second
		time.Sleep(500 * time.Millisecond)

		c.mu.Lock()

		if c.done {
			c.mu.Unlock()
			return
		}

		now := time.Now()
		for id, task := range c.assignedTasks {
			if now.Sub(task.startTime) > 10*time.Second {
				delete(c.assignedTasks, id)

				if !c.reducePhase {
					c.unassignedMapTasks <- id
				} else {
					c.unassignedReduceTasks <- id
				}

				c.cond.Broadcast()
			}
		}

		c.mu.Unlock()
	}
}

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(sockname string, files []string, nReduce int) *Coordinator {
	c := Coordinator{}

	// Your code here.
	// nReduceTask = nReduce // a low level error about processes
	c.nReduceTask = nReduce
	c.files = files
	c.assignedTasks = make(map[int]Task)
	c.mu = sync.Mutex{}
	c.cond = *sync.NewCond(&c.mu)

	c.unassignedMapTasks = make(chan int, len(files))
	for i := range files {
		c.unassignedMapTasks <- i
	}
	c.unassignedReduceTasks = make(chan int, c.nReduceTask)

	// 后台运行worker状态检查线程
	go c.CheckAlive()

	c.server(sockname)
	return &c
}
