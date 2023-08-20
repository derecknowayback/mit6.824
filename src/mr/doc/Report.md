# Lab1 Map-Reduce
## 思路
总的任务就是把各个文件作为输入，得到一些中间输出，再根据这些中间输出产生nReduce个最后结果。

## BUG Report
1. Map任务需要我们创建一个临时文件，但是需要注意，Map任务可能会fail，这样就有可能产生一个“不完整”的中间文件。 所以我们需要确保“产生中间文件+写完中间文件”是一个原子化的过程。怎么做到呢？看看824官方的建议:
>  To ensure that nobody observes partially written files in the presence of crashes, the MapReduce paper mentions the trick of using a temporary file and atomically renaming it once it is completely written. You can use `ioutil.TempFile` to create a temporary file and os.Rename to atomically rename it.

代码实现:
```go
// 收尾  
for hash, wrapper := range middleFileMap {  
    wrapper.writer.Flush()  
    middleFileName := fmt.Sprintf(MiddleFileFormat, hash, job.Name)  
    err := os.Rename(wrapper.file.Name(), middleFileName)  
    if err != nil {  
       logger.Printf("Rename file error: %v\n", err)  
    }  
    wrapper.file.Close()  
}  
```
中间文件最终的名字格式为“middle-file-hash值-文件序号”，文件序号由master提供
先写hash值是为了将来Reduce-worker查找文件的方便。


2. Map函数的原型如下:
```go
mapf func(string, string) []KeyValue
```
这里两个参数一个是文件名，一个是blob(content)，也就是文章的内容。文件名不能自己乱动，不然会通过不了indexer测试。


3. 每次Reduce任务结束之后，需要将中间文件删除，这一步不需要原子化（有可能会报错，但是可以避免且概率不大）
```go
SignalJobFinished(*job) // 先结束任务再清理
for _, d := range dir {  
    if d.IsDir() {  
       continue  
    }  
    name := d.Name()  
    if !strings.HasPrefix(name, prefix) {  
       continue  
    }  
    err = os.Remove(name)  
    if err != nil {  
       logger.Println(fmt.Sprintf("Remove middle file err: %v", err))  
    }  
}
```

4. AssignJob分发任务的逻辑是: 如果所有的Map任务完成了，那我们就产生Reduce任务，而且Master会在内存里保存"所有Map任务是否完成"这个状态。
```go
// AssignJob handles worker's request for a job
func (c *Coordinator) AssignJob(nothing Nothing, job *Job) error {  
    now := time.Now()  
    isJobTimeOut := func(job Job) bool {  
       if job.Start.IsZero() {  
          return false  
       }  
       return job.Start.Add(DefaultTimeout).Before(now)  
    }  
    var res Job  
    var found bool  
    // lock  
    c.lock.Lock()  
  
    for key, job := range c.JobList {  
       if !isJobTimeOut(job) && !job.Assigned {  
          job.Assigned = true  
          job.Start = now  
          c.JobList[key] = job  
          res = c.JobList[key]  
          found = true  
          break  
       }  
    }  
  
    // 如果所有map-job都结束了,产生reduce-job  
    if !found && len(c.JobList) == 0 && !c.ProduceReduce {  
       c.JobList = map[string]Job{}  
       for i := 0; i < c.nReduce; i++ {  
          c.JobList[strconv.Itoa(i)] = Job{Type: ReduceJobType, Name: strconv.Itoa(i), Assigned: false}  
       }  
       c.ProduceReduce = true  
       found = true  
       res = c.JobList["0"]  
       res.Assigned = true  
       res.Start = now  
       c.JobList["0"] = res  
    }  
  
    //log.Println("The JobList are: ")  
    //log.Println(c.GetJobStr())  
    // unlock    c.lock.Unlock()  
    if !found {  
       //log.Println("No find job ...")  
       *job = Job{Type: NoJobType}  
       return nil  
    }  
  
    *job = res  
    return nil  
}
```


## 超时时间
超时时间是一个我们需要考虑的问题。

```go
func (c *Coordinator) GetJobDone(job Job, nothing *Nothing) error {  
    // todo bug -> 有一个问题是, 假设我现在worker1太慢了超时了,我把工作assign给了另外一个worker2,ok,但是这时候突然又收到worker1的结束信号,豁  
    // 这不是会重复一个工作吗?  
    c.lock.Lock()  
    log.Println(fmt.Sprintf("Job-%s done: %s", job.Name, job.ToStr()))  
    delete(c.JobList, job.Name+job.Index)  
    c.lock.Unlock()  
    return nil  
}
```
可以看到，我们比较担心因为各种各样的原因，worker通知master的消息被阻塞延迟到达，导致master这边job的状态异常。
所以我们需要保证timeout足够长，可以让worker完成工作并且通知master。


看看下面这个报错:
```
TIMES: 310  
NOW: 2023-08-20 06:39:45.208064 +0800 CST m=+5463.217894085  
*** Cannot find timeout command; proceeding without timeouts.  
*** Starting crash test.  
2023/08/20 06:39:45 Check TIMEOUT  
Worker-2   2023/08/20 06:39:46 job-3 done: { Name:[3]  Index:[../pg-grimm.txt]  Type:[Map]  Assigned:[true] Start:[2023-08-20 06:39:46.354188 +0800 CST]}  
Worker-1   2023/08/20 06:39:46 job-4 done: { Name:[4]  Index:[../pg-huckleberry_finn.txt]  Type:[Map]  Assigned:[true] Start:[2023-08-20 06:39:46.35395 +0800 CST]}  
2023/08/20 06:39:46 Job-3 done: { Name:[3]  Index:[../pg-grimm.txt]  Type:[Map]  Assigned:[true] Start:[2023-08-20 06:39:46.354188 +0800 CST]}  
2023/08/20 06:39:46 Job-4 done: { Name:[4]  Index:[../pg-huckleberry_finn.txt]  Type:[Map]  Assigned:[true] Start:[2023-08-20 06:39:46.35395 +0800 CST]}  
Worker-1   2023/08/20 06:39:46 job-6 done: { Name:[6]  Index:[../pg-sherlock_holmes.txt]  Type:[Map]  Assigned:[true] Start:[2023-08-20 06:39:46.363648 +0800 CST]}  
2023/08/20 06:39:46 Job-6 done: { Name:[6]  Index:[../pg-sherlock_holmes.txt]  Type:[Map]  Assigned:[true] Start:[2023-08-20 06:39:46.363648 +0800 CST]}  
Worker-0   2023/08/20 06:39:47 job-1 done: { Name:[1]  Index:[../pg-dorian_gray.txt]  Type:[Map]  Assigned:[true] Start:[2023-08-20 06:39:47.39672 +0800 CST]}  
2023/08/20 06:39:47 Job-1 done: { Name:[1]  Index:[../pg-dorian_gray.txt]  Type:[Map]  Assigned:[true] Start:[2023-08-20 06:39:47.39672 +0800 CST]}  
2023/08/20 06:39:48 Check TIMEOUT  
Worker-2   2023/08/20 06:39:48 job-5 done: { Name:[5]  Index:[../pg-metamorphosis.txt]  Type:[Map]  Assigned:[true] Start:[2023-08-20 06:39:46.363196 +0800 CST]}  
2023/08/20 06:39:48 Job-5 done: { Name:[5]  Index:[../pg-metamorphosis.txt]  Type:[Map]  Assigned:[true] Start:[2023-08-20 06:39:46.363196 +0800 CST]}  
2023/08/20 06:39:51 Check TIMEOUT  
2023/08/20 06:39:54 Check TIMEOUT  
2023/08/20 06:39:54 Job-7 time out: { Name:[7]  Index:[../pg-tom_sawyer.txt]  Type:[Map]  Assigned:[true] Start:[2023-08-20 06:39:46.353979 +0800 CST m=+1.042832584]}  
2023/08/20 06:39:54 Job-0 time out: { Name:[0]  Index:[../pg-being_ernest.txt]  Type:[Map]  Assigned:[true] Start:[2023-08-20 06:39:46.367249 +0800 CST m=+1.056100793]}  
2023/08/20 06:39:54 Job-2 time out: { Name:[2]  Index:[../pg-frankenstein.txt]  Type:[Map]  Assigned:[true] Start:[2023-08-20 06:39:46.354209 +0800 CST m=+1.043062376]}  
Worker-0   2023/08/20 06:39:54 job-0 done: { Name:[0]  Index:[../pg-being_ernest.txt]  Type:[Map]  Assigned:[true] Start:[2023-08-20 06:39:54.437979 +0800 CST]}  
2023/08/20 06:39:54 Job-0 done: { Name:[0]  Index:[../pg-being_ernest.txt]  Type:[Map]  Assigned:[true] Start:[2023-08-20 06:39:54.437979 +0800 CST]}  
2023/08/20 06:39:57 Check TIMEOUT  
2023/08/20 06:40:00 Check TIMEOUT  
2023/08/20 06:40:00 Job-7 time out: { Name:[7]  Index:[../pg-tom_sawyer.txt]  Type:[Map]  Assigned:[true] Start:[2023-08-20 06:39:54.437811 +0800 CST m=+9.125708168]}  
2023/08/20 06:40:00 Job-2 time out: { Name:[2]  Index:[../pg-frankenstein.txt]  Type:[Map]  Assigned:[true] Start:[2023-08-20 06:39:54.429488 +0800 CST m=+9.117386418]}  
Worker-0   2023/08/20 06:40:00 job-7 done: { Name:[7]  Index:[../pg-tom_sawyer.txt]  Type:[Map]  Assigned:[true] Start:[2023-08-20 06:40:00.46727 +0800 CST]}  
2023/08/20 06:40:00 Job-7 done: { Name:[7]  Index:[../pg-tom_sawyer.txt]  Type:[Map]  Assigned:[true] Start:[2023-08-20 06:40:00.46727 +0800 CST]}  
2023/08/20 06:40:03 Check TIMEOUT  
2023/08/20 06:40:06 Check TIMEOUT  
2023/08/20 06:40:06 Job-2 time out: { Name:[2]  Index:[../pg-frankenstein.txt]  Type:[Map]  Assigned:[true] Start:[2023-08-20 06:40:00.478875 +0800 CST m=+15.166292293]}  
Worker-0   2023/08/20 06:40:07 job-2 done: { Name:[2]  Index:[../pg-frankenstein.txt]  Type:[Map]  Assigned:[true] Start:[2023-08-20 06:40:00.478875 +0800 CST]}  
2023/08/20 06:40:07 Job-2 done: { Name:[2]  Index:[../pg-frankenstein.txt]  Type:[Map]  Assigned:[true] Start:[2023-08-20 06:40:00.478875 +0800 CST]}  
Worker-0   2023/08/20 06:40:07 job-0 kvs -- [{c 138885} {c 453168} {c 441033} {c 540174} {c 594262} {c 139054} {c 581863} {c 412665}]  
Worker-2   2023/08/20 06:40:07 job-3 kvs -- []  
Worker-2   2023/08/20 06:40:07 job-3 done: { Name:[3]  Index:[]  Type:[Reduce]  Assigned:[true] Start:[2023-08-20 06:40:07.889189 +0800 CST]}  
2023/08/20 06:40:07 Job-3 done: { Name:[3]  Index:[]  Type:[Reduce]  Assigned:[true] Start:[2023-08-20 06:40:07.889189 +0800 CST]}  
Worker-2   2023/08/20 06:40:07 job-8 kvs -- []  
Worker-2   2023/08/20 06:40:07 job-8 done: { Name:[8]  Index:[]  Type:[Reduce]  Assigned:[true] Start:[2023-08-20 06:40:07.892294 +0800 CST]}  
2023/08/20 06:40:07 Job-8 done: { Name:[8]  Index:[]  Type:[Reduce]  Assigned:[true] Start:[2023-08-20 06:40:07.892294 +0800 CST]}  
Worker-2   2023/08/20 06:40:07 job-5 kvs -- [{d xyzzy} {d xyzzy} {d xyzzy} {d xyzzy} {d xyzzy} {d xyzzy} {d xyzzy} {d xyzzy}]  
Worker-2   2023/08/20 06:40:07 write d xyzzy xyzzy xyzzy xyzzy xyzzy xyzzy xyzzy xyzzy  
Worker-2   2023/08/20 06:40:07 job-5 done: { Name:[5]  Index:[]  Type:[Reduce]  Assigned:[true] Start:[2023-08-20 06:40:07.894311 +0800 CST]}  
2023/08/20 06:40:07 Job-5 done: { Name:[5]  Index:[]  Type:[Reduce]  Assigned:[true] Start:[2023-08-20 06:40:07.894311 +0800 CST]}  
  
  
ATTENTION  
Worker-2   2023/08/20 06:40:07 job-9 kvs -- [{b 22} {b 21} {b 22} {b 15} {b 26} {b 23} {b 25} {b 20}]  
  
  
  
Worker-1   2023/08/20 06:40:08 job-6 kvs -- []  
Worker-1   2023/08/20 06:40:08 job-6 done: { Name:[6]  Index:[]  Type:[Reduce]  Assigned:[true] Start:[2023-08-20 06:40:08.519438 +0800 CST]}  
2023/08/20 06:40:08 Job-6 done: { Name:[6]  Index:[]  Type:[Reduce]  Assigned:[true] Start:[2023-08-20 06:40:08.519438 +0800 CST]}  
Worker-1   2023/08/20 06:40:08 job-7 kvs -- []  
Worker-1   2023/08/20 06:40:08 job-7 done: { Name:[7]  Index:[]  Type:[Reduce]  Assigned:[true] Start:[2023-08-20 06:40:08.524411 +0800 CST]}  
2023/08/20 06:40:08 Job-7 done: { Name:[7]  Index:[]  Type:[Reduce]  Assigned:[true] Start:[2023-08-20 06:40:08.524411 +0800 CST]}  
Worker-1   2023/08/20 06:40:08 job-1 kvs -- []  
Worker-1   2023/08/20 06:40:08 job-1 done: { Name:[1]  Index:[]  Type:[Reduce]  Assigned:[true] Start:[2023-08-20 06:40:08.526375 +0800 CST]}  
2023/08/20 06:40:08 Job-1 done: { Name:[1]  Index:[]  Type:[Reduce]  Assigned:[true] Start:[2023-08-20 06:40:08.526375 +0800 CST]}  
Worker-1   2023/08/20 06:40:08 job-2 kvs -- [{a ../pg-being_ernest.txt} {a ../pg-dorian_gray.txt} {a ../pg-frankenstein.txt} {a ../pg-grimm.txt} {a ../pg-huckleberry_finn.txt} {a ../pg-metamorphosis.txt} {a ../pg-sherlock_holmes.txt} {a ../pg-tom_sawyer.txt}]  
Worker-2   2023/08/20 06:40:08 job-4 kvs -- []  
Worker-2   2023/08/20 06:40:08 job-4 done: { Name:[4]  Index:[]  Type:[Reduce]  Assigned:[true] Start:[2023-08-20 06:40:08.738342 +0800 CST]}  
2023/08/20 06:40:08 Job-4 done: { Name:[4]  Index:[]  Type:[Reduce]  Assigned:[true] Start:[2023-08-20 06:40:08.738342 +0800 CST]}  
2023/08/20 06:40:09 Check TIMEOUT  
2023/08/20 06:44:55 Check TIMEOUT  
Worker-2   2023/08/20 06:44:55 job-2 done: { Name:[2]  Index:[../pg-frankenstein.txt]  Type:[Map]  Assigned:[true] Start:[2023-08-20 06:40:06.512721 +0800 CST]}  
2023/08/20 06:44:55 Job-2 done: { Name:[2]  Index:[../pg-frankenstein.txt]  Type:[Map]  Assigned:[true] Start:[2023-08-20 06:40:06.512721 +0800 CST]}  
2023/08/20 06:44:58 Check TIMEOUT  
2023/08/20 06:44:58 Job-2 time out: { Name:[2]  Index:[]  Type:[Reduce]  Assigned:[true] Start:[2023-08-20 06:40:08.528982 +0800 CST m=+23.215953418]}  
2023/08/20 06:44:58 Job-0 time out: { Name:[0]  Index:[]  Type:[Reduce]  Assigned:[true] Start:[2023-08-20 06:40:07.693158 +0800 CST m=+22.380168126]}  
2023/08/20 06:44:58 Job-9 time out: { Name:[9]  Index:[]  Type:[Reduce]  Assigned:[true] Start:[2023-08-20 06:40:07.899931 +0800 CST m=+22.586931459]}  
  
  
  
Worker-2   2023/08/20 06:44:58 write b 15 20 21 22 22 23 25 26  
Worker-2   2023/08/20 06:44:58 job-9 done: { Name:[9]  Index:[]  Type:[Reduce]  Assigned:[true] Start:[2023-08-20 06:40:07.899931 +0800 CST]}  
  
  
  
2023/08/20 06:44:58 Job-9 done: { Name:[9]  Index:[]  Type:[Reduce]  Assigned:[true] Start:[2023-08-20 06:40:07.899931 +0800 CST]}  
  
  
Because last time job-9 has successfully written b to the output file, so it removed some of the middle-file and quit.  
So this time, there will be some middle-file missing.  
但是为什么只删除了一部分的中间文件呢？照理来说第二次应该一个文件都不会有  
哦，我明白了。这两个进程一个在读一个在删，所以读的进程只抢到一部分的文件，剩下的文件被删除的进程抢走了，  
  
Worker-0   2023/08/20 06:44:58 Open file middle-file-9-2 error: open middle-file-9-2: no such file or directory  
Worker-0   2023/08/20 06:44:58 Open file middle-file-9-3 error: open middle-file-9-3: no such file or directory  
Worker-0   2023/08/20 06:44:58 Open file middle-file-9-5 error: open middle-file-9-5: no such file or directory  
Worker-0   2023/08/20 06:44:58 Open file middle-file-9-6 error: open middle-file-9-6: no such file or directory  
Worker-0   2023/08/20 06:44:58 job-9 kvs -- [{b 22} {b 21} {b 26} {b 20}]  
Worker-0   2023/08/20 06:44:58 write b 20 21 22 26  
Worker-0   2023/08/20 06:44:58 job-9 done: { Name:[9]  Index:[]  Type:[Reduce]  Assigned:[true] Start:[2023-08-20 06:44:58.605629 +0800 CST]}  
2023/08/20 06:44:58 Job-9 done: { Name:[9]  Index:[]  Type:[Reduce]  Assigned:[true] Start:[2023-08-20 06:44:58.605629 +0800 CST]}  
Worker-0   2023/08/20 06:44:58 Remove middle file err: remove middle-file-9-0: no such file or directory  
Worker-0   2023/08/20 06:44:58 Remove middle file err: remove middle-file-9-1: no such file or directory  
Worker-0   2023/08/20 06:44:58 Remove middle file err: remove middle-file-9-2: no such file or directory  
Worker-0   2023/08/20 06:44:58 Remove middle file err: remove middle-file-9-3: no such file or directory  
Worker-0   2023/08/20 06:44:58 Remove middle file err: remove middle-file-9-4: no such file or directory  
Worker-0   2023/08/20 06:44:58 Remove middle file err: remove middle-file-9-5: no such file or directory  
Worker-0   2023/08/20 06:44:58 Remove middle file err: remove middle-file-9-6: no such file or directory  
Worker-0   2023/08/20 06:44:58 Remove middle file err: remove middle-file-9-7: no such file or directory  
  
  
  
Worker-2   2023/08/20 06:44:58 job-0 kvs -- [{c 138885} {c 453168} {c 441033} {c 540174} {c 594262} {c 139054} {c 581863} {c 412665}]  
Worker-2   2023/08/20 06:44:58 write c 138885 139054 412665 441033 453168 540174 581863 594262  
Worker-2   2023/08/20 06:44:58 job-0 done: { Name:[0]  Index:[]  Type:[Reduce]  Assigned:[true] Start:[2023-08-20 06:44:58.610194 +0800 CST]}  
2023/08/20 06:44:58 Job-0 done: { Name:[0]  Index:[]  Type:[Reduce]  Assigned:[true] Start:[2023-08-20 06:44:58.610194 +0800 CST]}  
Worker-0   2023/08/20 06:44:58 job-2 kvs -- [{a ../pg-being_ernest.txt} {a ../pg-dorian_gray.txt} {a ../pg-frankenstein.txt} {a ../pg-grimm.txt} {a ../pg-huckleberry_finn.txt} {a ../pg-metamorphosis.txt} {a ../pg-sherlock_holmes.txt} {a ../pg-tom_sawyer.txt}]  
Worker-0   2023/08/20 06:45:00 write a ../pg-being_ernest.txt ../pg-dorian_gray.txt ../pg-frankenstein.txt ../pg-grimm.txt ../pg-huckleberry_finn.txt ../pg-metamorphosis.txt ../pg-sherlock_holmes.txt ../pg-tom_sawyer.txt  
Worker-0   2023/08/20 06:45:00 job-2 done: { Name:[2]  Index:[]  Type:[Reduce]  Assigned:[true] Start:[2023-08-20 06:44:58.612308 +0800 CST]}  
2023/08/20 06:45:00 Job-2 done: { Name:[2]  Index:[]  Type:[Reduce]  Assigned:[true] Start:[2023-08-20 06:44:58.612308 +0800 CST]}  
2023/08/20 06:45:01 Check TIMEOUT  
Worker-2   2023/08/20 06:45:01 dialing:dial unix /var/tmp/5840-mr-501: connect: connection refused  
Worker-2   2023/08/20 06:45:01 dialing:dial unix /var/tmp/5840-mr-501: connect: connection refused  
Worker-2   2023/08/20 06:45:01 dialing:dial unix /var/tmp/5840-mr-501: connect: connection refused  
Worker-0   2023/08/20 06:45:02 dialing:dial unix /var/tmp/5840-mr-501: connect: connection refused  
mr-crash-all mr-correct-crash.txt differ: char 187, line 2  
--- crash output is not the same as mr-correct-crash.txt  
--- crash test: FAIL  
*** FAILED SOME TESTS  
exit status 1  
Exiting.
```


## 测试
这次我们通过写go代码来自动化测试，并且加上了单元测试来测试函数。