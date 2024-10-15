# Mit_6.5840
Mit_6.5840(6.824) lab1-lab5

课程地址：https://pdos.csail.mit.edu/6.824/schedule.html

在此记录一下对Mit-6.5840的lab2学习

本次实验的主要任务是在考虑RPC消息丢失，重复请求和内存释放的情况下，完成一些基础对map的基础操作。

实验内容：实验内容分为三个部分commen.go,client.go,server.go
1、server.go中实现了请求的具体操作，处理重复请求，处理内存释放（根据实验提示，每次释放上一个requestId的内存）。主要是维护一个map（map[int64]PutAppendReply）记录请求id和其对应的value的关系，用这个map同时实现判重和记录正确返回值的任务

实验问题记录：

--- FAIL: TestMemGet2 (0.47s)
test_test.go:404: error: server using too much memory 10
单次get请求内存一直溢出 

遇到这个第一反应就是去看了下内存使用情况，在test.go文件中引入了pprof去查看内存情况：
go tool pprof http://localhost:6060/debug/pprof/heap

在 test.go中添加	  
go func() {   
log.Println("Starting pprof server on http://localhost:6060")
if err := http.ListenAndServe("localhost:6060", nil); err != nil {
log.Fatalf("pprof server failed to start: %v", err)}}()

得到：
flat  flat%   sum%        cum   cum%   
10MB 90.45% 90.45%       10MB 90.45%  encoding/gob.decString   
1.06MB  9.55%   100%     1.06MB  9.55%  time.newTimer  
0     0%   100%       10MB 90.45%  6.5840/labgob.(*LabDecoder).Decode
0     0%   100%     1.06MB  9.55%  6.5840/labrpc.(*Network).processReq
0     0%   100%       10MB 90.45%  6.5840/labrpc.(*Network).processReq.func1
0     0%   100%       10MB 90.45%  6.5840/labrpc.(*Server).dispatch
0     0%   100%       10MB 90.45%  6.5840/labrpc.(*Service).dispatch
0     0%   100%       10MB 90.45%  encoding/gob.(*Decoder).Decode
0     0%   100%       10MB 90.45%  encoding/gob.(*Decoder).DecodeValue
0     0%   100%       10MB 90.45%  encoding/gob.(*Decoder).decodeStruct

可以看到gob.Decoder 的 decString 方法占用了 10240kB，约占总内存使用的 95.24%，这是一个显著的内存占用点。

gob.Decoder 的 decString 方法与字段的数量和类型有关，发现Get方法RPC参数是有冗余的，就算重复调用get方法也不需要Id标识去判重和存储信息，去掉Get相关结构中的Id变量后便能通过测试。
