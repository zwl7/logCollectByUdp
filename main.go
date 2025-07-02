package main

import (
	"encoding/json"
	"fmt"
	"log"
	"net"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"time"
)

// WorkerPool 简单固定工作池
type WorkerPool struct {
	workers   int
	taskQueue chan func()
	wg        sync.WaitGroup
}

// LogData:{"date":1751372240.0,"remote_addr":"172.28.0.1","remote_user":"-","request":"GET /hyperfApi/demo/test HTTP/1.1","status":"200","body_bytes_sent":"154","http_referer":"-","http_user_agent":"PostmanRuntime-ApipostRuntime/1.1.0","request_time":"0.071","upstream_response_time":"0.071","request_length":"201","bytes_sent":"553","http_x_forwarded_for":"-","http_x_real_ip":"-","scheme":"http","http_host":"localhost","server_name":"localhost"}
type LogData struct {
	Date                 float64 `json:"date"`
	RemoteAddr           string  `json:"remote_addr"`
	RemoteUser           string  `json:"remote_user"`
	Request              string  `json:"request"`
	Status               string  `json:"status"`
	BodyBytesSent        string  `json:"body_bytes_sent"`
	HttpReferer          string  `json:"http_referer"`
	HttpUserAgent        string  `json:"http_user_agent"`
	RequestTime          string  `json:"request_time"`
	UpstreamResponseTime string  `json:"upstream_response_time"`
	RequestLength        string  `json:"request_length"`
	BytesSent            string  `json:"bytes_sent"`
	HttpXForwardedFor    string  `json:"http_x_forwarded_for"`
	HttpXRealIp          string  `json:"http_x_real_ip"`
	Scheme               string  `json:"scheme"`
	HttpHost             string  `json:"http_host"`
	ServerName           string  `json:"server_name"`
}

// -- `default`.nginx_access_logs definition
type NginxAccessLog struct {
	TimeIso8601          string  `json:"time_iso8601"`
	RemoteAddr           string  `json:"remote_addr"`
	RemoteUser           string  `json:"remote_user"`
	Method               string  `json:"method"`
	RequestUri           string  `json:"request_uri"`
	HttpVersion          string  `json:"http_version"`
	Status               int     `json:"status"`
	BodyBytesSent        int     `json:"body_bytes_sent"`
	HttpReferer          string  `json:"http_referer"`
	HttpUserAgent        string  `json:"http_user_agent"`
	RequestTime          float64 `json:"request_time"`
	UpstreamResponseTime float64 `json:"upstream_response_time"`
	RequestLength        int     `json:"request_length"`
	BytesSent            int     `json:"bytes_sent"`
	HttpXForwardedFor    string  `json:"http_x_forwarded_for"`
	HttpXRealIp          string  `json:"http_x_real_ip"`
	Scheme               string  `json:"scheme"`
	HttpHost             string  `json:"http_host"`
	ServerName           string  `json:"server_name"`
}

// NewWorkerPool 创建新的工作池
func NewWorkerPool(workers int) *WorkerPool {

	wp := &WorkerPool{
		workers:   workers,
		taskQueue: make(chan func(), workers*100),
	}

	// 启动工作goroutine
	for i := 0; i < workers; i++ {
		wp.wg.Add(1)
		go wp.worker(i)
	}

	return wp
}

// worker 工作goroutine
func (wp *WorkerPool) worker(id int) {
	defer wp.wg.Done()

	for task := range wp.taskQueue {
		// 执行任务，捕获panic
		func() {
			defer func() {
				if r := recover(); r != nil {
					log.Printf("工作goroutine %d panic: %v", id, r)
				}
			}()
			task()
		}()

	}
}

// Submit 提交任务到工作池
func (wp *WorkerPool) Submit(task func()) bool {
	if task == nil {
		return false
	}

	select {
	case wp.taskQueue <- task:
		return true
	default:
		return false // 队列已满
	}
}

// Close 关闭工作池
func (wp *WorkerPool) Close() {
	close(wp.taskQueue)
	wp.wg.Wait()
}

// 全局工作池
var workerPool *WorkerPool

func main() {
	// 创建工作池
	workerCount := runtime.NumCPU() * 50
	workerPool = NewWorkerPool(workerCount)
	defer workerPool.Close()

	fmt.Printf("UDP日志收集服务器启动\n")
	fmt.Printf("工作goroutine数量: %d\n", workerCount)
	fmt.Println("等待UDP连接...")

	//initClickhouse()
	// 启动UDP服务器
	startUDPServer()
}

// startUDPServer 启动UDP服务器
func startUDPServer() {
	// 设置UDP服务器地址和端口
	addr := ":9001"

	// 解析UDP地址
	udpAddr, err := net.ResolveUDPAddr("udp", addr)
	if err != nil {
		log.Fatalf("解析UDP地址失败: %v", err)
	}

	// 创建UDP连接
	conn, err := net.ListenUDP("udp", udpAddr)
	if err != nil {
		log.Fatalf("启动UDP服务器失败: %v", err)
	}
	defer conn.Close()

	fmt.Printf("UDP服务器已启动，监听端口: %s\n", addr)

	// 创建缓冲区来接收数据
	buffer := make([]byte, 1024*60) // 60KB缓冲区

	// 持续监听UDP数据
	for {
		// 读取UDP数据
		n, remoteAddr, err := conn.ReadFromUDP(buffer)
		if err != nil {
			log.Printf("读取UDP数据失败: %v", err)
			continue
		}

		// 获取接收到的数据
		data := string(buffer[:n])

		// 提交任务到工作池
		if !workerPool.Submit(func() {
			processData(data, remoteAddr)
		}) {
			// 工作池满了，记录日志但不阻塞
			log.Printf("工作池已满，丢弃来自 %s 的数据", remoteAddr.String())
		}
	}
}

// processData 处理接收到的数据
func processData(data string, remoteAddr *net.UDPAddr) {
	// 数据验证
	if len(data) == 0 {
		return
	}

	// 去除空白字符
	trimmedData := strings.TrimSpace(data)
	if len(trimmedData) == 0 {
		return
	}

	println("+++++++++++++")
	fmt.Println(trimmedData)
	println("+++++++++++++")

	// // 记录处理信息
	//logStr := remoteAddr.String()
	//log.Printf("处理来自 %s 的数据: %s", remoteAddr.String(), trimmedData)

	// TODO: 在这里添加实际的业务逻辑
	// 例如：解析日志数据、写入数据库等

	// 模拟处理时间
	// 把字符串写入到数据库中（mysql，clickhosue ，mongodb,elasticSearch 等） 连接到clickhouse 写入到clickhouse中

	//
	//{"date":1751372674.0,"remote_addr":"172.28.0.1","remote_user":"-","request":"GET /hyperfApi/demo/test HTTP/1.1","status":"200","body_bytes_sent":"154","http_referer":"-","http_user_agent":"PostmanRuntime-ApipostRuntime/1.1.0","request_time":"0.067","upstream_response_time":"0.067","request_length":"201","bytes_sent":"553","http_x_forwarded_for":"-","http_x_real_ip":"-","scheme":"http","http_host":"localhost","server_name":"localhost"}
	logData := LogData{}
	json.Unmarshal([]byte(trimmedData), &logData)

	t := time.Unix(int64(logData.Date), 0)
	Status, _ := strconv.Atoi(logData.Status)	
	BodyBytesSent, _ := strconv.Atoi(logData.BodyBytesSent)
	RequestTime, _ := strconv.ParseFloat(logData.RequestTime, 64)
	UpstreamResponseTime, _ := strconv.ParseFloat(logData.UpstreamResponseTime, 64)
	RequestLength, _ := strconv.Atoi(logData.RequestLength)
	BytesSent, _ := strconv.Atoi(logData.BytesSent)
	
	url := strings.Split(logData.Request, " ")
	nginxAccessLog := NginxAccessLog{
		TimeIso8601: t.Format("2006-01-02 15:04:05"),
		RemoteAddr:  logData.RemoteAddr,
		RemoteUser:  logData.RemoteUser,

		Method:               url[0],
		RequestUri:           url[1],
		HttpVersion:          url[2],
		Status:               Status,
		BodyBytesSent:        BodyBytesSent,
		HttpReferer:          logData.HttpReferer,
		HttpUserAgent:        logData.HttpUserAgent,
		RequestTime:          RequestTime,
		UpstreamResponseTime: UpstreamResponseTime,
		RequestLength:        RequestLength,
		BytesSent:            BytesSent,
		HttpXForwardedFor:    logData.HttpXForwardedFor,
		HttpXRealIp:          logData.HttpXRealIp,
		Scheme:               logData.Scheme,
		HttpHost:             logData.HttpHost,
		ServerName:           logData.ServerName,
	}
	println("+++++++++++++")
	fmt.Println(nginxAccessLog)
	println("+++++++++++++")

	//
	// err := DB.Create(&nginxAccessLog).Error
	// if err != nil {
	// 	fmt.Println(err)
	// 	return
	// }

	println("+++++++++++++")
	fmt.Println("插入clickhouse成功")
	println("+++++++++++++")

}
