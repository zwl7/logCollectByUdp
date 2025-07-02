package main

import (
	"fmt"
	"log"
	"net"
	"sync"
	"time"
)

// UDPClient UDP客户端
type UDPClient struct {
	conn net.Conn
	addr string
}

// NewUDPClient 创建新的UDP客户端
func NewUDPClient(addr string) (*UDPClient, error) {
	conn, err := net.Dial("udp", addr)
	if err != nil {
		return nil, err
	}

	return &UDPClient{
		conn: conn,
		addr: addr,
	}, nil
}

// SendMessage 发送消息到UDP服务器
func (c *UDPClient) SendMessage(message string) error {
	_, err := c.conn.Write([]byte(message))
	return err
}

// Close 关闭连接
func (c *UDPClient) Close() error {
	return c.conn.Close()
}

// TestUDPServer 测试UDP服务器性能
func TestUDPServer() {
	// 创建UDP客户端
	client, err := NewUDPClient("localhost:8080")
	if err != nil {
		log.Fatalf("创建UDP客户端失败: %v", err)
	}
	defer client.Close()

	fmt.Println("UDP客户端已连接到服务器")

	// 性能测试参数
	const (
		messageCount = 10000 // 总消息数
		concurrency  = 100   // 并发数
		messageSize  = 100   // 消息大小
	)

	// 生成测试消息
	testMessage := generateMessage(messageSize)

	// 并发发送消息
	var wg sync.WaitGroup
	startTime := time.Now()

	for i := 0; i < concurrency; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()

			messagesPerGoroutine := messageCount / concurrency
			for j := 0; j < messagesPerGoroutine; j++ {
				message := fmt.Sprintf("[%d-%d] %s", id, j, testMessage)
				err := client.SendMessage(message)
				if err != nil {
					log.Printf("发送消息失败: %v", err)
					continue
				}

				// 控制发送速率，避免过载
				time.Sleep(time.Microsecond * 100)
			}
		}(i)
	}

	wg.Wait()
	duration := time.Since(startTime)

	// 输出性能统计
	fmt.Printf("\n性能测试结果:\n")
	fmt.Printf("总消息数: %d\n", messageCount)
	fmt.Printf("并发数: %d\n", concurrency)
	fmt.Printf("消息大小: %d bytes\n", messageSize)
	fmt.Printf("总耗时: %v\n", duration)
	fmt.Printf("平均吞吐量: %.2f 消息/秒\n", float64(messageCount)/duration.Seconds())
	fmt.Printf("平均延迟: %v\n", duration/time.Duration(messageCount))
}

// generateMessage 生成指定长度的测试消息
func generateMessage(size int) string {
	if size <= 0 {
		return "test"
	}

	// 生成重复的测试字符串
	base := "abcdefghijklmnopqrstuvwxyz0123456789"
	result := ""
	for len(result) < size {
		result += base
	}
	return result[:size]
}

// BenchmarkUDPServer 基准测试
func BenchmarkUDPServer() {
	client, err := NewUDPClient("localhost:8080")
	if err != nil {
		log.Fatalf("创建UDP客户端失败: %v", err)
	}
	defer client.Close()

	fmt.Println("开始基准测试...")

	// 预热
	for i := 0; i < 1000; i++ {
		client.SendMessage(fmt.Sprintf("warmup-%d", i))
	}
	time.Sleep(time.Second)

	// 基准测试
	const testDuration = 30 * time.Second
	startTime := time.Now()
	messageCount := 0

	for time.Since(startTime) < testDuration {
		message := fmt.Sprintf("benchmark-%d", messageCount)
		err := client.SendMessage(message)
		if err != nil {
			log.Printf("发送消息失败: %v", err)
			continue
		}
		messageCount++

		// 控制发送速率
		time.Sleep(time.Microsecond * 50)
	}

	duration := time.Since(startTime)
	fmt.Printf("\n基准测试结果:\n")
	fmt.Printf("测试时长: %v\n", duration)
	fmt.Printf("发送消息数: %d\n", messageCount)
	fmt.Printf("平均吞吐量: %.2f 消息/秒\n", float64(messageCount)/duration.Seconds())
}

// SimpleTest 简单测试
func SimpleTest() {
	client, err := NewUDPClient("localhost:8080")
	if err != nil {
		log.Fatalf("创建UDP客户端失败: %v", err)
	}
	defer client.Close()

	fmt.Println("UDP客户端已连接到服务器")

	// 发送简单测试消息
	testMessages := []string{
		"Hello UDP Server!",
		"这是一条测试日志",
		"Log message from client",
		"测试数据 123",
	}

	for i, message := range testMessages {
		fmt.Printf("发送消息 %d: %s\n", i+1, message)

		err := client.SendMessage(message)
		if err != nil {
			log.Printf("发送消息失败: %v", err)
			continue
		}

		// 等待一秒再发送下一条消息
		time.Sleep(1 * time.Second)
	}

	fmt.Println("简单测试完成")
}

func test() {
	fmt.Println("UDP客户端测试工具")
	fmt.Println("1. 简单测试")
	fmt.Println("2. 性能测试")
	fmt.Println("3. 基准测试")

	var choice int
	fmt.Print("请选择测试类型 (1, 2 或 3): ")
	fmt.Scanf("%d", &choice)

	switch choice {
	case 1:
		SimpleTest()
	case 2:
		TestUDPServer()
	case 3:
		BenchmarkUDPServer()
	default:
		fmt.Println("无效选择，执行简单测试")
		SimpleTest()
	}
}
