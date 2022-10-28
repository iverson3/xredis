package tcp

import (
	"context"
	"fmt"
	"log"
	"net"
	"os"
	"os/signal"
	"studygolang/wangdis/interface/tcp"
	"studygolang/wangdis/lib/sync/atomic"
	"sync"
	"syscall"
	"time"
)

type Config struct {
	Address string           `yaml:"address"`
	MaxConnect uint32        `yaml:"max-connect"`
	Timeout    time.Duration `yaml:"timeout"`
}

func ListenAndServe(conf *Config, handler tcp.Handler) {
	listener, err := net.Listen("tcp", conf.Address)
	if err != nil {
		log.Fatal(fmt.Sprintf("listen failed, error: %v", err))
	}

	// 监听中断信号
	var closing atomic.Boolean
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGHUP, syscall.SIGQUIT, syscall.SIGTERM, syscall.SIGINT)
	go func() {
		sig := <-sigCh
		switch sig {
		case syscall.SIGHUP, syscall.SIGQUIT, syscall.SIGTERM, syscall.SIGINT:
			// 收到中断信号后开始关闭流程
			log.Println("shuting down...")
			// 设置标志位为关闭中，使用原子操作保证线程可见性
			closing.Set(true)
			// 先关闭listener 阻止新连接进入
			// listener关闭后 listener.Accept() 会立即返回错误
			_ = listener.Close()
			// 逐个关闭已建立的连接
			_ = handler.Close()
		}
	}()

	log.Printf("bind: %s, start listening...", conf.Address)
	defer func() {
		// 在出现未知错误或panic时保持正常关闭
		// 这里存在一个问题：当应用正常关闭后会再次执行关闭操作
		_ = listener.Close()
		_ = handler.Close()
	}()

	ctx, _ := context.WithCancel(context.Background())

	// waitGroup的计数是当前仍然存活的连接数
	// 进入关闭流程时，主协程应该等待所有连接都关闭后再退出
	var waitDone sync.WaitGroup
	for {
		conn, err := listener.Accept()
		if err != nil {
			if closing.Get() {
				// 收到关闭信号后进入此分支，此时listener已被监听系统信号的协程关闭
				log.Println("waiting disconnect...")
				waitDone.Wait()
				return
			}
			log.Println(fmt.Sprintf("accept failed, error: %v", err))
			continue
		}

		log.Println("accept a new client connection")
		// 创建一个新协程处理新建的连接
		go func() {
			defer func() {
				waitDone.Done()
			}()
			waitDone.Add(1)
			handler.Handle(ctx, conn)
		}()
	}
}

