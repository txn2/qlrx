package main

import (
	"flag"
	"fmt"
	"io"
	"net"
	"os"
	"strconv"
	"time"

	"github.com/txn2/micro"
	"go.uber.org/zap"
)

var (
	tcpIpEnv          = getEnv("TCP_IP", "127.0.0.1")
	tcpPortEnv        = getEnv("TCP_PORT", "3000")
	tcpReadTimeoutEnv = getEnv("TCP_READ_TIMEOUT", "10")
	tcpBufferSizeEnv  = getEnv("TCP_BUFFER_SIZE", "1600")
)

func main() {
	tcpReadTimeoutEnvInt, err := strconv.Atoi(tcpReadTimeoutEnv)
	if err != nil {
		fmt.Printf("TCP listener read timeout must be integer, parse error: " + err.Error())
	}

	tcpBufferSizeEnvInt, err := strconv.Atoi(tcpBufferSizeEnv)
	if err != nil {
		fmt.Printf("TCP buffer size must be an integer, parse error: " + err.Error())
	}

	tcpIp := flag.String("tcpIp", tcpIpEnv, "TCP listener IP address.")
	tcpPort := flag.String("tcpPort", tcpPortEnv, "TCP listener port.")
	tcpReadTimeout := flag.Int("tcpReadTimeout", tcpReadTimeoutEnvInt, "TCP listener read timeout.")
	tcpBuffer := flag.Int("tcpBufferSize", tcpBufferSizeEnvInt, "TCP buffer size in bytes.")

	serverCfg, _ := micro.NewServerCfg("qlrx")
	server := micro.NewServer(serverCfg)

	// background web server
	go func() {
		server.Run()
	}()

	// Handle TCP connection
	tcpHandler := func(c net.Conn) {
		defer c.Close()

		server.Logger.Info("Serving", zap.String("remote", c.RemoteAddr().String()))
		buf := make([]byte, *tcpBuffer)

		bufLen, err := c.Read(buf)
		if err != nil {
			if err != io.EOF {
				server.Logger.Error("TCP Read Error", zap.Error(err))
			}
			return
		}

		msg := buf[:bufLen]
		server.Logger.Info("Message received", zap.ByteString("message", msg))
	}

	// Listen for TCP connections
	l, err := net.Listen("tcp4", fmt.Sprintf("%s:%s", *tcpIp, *tcpPort))
	if err != nil {
		fmt.Println(err)
		return
	}

	server.Logger.Info("TCP server started",
		zap.String("ip", *tcpIp),
		zap.String("port", *tcpPort),
		zap.Int("buffer_size", *tcpBuffer),
	)

	// for each connection
	for {
		c, err := l.Accept()
		if err != nil {
			server.Logger.Error("TCP Accept Error", zap.Error(err))
			return
		}

		err = c.SetDeadline(time.Now().Add(time.Duration(*tcpReadTimeout) * time.Second))
		if err != nil {
			server.Logger.Error("TCP SetDeadline Error", zap.Error(err))
			return
		}

		go tcpHandler(c)
	}

}

// getEnv gets an environment variable or sets a default if
// one does not exist.
func getEnv(key, fallback string) string {
	value := os.Getenv(key)
	if len(value) == 0 {
		return fallback
	}

	return value
}
