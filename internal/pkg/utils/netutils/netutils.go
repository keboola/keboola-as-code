package netutils

import (
	"context"
	"net"
	"time"
)

func FreePort() (int, error) {
	addr, err := net.ResolveTCPAddr("tcp", "localhost:0")
	if err != nil {
		return 0, err
	}

	l, err := net.ListenTCP("tcp", addr)
	if err != nil {
		return 0, err
	}

	defer l.Close()
	return l.Addr().(*net.TCPAddr).Port, nil
}

func WaitForTCP(addr string, timeout time.Duration) (err error) {
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()

	var conn net.Conn
	for {
		select {
		case <-ctx.Done():
			if err == nil {
				err = ctx.Err()
			}
			return err
		default:
			conn, err = net.DialTimeout("tcp", addr, time.Second)
			if err == nil {
				_ = conn.Close()
				return nil
			}
		}
	}
}
