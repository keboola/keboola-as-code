package main

import (
	"context"
	"errors"
	"fmt"
	"net"
	"net/http"
	"time"
)

func main() {
	target := "app-4452857.sandbox.svc.cluster.local:8888"

	fmt.Println("=== DNS lookup ===")
	ips, err := net.LookupIP("app-4452857.sandbox.svc.cluster.local")
	if err != nil {
		panic(err)
	}
	for _, ip := range ips {
		fmt.Println(" ", ip)
	}

	fmt.Println("\n=== HTTP dial ===")
	dialer := &net.Dialer{
		Timeout: 2 * time.Second,
	}

	transport := &http.Transport{
		DialContext: func(ctx context.Context, network, addr string) (net.Conn, error) {
			start := time.Now()
			fmt.Printf("Dialing %s %s ...\n", network, addr)
			conn, err := dialer.DialContext(ctx, network, addr)
			fmt.Printf("Dial result after %v\n", time.Since(start))
			return conn, err
		},
	}

	client := &http.Client{
		Transport: transport,
	}

	req, _ := http.NewRequest("GET", "http://"+target, nil)

	start := time.Now()
	resp, err := client.Do(req)
	elapsed := time.Since(start)

	fmt.Printf("\n=== Result (after %v) ===\n", elapsed)

	if err != nil {
		fmt.Println("Error:", err)

		var ne net.Error
		if errors.As(err, &ne) {
			fmt.Println("net.Error timeout:", ne.Timeout())
			fmt.Println("net.Error temporary:", ne.Temporary())
		}

		if errors.Is(err, context.DeadlineExceeded) {
			fmt.Println("context deadline exceeded")
		}

		return
	}

	fmt.Println("Status:", resp.Status)
	resp.Body.Close()
}
