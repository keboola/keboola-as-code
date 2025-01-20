package server

import (
	"math/rand"
	"net"
	"os"
	"path"
	"slices"
	"strconv"
	"sync"
	"testing"
	"time"
)

const (
	numberOfPorts = 2000
)

type PortManager interface {
	GeneratePorts()
	GetFreePort() int
}

type portManager struct {
	random *rand.Rand
	ports  map[int]int
	mu     *sync.Mutex
	dir    string
}

func NewPortManager(t *testing.T, tempDir, subFolder string) (pm *portManager, err error) {
	source := rand.NewSource(time.Now().UnixNano())
	random := rand.New(source)
	p := path.Join(tempDir, subFolder)
	// Remove test folder so the used ports can be saved
	os.RemoveAll(p)
	err = os.MkdirAll(p, 0644)
	if err != nil {
		panic(err)
	}

	pm = &portManager{
		random: random,
		ports:  make(map[int]int, numberOfPorts),
		mu:     &sync.Mutex{},
		dir:    p,
	}
	pm.GeneratePorts()
	return pm, err
}

func (p portManager) GeneratePorts() {
	// Generate ports (1024-65535)
	duplicates := make([]int, 0, numberOfPorts)
	for i := 0; i < numberOfPorts; i++ {
		port := p.random.Intn(65535-1024+1) + 1024
		for IsPortOccupied(port) && slices.Contains(duplicates, port) {
			port = p.random.Intn(65535-1024+1) + 1024
		}

		duplicates = append(duplicates, port)
		p.ports[i] = port
	}
}

func (p portManager) GetFreePort() int {
	source := rand.NewSource(time.Now().UnixNano())
	random := rand.New(source)
	p.mu.Lock()
	defer p.mu.Unlock()
	randomPort := random.Intn(len(p.ports))
	port := p.ports[randomPort]
	dir := path.Join(p.dir, strconv.FormatInt(int64(port), 10))
	for _, err := os.Open(dir); err == nil; {
		randomPort = random.Intn(len(p.ports))
		port = p.ports[randomPort]
		dir = path.Join(p.dir, strconv.FormatInt(int64(port), 10))
		_, err = os.Open(dir)
	}

	delete(p.ports, randomPort)
	os.Mkdir(dir, 0644)
	return port
}

func IsPortOccupied(port int) bool {
	conn, err := net.DialTimeout("tcp", net.JoinHostPort("127.0.0.1", strconv.FormatInt(int64(port), 10)), time.Second)
	if err != nil {
		return false
	}

	conn.Close()
	return true
}
