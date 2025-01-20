package server

import (
	rand "math/rand/v2"
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
	t.Helper()
	source := rand.NewPCG(42, 1024)
	random := rand.New(source) // nolint:gosec
	p := path.Join(tempDir, subFolder)
	// Remove test folder so the used ports can be saved
	err = os.RemoveAll(tempDir) // nolint:forbidigo
	if err != nil {
		panic(err)
	}

	err = os.MkdirAll(p, 0o755)
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
	for i := range numberOfPorts {
		port := p.random.IntN(65535-1024+1) + 1024
		for IsPortOccupied(port) && slices.Contains(duplicates, port) {
			port = p.random.IntN(65535-1024+1) + 1024
		}

		duplicates = append(duplicates, port)
		p.ports[i] = port
	}
}

func (p portManager) GetFreePort() int {
	p.mu.Lock()
	defer p.mu.Unlock()
	randomPort := p.random.IntN(len(p.ports))
	port := p.ports[randomPort]
	dir := path.Join(p.dir, strconv.FormatInt(int64(port), 10))
	for _, err := os.Open(dir); err == nil; { // nolint:forbidigo
		randomPort = p.random.IntN(len(p.ports))
		port = p.ports[randomPort]
		dir = path.Join(p.dir, strconv.FormatInt(int64(port), 10))
		_, err = os.Open(dir) // nolint:forbidigo
	}

	delete(p.ports, randomPort)
	err := os.Mkdir(dir, 0o644) // nolint:forbidigo
	if err != nil {
		panic("unable to create random port. All ports exhausted" + err.Error())
	}

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
