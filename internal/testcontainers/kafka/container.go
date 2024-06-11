package kafka

import (
	"context"
	"fmt"
	"github.com/docker/go-connections/nat"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"
	"log"
	"runtime"
	"time"
)

const (
	RedpandaImage   = "redpandadata/redpanda"
	RedpandaVersion = "v24.1.3"
)

type TestContainerWrapper struct {
	container testcontainers.Container
	hostPort  int
}

func (t *TestContainerWrapper) RunContainer(portInfo string) error {
	req := testcontainers.ContainerRequest{
		Image: fmt.Sprintf("%s:%s", RedpandaImage, RedpandaVersion),
		ExposedPorts: []string{
			portInfo + ":" + portInfo + "/tcp",
		},
		Cmd: []string{
			"redpanda",
			"start",
			"--overprovisioned --smp 1 --memory 1G --reserve-memory 0M --check=false",
		},
		WaitingFor: wait.ForLog("Successfully started Redpanda!"),
		AutoRemove: true,
	}

	container, err := testcontainers.GenericContainer(context.Background(), testcontainers.GenericContainerRequest{
		ContainerRequest: req,
		Started:          true,
		Reuse:            false,
	})
	if err != nil {
		log.Fatalf("could not create container: %v", err)
	}

	// Container'ın loglarını almak için
	logs, err := container.Logs(context.Background())
	if err != nil {
		log.Fatalf("could not get container logs: %v", err)
	}

	// Logları yazdır
	log.Println("Container logs:")
	_, err = fmt.Println(logs)
	if err != nil {
		log.Fatalf("could not write logs: %v", err)
	}

	mPort, err := container.MappedPort(context.Background(), nat.Port(portInfo))
	if err != nil {
		return fmt.Errorf("could not get mapped port from the container: %w", err)
	}

	t.container = container
	t.hostPort = mPort.Int()

	log.Printf("Container %s is running on port: %d", "redpanda", t.hostPort)
	return nil
}

func (t *TestContainerWrapper) CleanUp() {
	ctx, cancelFunc := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancelFunc()

	if err := t.container.Terminate(ctx); err != nil {
		log.Printf("could not terminate container: %s\n", err)
	}
}

func (t *TestContainerWrapper) GetBrokerAddress() string {
	ip := "localhost"
	if isRunningOnOSX() {
		ip = "127.0.0.1"
	}

	return fmt.Sprintf("%s:%d", ip, t.hostPort)
}

func isRunningOnOSX() bool {
	return runtime.GOOS == "darwin"
}
