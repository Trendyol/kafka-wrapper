package kafka_wrapper_test

import (
	"github.com/Trendyol/kafka-wrapper/internal/testcontainers/kafka"
	"github.com/stretchr/testify/suite"
	"log"
	"testing"
)

type testKafkaSuite struct {
	suite.Suite
	Wrapper       kafka.TestContainerWrapper
	RemoteWrapper kafka.TestContainerWrapper
}

func TestKafka(t *testing.T) {
	suite.Run(t, new(testKafkaSuite))
}

func (s *testKafkaSuite) SetupSuite() {

	log.Println("Starting local container")
	if err := s.Wrapper.RunContainer("9092"); err != nil {
		log.Fatalf("Error occurred while running local container, err: %+v", err)
	}

	log.Println("Starting remote container")
	if err := s.RemoteWrapper.RunContainer("9093"); err != nil {
		log.Fatalf("Error occurred while running remote container, err: %+v", err)
	}

	log.Printf("Local Container is running on address: %s", s.Wrapper.GetBrokerAddress())
	log.Printf("Remote Container is running on address: %s", s.RemoteWrapper.GetBrokerAddress())

}

func (s *testKafkaSuite) TearDownSuite() {
	s.Wrapper.CleanUp()
	s.RemoteWrapper.CleanUp()
}
