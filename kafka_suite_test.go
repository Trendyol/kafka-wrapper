package kafka_wrapper_test

import (
	"fmt"
	"github.com/Trendyol/kafka-wrapper/internal/testcontainers/kafka"
	"github.com/stretchr/testify/suite"
	"testing"
)

type testKafkaSuite struct {
	suite.Suite
	Wrapper kafka.TestContainerWrapper
}

func TestKafka(t *testing.T) {
	suite.Run(t, new(testKafkaSuite))
}

func (s *testKafkaSuite) SetupSuite() {
	if err := s.Wrapper.RunContainer(); err != nil {
		fmt.Printf("Error occurred while running container, err: %+v", err)
		return
	}

	fmt.Printf("Container is running on address: %s \n", s.Wrapper.GetBrokerAddress())
}

func (s *testKafkaSuite) TearDownSuite() {
	s.Wrapper.CleanUp()
}
