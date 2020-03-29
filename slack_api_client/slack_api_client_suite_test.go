package slack_api_client_test

import (
	"github.com/Trendyol/kafka-wrapper/internal/integrationcontainers/mountebank"
	"github.com/Trendyol/kafka-wrapper/pkg/rest"
	"github.com/Trendyol/kafka-wrapper/slack_api_client"
	"github.com/durmaze/gobank"
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
	"testing"
)

func TestOmsApiClient(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "Slack Api Client Suite Test")
}

var (
	userName            = "userName"
	slackChannel        = "slackChannel"
	mountebankContainer *mountebank.Container
	slackClient         slack_api_client.Client
	Mountebank          *gobank.Client
)

var _ = BeforeSuite(func() {
	mountebankContainer = mountebank.NewContainer("andyrbell/mountebank:2.1.0")
	mountebankContainer.SetMockMode(true)
	mountebankContainer.Run()

	Mountebank = gobank.NewClient(mountebankContainer.AdminUri())
	slackClient = slack_api_client.NewClient(mountebankContainer.Uri(), userName, slackChannel, rest.NewClient())
})

var _ = AfterSuite(func() {
	mountebankContainer.ForceRemoveAndPrune()
})
