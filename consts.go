package kafka_wrapper

import (
	"io/ioutil"
	"log"
)

var Logger = log.New(ioutil.Discard, "[kafka-wrapper] ", log.LstdFlags)
