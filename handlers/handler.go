package handlers

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"regexp"

	"github.com/aws/aws-sdk-go/aws"
	mq "github.com/remind101/mq-go"
)

type SQSHandler struct {
	QueueURL   string
	Start      bool
	JobConfigs []JobConfig
	Server     *mq.Server
}

type JobConfig struct {
	Name        string      `name`
	Pattern     string      `pattern`
	Image       string      `image`
	ImageConfig interface{} `image_config`
}

// NewSQSHandler creates new SQSHandler instance
func NewSQSHandler(queueURL string) *SQSHandler {
	sqsHandler := new(SQSHandler)
	sqsHandler.QueueURL = queueURL
	//sqsHandler.PatternMap = GetNewImagePatternMap()
	return sqsHandler
}

// StartServer starts a server
func (handler *SQSHandler) StartServer() error {
	// return nil if the server already start
	if handler.Server != nil {
		return nil
	}

	fmt.Println("Start a new server")
	newClient, err := NewSQSClient()
	if err != nil {
		return err
	}

	handler.Server = mq.NewServer(handler.QueueURL, mq.HandlerFunc(func(m *mq.Message) error {
		return handler.HandleSQSMessage(m)
	}), mq.WithClient(newClient))
	handler.Server.Start()

	return nil

}

// ShutdownServer shutdowns a server
func (handler *SQSHandler) ShutdownServer() error {
	fmt.Println("Shutdown the server")
	if handler.Server == nil {
		return nil
	}
	err := handler.Server.Shutdown(context.Background())
	handler.Server = nil
	return err
}

// HandleSQSMessage handles SQS message
//
// It takes a sqs message as input, extract the object urls and
// decide which image need to be pulled to handle the s3 object
// based on the object url
func (handler *SQSHandler) HandleSQSMessage(m *mq.Message) error {
	mapping := make(map[string][]interface{})
	msgBody := aws.StringValue(m.SQSMessage.Body)
	if err := json.Unmarshal([]byte(msgBody), &mapping); err != nil {
		return err
	}
	records := mapping["Records"]
	for _, record := range records {
		recordByte, err := json.Marshal(record)
		if err != nil {
			log.Println(err)
			continue
		}
		bucket, err := GetValueFromJson(recordByte, []string{"s3", "bucket", "name"})
		if err != nil {
			log.Println(err)
			continue
		}
		key, err := GetValueFromJson(recordByte, []string{"s3", "object", "key"})
		if err != nil {
			log.Println(err)
			continue
		}
		bucketName := bucket.(string)
		keyName := key.(string)

		objectPath := "s3://" + bucketName + "/" + keyName

		fmt.Println("Processing: ", objectPath)

		for _, jobConfig := range handler.JobConfigs {
			re := regexp.MustCompile(jobConfig.Pattern)
			if re.MatchString(objectPath) {
				result, err := createK8sJob(objectPath, jobConfig)
				if err != nil {
					log.Println(err)
					return err
				}
				out, err := json.Marshal(result)
				if err != nil {
					log.Println(err)
					return err
				}
				log.Println(string(out))
			}

		}
	}
	return nil
}

func (handler *SQSHandler) addNewJobConfig(jsonBytes []byte) error {
	jobConfig := JobConfig{}
	if err := json.Unmarshal(jsonBytes, &jobConfig); err != nil {
		return err
	}
	if jobConfig.Name != "" && jobConfig.Image != "" {
		handler.JobConfigs = append(handler.JobConfigs, jobConfig)
	} else {

		return errors.New("Name and image args are required\n;")
	}
	return nil
}

func (handler *SQSHandler) deleteJobConfig(pattern string) error {
	for idx, job := range handler.JobConfigs {
		if job.Pattern == pattern {
			handler.JobConfigs = append(handler.JobConfigs[:idx], handler.JobConfigs[idx+1:]...)
			return nil
		}
	}
	return fmt.Errorf("There is no job with provided pattern\n %s", pattern)
}

func (handler *SQSHandler) listAllJobConfigs() (string, error) {
	str := ""
	for _, job := range handler.JobConfigs {
		jsonBytes, err := json.Marshal(job)
		if err != nil {
			return "", err
		}
		str = str + string(jsonBytes) + ","
	}
	return "[" + str + "]", nil
}
