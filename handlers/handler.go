package handlers

import (
	"encoding/json"
	"errors"
	"fmt"
	"regexp"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/sqs"
	"github.com/aws/aws-sdk-go/service/sqs/sqsiface"
	"github.com/golang/glog"
)

const (
	MAX_RETRIES int = 3
)

type SQSHandler struct {
	sqsClient  sqsiface.SQSAPI
	QueueURL   string
	Start      bool
	JobConfigs []JobConfig
}

type JobConfig struct {
	Name           string      `name`
	Pattern        string      `pattern`
	Image          string      `image`
	ImageConfig    interface{} `image_config`
	RequestCPU     string      `request_cpu`
	RequestMem     string      `request_mem`
	DeadLine       int64       `deadline`
	ServiceAccount string      `serviceaccount`
}

type RetryMessage struct {
	Bucket string `bucket`
	Key    string `key`
}

// NewSQSHandler creates new SQSHandler instance
func NewSQSHandler(queueURL string) *SQSHandler {
	sqsHandler := new(SQSHandler)
	sqsClient, err := NewSQSClient()
	if err != nil {
		panic(err)
	}
	sqsHandler.sqsClient = sqsClient
	sqsHandler.QueueURL = queueURL
	//sqsHandler.PatternMap = GetNewImagePatternMap()
	return sqsHandler
}

// StartServer starts a server
func (handler *SQSHandler) StartServer() error {

	glog.Info("Starting a new server ...")

	go handler.StartConsumingProcess()
	go handler.StartMonitoringProcess()

	glog.Info("The server is started")

	return nil

}

// StartConsumingProcess starts consumming the queue
func (handler *SQSHandler) StartConsumingProcess() error {
	receiveParams := &sqs.ReceiveMessageInput{
		QueueUrl:            aws.String(handler.QueueURL),
		MaxNumberOfMessages: aws.Int64(1),
		VisibilityTimeout:   aws.Int64(30),
		WaitTimeSeconds:     aws.Int64(20),
	}
	for {
		time.Sleep(1 * time.Second)
		receiveResp, err := handler.sqsClient.ReceiveMessage(receiveParams)
		if err != nil {
			glog.Error(err)
		}

		for _, message := range receiveResp.Messages {
			err := handler.HandleSQSMessage(message)
			if err != nil {
				glog.Errorf("Can not process the message. Error %s. Message %s", err, *message.Body)
				continue
			}

			// Immediately remove the message from the queue.
			handler.RemoveSQSMessage(message)
		}
	}
}

// RemoveSQSMessage removes SQS message
func (handler *SQSHandler) RemoveSQSMessage(message *sqs.Message) error {
	deleteParams := &sqs.DeleteMessageInput{
		QueueUrl:      aws.String(handler.QueueURL), // Required
		ReceiptHandle: message.ReceiptHandle,        // Required
	}
	_, err := handler.sqsClient.DeleteMessage(deleteParams) // No response returned when successed.
	if err != nil {
		glog.Error(err)
		return err
	}
	glog.Infof("Message ID: %s has beed deleted.\n\n", *message.MessageId)
	return nil
}

// ResendSQSMessage resends the message
func (handler *SQSHandler) ResendSQSMessage(queueURL string, message *sqs.Message) error {
	sentMessageInput := &sqs.SendMessageInput{
		MessageBody: message.Body,
		QueueUrl:    aws.String(queueURL),
	}
	_, err := handler.sqsClient.SendMessage(sentMessageInput)
	return err

}

// StartMonitoringProcess starts the process to monitor the created job
func (handler *SQSHandler) StartMonitoringProcess() {
	retries := make(map[string]int)

	for {
		for _, job := range listJobs() {
			k8sJob, err := GetJobStatusByID(job.UID)
			if err != nil {
				glog.Errorf("error getting job status %s", err)
				continue
			}

			glog.Infof("job status for %s: %s", k8sJob.Name, k8sJob.Status)

			switch k8sJob.Status {
			case "Completed":
				deleteJobByID(job.UID, GRACE_PERIOD)
			case "Failed":
				retries[job.UID] += 1
				retryCount := retries[job.UID]
				if retryCount < MAX_RETRIES {
					glog.Errorf("The k8s job %s failed (%d/%d retries). Resend the message to the queue", job.Name, retryCount, MAX_RETRIES)
					handler.ResendSQSMessage(handler.QueueURL, job.SQSMessage)
				} else {
					glog.Errorf("The k8s job %s exceeded retry request limits. Discarding.", job.Name)
				}
				deleteJobByID(job.UID, GRACE_PERIOD)
			}
		}

		time.Sleep(30 * time.Second)
	}
}

/*
getObjectFromSQSMessage returns s3 object from sqs message

The format of a SQS message body:
{
	"Type" : "Notification",
  	"MessageId" : "f0207b9c-7255-5f61-998a-2f0e64c6eef0",
	"TopicArn" : "arn:aws:sns:us-east-1:707,
	"Subject" : "Amazon S3 Notification",
	"Message":  {"Records":[
		{"eventVersion":"2.0","eventSource":"aws:s3","awsRegion":"us-east-1",
		"eventTime":"2018-11-19T00:57:57.693Z","eventName":"ObjectCreated:Put",
		"userIdentity":{"principalId":"AWS:AIDAIU3LRUEK5OHS6FXRQ"},
		"requestParameters":{"sourceIPAddress":"173.24.34.163"},
		"responseElements":{"x-amz-request-id":"91CF670A054E0332",
		"x-amz-id-2":"h0ZQgg6w2qzKUkzivRizP1E1Jf9QAXSu1bUllWaF2b7j/63XRgjGLMNyI7sl016QKSaxK1L2RrI="},
		"s3":{"s3SchemaVersion":"1.0","configurationId":"Giang Bui",
		"bucket":{"name":"xssxs","ownerIdentity":{"principalId":"A365FU9MXCCF0S"},
		"arn":"arn:aws:s3:::xssxs"},"object":{"key":"api.py","size":8005,"eTag":"b4ef93035ff791f7d507a47342c89cd6",
		"sequencer":"005BF20A95A51A4C46"}}}]}
	}
}
*/

func getObjectsFromSQSMessage(msgBody string) []string {
	objectPaths := make([]string, 0)
	mapping := make(map[string][]interface{})
	//msgBody := aws.StringValue(m.SQSMessage.Body)

	msgBodyInf, err := GetValueFromJSON([]byte(msgBody), []string{"Message"})
	if err != nil {
		glog.Infoln("The message is not the one from the bucket POST/PUT events. Detail ", err)
		return objectPaths
	}

	msgBody2 := msgBodyInf.(string)
	if err := json.Unmarshal([]byte(msgBody2), &mapping); err != nil {
		glog.Infoln("The message is not the one from the bucket POST/PUT events. Detail ", err)
		return objectPaths
	}

	records := mapping["Records"]
	for _, record := range records {
		recordByte, err := json.Marshal(record)
		if err != nil {
			glog.Errorln(err)
			continue
		}
		bucket, err := GetValueFromJSON(recordByte, []string{"s3", "bucket", "name"})
		if err != nil {
			glog.Errorln(err)
			continue
		}
		key, err := GetValueFromJSON(recordByte, []string{"s3", "object", "key"})
		if err != nil {
			glog.Errorln(err)
			continue
		}
		bucketName := bucket.(string)
		keyName := key.(string)

		objectPaths = append(objectPaths, "s3://"+bucketName+"/"+keyName)
	}

	return objectPaths
}

/*
HandleSQSMessage handles SQS message

The function takes a sqs message as input, extract the object urls and
decide which image need to be pulled to handle the s3 object
based on the object url

A SQS message may contains multiple records. The service goes though all
the records and compute the number of records need to be processed base
on their url and jobConfig list. If the number is larger than the availbility
of jobpool, the service will take a sleep until the resource is available.

If the function returns an error other than nil, the message is put back
to the queue and retry later (handled by `md` library). That makes sure
the message is properly handle before it actually deleted

*/
func (handler *SQSHandler) HandleSQSMessage(message *sqs.Message) error {

	jsonBody := *message.Body
	objectPaths := getObjectsFromSQSMessage(jsonBody)

	jobNameList := make([]string, 0)
	for _, jobConfig := range handler.JobConfigs {
		jobNameList = append(jobNameList, jobConfig.Name)
	}

	jobMap := make(map[string]JobConfig)
	for _, objectPath := range objectPaths {
		for _, jobConfig := range handler.JobConfigs {
			re := regexp.MustCompile(jobConfig.Pattern)
			if re.MatchString(objectPath) {
				jobMap[objectPath] = jobConfig
			}
		}
	}
	glog.Info("message:", jsonBody)

	glog.Infof("Start to run %d jobs", len(jobMap))

	for objectPath, jobConfig := range jobMap {
		for GetNumberRunningJobs() > GetMaxJobConfig() {
			time.Sleep(5 * time.Second)
		}
		jobInfo, err := CreateK8sJob(objectPath, jobConfig)
		if err != nil {
			glog.Infof("Error :%s", err)
			glog.Errorln(err)
			return err
		}
		jobInfo.SQSMessage = message
		out, err := json.Marshal(jobInfo)
		if err != nil {
			glog.Infof("Error :%s", err)
			glog.Errorln(err)
			return err
		}
		glog.Info(string(out))
	}

	return nil
}

func (handler *SQSHandler) handleAddNewJobConfig(jsonBytes []byte) error {
	jobConfig := JobConfig{}
	if err := json.Unmarshal(jsonBytes, &jobConfig); err != nil {
		return err
	}
	if jobConfig.Name != "" && jobConfig.Image != "" {
		handler.JobConfigs = append(handler.JobConfigs, jobConfig)
	} else {

		return errors.New("Name and image args are required")
	}
	return nil
}

func (handler *SQSHandler) handleDeleteJobConfig(pattern string) error {
	for idx, job := range handler.JobConfigs {
		if job.Pattern == pattern {
			handler.JobConfigs = append(handler.JobConfigs[:idx], handler.JobConfigs[idx+1:]...)
			return nil
		}
	}
	return fmt.Errorf("There is no job with provided pattern\n %s", pattern)
}

func (handler *SQSHandler) handleListJobConfigs() (string, error) {
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

// RetryCreateIndexingJob creates a manual job
func (handler *SQSHandler) RetryCreateIndexingJob(jsonBytes []byte) error {
	retryMessage := RetryMessage{}
	if err := json.Unmarshal(jsonBytes, &retryMessage); err != nil {
		return err
	}

	str := fmt.Sprintf(`{
		"Type" : "Notification",
		"Message" : "{\"Records\":[{\"eventSource\":\"aws:s3\",\"awsRegion\":\"us-east-1\",\"eventName\":\"ObjectCreated:Put\",\"s3\":{\"s3SchemaVersion\":\"1.0\",\"bucket\":{\"name\":\"%s\"},\"object\":{\"key\":\"%s\"}}}]}"}`, retryMessage.Bucket, retryMessage.Key)
	sqsMessage := sqs.Message{}
	sqsMessage.SetBody(str)

	return handler.HandleSQSMessage(&sqsMessage)
}

func (handler *SQSHandler) getJobStatusByCheckingMonitoredJobs(url string) string {
	for _, jobInfo := range listJobs() {
		if jobInfo.URL == url {
			return jobInfo.Status
		}
	}
	return ""
}
