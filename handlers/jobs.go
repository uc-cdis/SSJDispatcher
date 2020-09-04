package handlers

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/http"
	"os"

	"github.com/aws/aws-sdk-go/service/sqs"
	"github.com/golang/glog"

	"k8s.io/client-go/kubernetes"
	batchtypev1 "k8s.io/client-go/kubernetes/typed/batch/v1"
	"k8s.io/client-go/rest"
)

var (
	trueVal  = true
	falseVal = false
)

// SOWER_URL
const SOWER_URL = "http://sower-service"

type JobsArray struct {
	JobInfo []JobInfo `json:"jobs"`
}

type JobInfo struct {
	UID        string `json:"uid"`
	Name       string `json:"name"`
	Status     string `json:"status"`
	URL        string `json:"url"`
	SQSMessage *sqs.Message
}

func getJobClient() batchtypev1.JobInterface {
	// creates the in-cluster config
	config, err := rest.InClusterConfig()
	if err != nil {
		panic(err.Error())
	}
	// creates the clientset
	clientset, err := kubernetes.NewForConfig(config)
	// Access jobs. We can't do it all in one line, since we need to receive the
	// errors and manage thgem appropriately
	batchClient := clientset.BatchV1()
	jobsClient := batchClient.Jobs(os.Getenv("GEN3_NAMESPACE"))
	return jobsClient
}

// CreateSowerJob creates a sower job
func CreateSowerJob(inputURL string, jobConfig JobConfig) (*JobInfo, error) {

	requestBody := fmt.Sprintf(`{"action":"index-object", "input": {"URL": "%s"}}`, inputURL)

	// TODO: Need to obtain a token and add to the post request header
	resp, err := http.Post(fmt.Sprintf("%s/dispatch", SOWER_URL), "application/json", bytes.NewBuffer([]byte(requestBody)))

	if err != nil {
		return nil, err
	}

	defer resp.Body.Close()

	body, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}
	jobinfo := new(JobInfo)
	_ = json.Unmarshal(body, &jobinfo)
	glog.Info(string(body))
	return jobinfo, nil

}

// GetJobStatusByID
func GetJobStatusByID(jobID string) (*JobInfo, error) {
	resp, err := http.Get(fmt.Sprintf("%s/status?UID=%s", SOWER_URL, jobID))
	if err != nil {
		glog.Infof("Can not get job status of job %s", jobID)
		return nil, err
	}

	defer resp.Body.Close()
	body, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}
	jobinfo := new(JobInfo)
	_ = json.Unmarshal(body, &jobinfo)
	glog.Info(string(body))
	return jobinfo, nil

}
