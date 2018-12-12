/*
K8S Dispatcher Service

// register SQS url
curl -X PUT http://localhost:8000/sqs?url=https://sqs.us-east-1.amazonaws.com/440721843528/mySQS

// start the service/server
curl -X PUT http://localhost:8000/server?start=true

// register new job
curl -X POST http://localhost:8000/job -d '{"Name":"TEST3", "Pattern":"s3://test/*","Image":"quay.io/cdis/indexs3client:master", "ImageConfig":{}}'

*/
package main

import (
	"context"
	"encoding/json"
	"log"
	"net/http"

	"github.com/uc-cdis/ssjdispatcher/handlers"
)

func main() {
	jsonBytes, err := handlers.ReadFile(handlers.Lookup_cred_file())
	if err != nil {
		log.Println("Can not read credential file!")
		return
	}

	var sqsURL string
	if sqs, err := handlers.GetValueFromJson(jsonBytes, []string{"SQS", "url"}); err != nil {
		log.Println("Can not read SQS url from credential file!")
		return
	} else {
		sqsURL = sqs.(string)
	}

	jobInterfaces, _ := handlers.GetValueFromJson(jsonBytes, []string{"JOBS"})

	b, err := json.Marshal(jobInterfaces)
	if err != nil {
		log.Println(err)
		return
	}
	jobConfigs := make([]handlers.JobConfig, 0)
	json.Unmarshal(b, &jobConfigs)

	// start an SQSHandler instance
	SQSHandler := handlers.NewSQSHandler(sqsURL)
	SQSHandler.StartServer()
	defer SQSHandler.Server.Shutdown(context.Background())

	SQSHandler.JobConfigs = jobConfigs

	SQSHandler.RegisterSQSHandler()

	handlers.RegisterJob()
	handlers.RegisterSystem()

	log.Fatal(http.ListenAndServe("0.0.0.0:8000", nil))
}
