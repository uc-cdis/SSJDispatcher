package handlers

import (
	"fmt"
	"io/ioutil"
	"net/http"
)

// RegisterSQSHandler registers endpoints
func (handler *SQSHandler) RegisterSQSHandler() {
	http.HandleFunc("/jobConfig", handler.HandleJobConfig)
	http.HandleFunc("/dispatchJob", handler.HandleDispatchJob)
}

// HandleJobConfig handles job config endpoints
// to add a jobConfig
//		curl -X POST http://localhost/jobConfig -d `{"name": "usersync", "pattern": "s3://bucket/usersync.yaml", "image": "quay.io/cdis/fence:master", "imageConfig":{}}`
// to delete jobConfig
// 		curl -X DELETE http://localhost/jobConfig?pattern=s3://bucket/usersync.yaml
func (handler *SQSHandler) HandleJobConfig(w http.ResponseWriter, r *http.Request) {
	if r.Method == "GET" {
		handler.listJobConfigs(w, r)
	} else if r.Method == "POST" {
		handler.addJobConfig(w, r)
	} else if r.Method == "DELETE" {
		handler.deleteJobConfig(w, r)
	}
}

// addJob adds an job config
func (handler *SQSHandler) addJobConfig(w http.ResponseWriter, r *http.Request) {
	// An example of an json body
	//
	// {
	// 	"name": "indexing",
	// 	"pattern": "s3://bucket/*",
	// 	"image": "quay.io/cdis/indexs3client:master",
	// 	"imageConfig": {
	//	   "indexURL": "http://index-service/"
	//	}
	// }
	//
	// Try to read the request body.
	body, err := ioutil.ReadAll(r.Body)
	if err != nil {
		msg := fmt.Sprintf("failed to read request body; encountered error: %s", err)
		http.Error(w, msg, http.StatusBadRequest)
		return
	}
	if err = handler.handleAddNewJobConfig(body); err != nil {
		msg := fmt.Sprintf("failed to add new job config; encountered error: %s", err)
		http.Error(w, msg, http.StatusBadRequest)
		return
	}
	fmt.Fprintf(w, "Successfully add a new job!")
}

func (handler *SQSHandler) deleteJobConfig(w http.ResponseWriter, r *http.Request) {
	val := r.URL.Query().Get("pattern")
	if err := handler.handleDeleteJobConfig(val); err != nil {
		msg := fmt.Sprintf("failed to delete an job config; encountered error: %s", err)
		http.Error(w, msg, http.StatusBadRequest)
		return
	}
	fmt.Fprintf(w, "Successfully delete the job")
}

func (handler *SQSHandler) listJobConfigs(w http.ResponseWriter, r *http.Request) {
	str, err := handler.handleListJobConfigs()
	if err != nil {
		msg := fmt.Sprintf("failed to list job config; encountered error: %s", err)
		http.Error(w, msg, http.StatusInternalServerError)
	}
	fmt.Fprintf(w, str)
}

// HandleDispatchJob dispatch an job
func (handler *SQSHandler) HandleDispatchJob(w http.ResponseWriter, r *http.Request) {
	if r.Method == "POST" {
		handler.dispatchJob(w, r)
	}
}

// addJob adds an job config
func (handler *SQSHandler) dispatchJob(w http.ResponseWriter, r *http.Request) {
	// Try to read the request body.
	body, err := ioutil.ReadAll(r.Body)
	if err != nil {
		msg := fmt.Sprintf("failed to read request body; encountered error: %s", err)
		http.Error(w, msg, http.StatusBadRequest)
		return
	}
	if err = handler.RetryCreateIndexingJob(body); err != nil {
		msg := fmt.Sprintf("failed to dispatch an job; encountered error: %s", err)
		http.Error(w, msg, http.StatusBadRequest)
		return
	}
	fmt.Fprintf(w, "Successfully dispatch a new job!")
}

// getIndexingJobStatus get indexing job status
func (handler *SQSHandler) getIndexingJobStatus(w http.ResponseWriter, r *http.Request) {
	if r.Method != "GET" {
		http.Error(w, "Not supported request method.", 405)
		return
	}
	// get object url
	url := r.URL.Query().Get("url")
	if url != "" {
		status := handler.getJobStatusByCheckingMonitoredJobs(url)
		fmt.Fprintf(w, status)
	} else {
		http.Error(w, "Missing url argument", 300)
		return
	}
}
