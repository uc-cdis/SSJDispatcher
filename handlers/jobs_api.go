package handlers

import (
	"encoding/json"
	"fmt"
	"net/http"
)

func RegisterJob() {
	http.HandleFunc("/job/status", status)
	http.HandleFunc("/job/list", list)
}

// status checks the status of the job given UID
func status(w http.ResponseWriter, r *http.Request) {
	if r.Method != "GET" {
		http.Error(w, "Not supported request method.", 405)
		return
	}
	UID := r.URL.Query().Get("UID")
	if UID != "" {
		result, errUID := getJobStatusByID(UID)
		if errUID != nil {
			http.Error(w, errUID.Error(), 500)
			return
		}

		out, err := json.Marshal(result)
		if err != nil {
			http.Error(w, err.Error(), 500)
			return
		}

		fmt.Fprintf(w, string(out))
	} else {
		http.Error(w, "Missing UID argument", 300)
		return
	}
}

// list all the jobs
func list(w http.ResponseWriter, r *http.Request) {
	if r.Method != "GET" {
		http.Error(w, "Not supported request method.", 405)
		return
	}
	result := listJobs(getJobClient())

	out, err := json.Marshal(result)
	if err != nil {
		http.Error(w, err.Error(), 500)
		return
	}

	fmt.Fprintf(w, string(out))
}
