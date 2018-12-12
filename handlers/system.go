package handlers

import (
	"fmt"
	"net/http"
)

func RegisterSystem() {
	http.HandleFunc("/_status", systemStatus)
}

func systemStatus(w http.ResponseWriter, r *http.Request) {
	fmt.Fprintf(w, "Healthy")
}
