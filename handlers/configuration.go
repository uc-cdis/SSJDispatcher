package handlers

import (
	"os"
)

func Lookup_cred_file() string {
	val, found := os.LookupEnv("CRED_FILE")
	if found == false {
		val = "./credentials.json"
	}
	return val
}
