package handlers

import (
	"encoding/json"
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
)

func makeTestJson() string {
	return `{
		"AWS": {
		  "region": "us-east-1",
		  "aws_access_key_id" : "access_key_test",
		  "aws_secret_access_key": "secret_access_key_test"
		},
		"SQS": {
		  "url": "https://sqs.us-east-1.amazonaws.com/440721843528/mySQS"
		},
		"JOBS": [
		  {
			"name": "indexing",
			"pattern": "s3://xssxs/*",
			"image": "quay.io/cdis/indexs3client:master",
			"imageConfig": {
			  "url": "http://indexd-service/",
			  "username": "test",
			  "password": "test"
			}
		  },
		  {
			"name": "usersync",
			"pattern": "s3://xssxs/user.yaml",
			"image": "quay.io/cdis/fence:master",
			"imageConfig" :{}
		  }
		]
	  }`
}
func TestUtils(t *testing.T) {

	jsonStr := makeTestJson()
	regionInf, err := GetValueFromJson([]byte(jsonStr), []string{"AWS", "region"})
	if err != nil {
		t.Fatal(err.Error())
	}
	assert.Equal(t, regionInf.(string), "us-east-1")

	accessKeyInf, err := GetValueFromJson([]byte(jsonStr), []string{"AWS", "aws_access_key_id"})
	if err != nil {
		t.Fatal(err.Error())
	}
	assert.Equal(t, accessKeyInf.(string), "access_key_test")

	_, err = GetValueFromJson([]byte(jsonStr), []string{"AWS", "wrong_field"})
	if err == nil {
		t.Fatal(errors.New("err should not nil"))
	}

	jobInterfaces, err := GetValueFromJson([]byte(jsonStr), []string{"JOBS"})
	if err != nil {
		t.Fatal(err)
	}
	b, err := json.Marshal(jobInterfaces)
	jobConfigs := make([]JobConfig, 0)
	json.Unmarshal(b, &jobConfigs)
	assert.Equal(t, len(jobConfigs), 2)

}
