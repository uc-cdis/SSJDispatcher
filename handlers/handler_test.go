package handlers

import (
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestGetObjectsFromSQSMessage(t *testing.T) {
	recordsBytes, _ := json.Marshal(map[string]interface{}{
		"Records": []map[string]interface{}{
			{
				"s3": map[string]interface{}{
					"bucket": map[string]interface{}{
						"name": "mybucket",
					},
					"object": map[string]interface{}{
						"key": "mykey",
					},
				},
			},
		},
	})
	messageBytes, _ := json.Marshal(map[string]string{
		"Message": string(recordsBytes),
	})
	message := string(messageBytes)

	objects := getObjectsFromSQSMessage(message)

	assert.Equal(t, objects, []string{"s3://mybucket/mykey"})
}
