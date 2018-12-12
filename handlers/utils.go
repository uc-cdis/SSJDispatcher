package handlers

import (
	"encoding/json"
	"errors"
	"fmt"
	"io/ioutil"
)

func ReadFile(path string) ([]byte, error) {
	buff, err := ioutil.ReadFile(path)
	if err != nil {
		fmt.Println(err.Error())
		return nil, err
	}
	return buff, nil
}

func containKey(mapping map[string]interface{}, key string) bool {
	for k := range mapping {
		if k == key {
			return true
		}
	}
	return false
}

func GetValueFromJson(jsonBytes []byte, keys []string) (interface{}, error) {
	var dataMap interface{}
	json.Unmarshal(jsonBytes, &dataMap)
	for _, key := range keys {
		if containKey(dataMap.(map[string]interface{}), key) {
			dataMap = dataMap.(map[string]interface{})[key]
		} else {
			return nil, errors.New("Does not contain key " + key)
		}
	}
	return dataMap, nil

}
