package handlers

import (
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/sqs"
	"github.com/aws/aws-sdk-go/service/sqs/sqsiface"
	"github.com/golang/glog"
)

// S3Credentials contains AWS credentials
type AWSCredentials struct {
	region             string
	awsAccessKeyID     string
	awsSecretAccessKey string
}

// NewSQSClient create new SQSAPI client
func NewSQSClient() (sqsiface.SQSAPI, error) {
	cred, err := loadCredentialFromConfigFile(LookupCredFile())
	if err != nil {
		return nil, err
	}

	if cred.region != "" && cred.awsAccessKeyID != "" && cred.awsSecretAccessKey != "" {
		config := aws.NewConfig().WithRegion(cred.region).WithCredentials(credentials.NewStaticCredentials(cred.awsAccessKeyID, cred.awsSecretAccessKey, ""))
		return sqs.New(session.New(config)), nil
	} else {
		sess, _ := session.NewSession()
		return sqs.New(sess), nil
	}
}

// loadCredentialFromConfigFile loads AWS credentials from the config file
func loadCredentialFromConfigFile(path string) (*AWSCredentials, error) {
	credentials := new(AWSCredentials)
	// Read data file
	jsonBytes, err := ReadFile(path)
	if err != nil {
		return nil, err
	}

	if region, err := GetValueFromJSON(jsonBytes, []string{"AWS", "region"}); err != nil {
		glog.Info("Can not read region from credential file. Gonna use attached service account")
	} else {
		credentials.region = region.(string)
	}

	if awsKeyID, err := GetValueFromJSON(jsonBytes, []string{"AWS", "aws_access_key_id"}); err != nil {
		glog.Info("Can not read aws key from credential file. Gonna use attached service account ")
	} else {
		credentials.awsAccessKeyID = awsKeyID.(string)
	}

	if awsSecret, err := GetValueFromJSON(jsonBytes, []string{"AWS", "aws_secret_access_key"}); err != nil {
		glog.Info("Can not read aws secret key from credential file. Gonna use attached service account")
	} else {
		credentials.awsSecretAccessKey = awsSecret.(string)
	}

	return credentials, nil
}
