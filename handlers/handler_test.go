package handlers

import (
	"context"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/sqs"
	"github.com/aws/aws-sdk-go/service/sqs/sqsiface"
	mq "github.com/remind101/mq-go"
	"github.com/remind101/mq-go/pkg/memsqs"
	"github.com/stretchr/testify/assert"
)

func makeSQSHandler() *SQSHandler {
	// TODO
	return NewSQSHandler("http://aws_test_sqs")
}

func fakeResponse() string {
	return `{"Records":[
		{"eventVersion":"2.0","eventSource":"aws:s3","awsRegion":"us-east-1",
		"eventTime":"2018-11-19T00:57:57.693Z","eventName":"ObjectCreated:Put",
		"userIdentity":{"principalId":"AWS:AIDAIU3LRUEK5OHS6FXRQ"},
		"requestParameters":{"sourceIPAddress":"173.24.34.163"},
		"responseElements":{"x-amz-request-id":"91CF670A054E0332",
		"x-amz-id-2":"h0ZQgg6w2qzKUkzivRizP1E1Jf9QAXSu1bUllWaF2b7j/63XRgjGLMNyI7sl016QKSaxK1L2RrI="},
		"s3":{"s3SchemaVersion":"1.0","configurationId":"Giang Bui",
		"bucket":{"name":"xssxs","ownerIdentity":{"principalId":"A365FU9MXCCF0S"},
		"arn":"arn:aws:s3:::xssxs"},"object":{"key":"api.py","size":8005,"eTag":"b4ef93035ff791f7d507a47342c89cd6",
		"sequencer":"005BF20A95A51A4C46"}}}]}`
}

func TestServer(t *testing.T) {
	qURL := "jobs"
	done := make(chan struct{})
	c := memsqs.New()

	c.SendMessage(&sqs.SendMessageInput{
		QueueUrl: aws.String(qURL),
		//MessageBody: aws.String(`{"name":"test"}`),
		MessageBody: aws.String(fakeResponse()),
	})

	h := mq.HandlerFunc(func(m *mq.Message) error {
		assert.Equal(t, fakeResponse(), aws.StringValue(m.SQSMessage.Body))
		close(done)
		return nil
	})

	sp := newServer(t, qURL, h, c)
	sp.Start()
	<-done             // Wait for server to handle message
	closeServer(t, sp) // Wait for server to shutdown
	assert.Empty(t, len(c.Queue(qURL)))
}

func newServer(t *testing.T, qURL string, h mq.Handler, c sqsiface.SQSAPI, opts ...func(*mq.Server)) *mq.Server {
	testDefaults := func(s *mq.Server) {
		s.Client = c
		s.ErrorHandler = func(err error) {
			t.Fatal(err)
		}
	}
	opts = append([]func(*mq.Server){testDefaults}, opts...)

	handler := NewSQSHandler(qURL)
	handler.Server = mq.NewServer(handler.QueueURL, h, opts...)

	return handler.Server
}

func closeServer(t *testing.T, s *mq.Server) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
	defer cancel()
	assert.NoError(t, s.Shutdown(ctx))
}
