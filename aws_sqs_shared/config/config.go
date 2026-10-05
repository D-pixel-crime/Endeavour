package config

import (
	"github.com/aws/aws-sdk-go-v2/service/sqs"
)

type SQSConfig struct {
	Region          string
	AccessKey       string
	SecretAccessKey string
	Endpoint        string
}

func LoadFromEnv(region, accessKeyID, secretAccessKey, endpoint string) *SQSConfig {
	return &SQSConfig{
		Region:          region,
		AccessKey:       accessKeyID,
		SecretAccessKey: secretAccessKey,
		Endpoint:        endpoint,
	}
}

func NewSQSClient(cfg *SQSConfig) *sqs.Client {
	return nil
}
