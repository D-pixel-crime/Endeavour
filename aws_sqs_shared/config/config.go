package config

import (
	"context"
	"fmt"

	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
)

type SQSConfig struct {
	Region          string
	AccessKey       string
	SecretAccessKey string
	Endpoint        string
}

func GetSQSConfig(region, accessKeyID, secretAccessKey, endpoint string) *SQSConfig {
	return &SQSConfig{
		Region:          region,
		AccessKey:       accessKeyID,
		SecretAccessKey: secretAccessKey,
		Endpoint:        endpoint,
	}
}

func NewSQSClient(ctx context.Context, cfg *SQSConfig) (*sqs.Client, error) {
	var opts []func(*config.LoadOptions) error

	opts = append(opts, config.WithRegion(cfg.Region))

	if cfg.AccessKey != "" && cfg.SecretAccessKey != "" {
		opts = append(opts, config.WithCredentialsProvider(
			credentials.NewStaticCredentialsProvider(
				cfg.AccessKey,
				cfg.SecretAccessKey,
				"",
			),
		))
	}

	awsCfg, err := config.LoadDefaultConfig(ctx, opts...)
	if err != nil {
		return nil, fmt.Errorf("Error Loading AWS Config: %w", err)
	}

	var sqsOpts []func(*sqs.Options)

	if cfg.Endpoint != "" {
		sqsOpts = append(sqsOpts, func(o *sqs.Options) {
			o.BaseEndpoint = &cfg.Endpoint
		})
	}

	return sqs.NewFromConfig(awsCfg, sqsOpts...), nil
}
