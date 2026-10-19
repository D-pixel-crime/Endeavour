package config

import (
	"context"
	"fmt"

	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
)

func NewDefaultSQSClient(ctx context.Context) (*sqs.Client, error) {
	awsCfg, err := config.LoadDefaultConfig(ctx)
	if err != nil {
		return nil, fmt.Errorf("Error Loading AWS Config: %w", err)
	}
	return sqs.NewFromConfig(awsCfg), nil
}
