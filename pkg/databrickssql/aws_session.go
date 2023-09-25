package databrickssql

import (
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/sts"
	"time"
)

const (
	tokenRequestDurationSeconds = int64(3600) // 1 hour
	earlyExpireTime             = 5 * time.Minute
)

type (
	EnvMergedCmdProvider struct {
		mergedValue *credentials.Value
	}

	TemporaryCredentialsProvider struct {
		permanentCredentials *credentials.Credentials
		region               string
		expireTime           *time.Time
	}
)

func NewEnvMergedCmdProvider(mergedValue *credentials.Value) *EnvMergedCmdProvider {
	return &EnvMergedCmdProvider{
		mergedValue: mergedValue,
	}
}

func (p *EnvMergedCmdProvider) Retrieve() (credentials.Value, error) {
	return *p.mergedValue, nil
}

func (p *EnvMergedCmdProvider) IsExpired() bool {
	return false
}

func NewTemporaryCredentialsProvider(permanentCredentials *credentials.Value, region string) *TemporaryCredentialsProvider {
	envMergedCmdProvider := NewEnvMergedCmdProvider(permanentCredentials)
	return &TemporaryCredentialsProvider{
		permanentCredentials: credentials.NewCredentials(envMergedCmdProvider),
		region:               region,
		expireTime:           nil,
	}
}

func (tp *TemporaryCredentialsProvider) Retrieve() (credentials.Value, error) {
	mySession := session.Must(session.NewSession(&aws.Config{
		Credentials: tp.permanentCredentials,
		Region:      &tp.region,
	}))

	durationSeconds := tokenRequestDurationSeconds
	tokenOutput, err := sts.New(mySession).GetSessionToken(&sts.GetSessionTokenInput{
		DurationSeconds: &durationSeconds,
	})
	if err != nil {
		return credentials.Value{}, err
	}

	tp.expireTime = tokenOutput.Credentials.Expiration
	return credentials.Value{
		AccessKeyID:     *tokenOutput.Credentials.AccessKeyId,
		SecretAccessKey: *tokenOutput.Credentials.SecretAccessKey,
		SessionToken:    *tokenOutput.Credentials.SessionToken,
	}, nil
}

func (tp *TemporaryCredentialsProvider) IsExpired() bool {
	if tp.expireTime == nil {
		return true
	}

	// Token will expire 5 minutes in advance
	// to avoid the token expiring during the retrieval progress
	return tp.expireTime.Before(time.Now().Add(earlyExpireTime))
}
