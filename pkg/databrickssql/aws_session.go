// Copyright 2022 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

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
