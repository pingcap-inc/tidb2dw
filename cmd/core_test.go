package cmd

import (
	"os"
	"testing"

	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/stretchr/testify/require"
)

func TestGetS3URIWithCredentialsDoesNotEmbedSecrets(t *testing.T) {
	t.Setenv("AWS_ACCESS_KEY_ID", "")
	t.Setenv("AWS_SECRET_ACCESS_KEY", "")
	t.Setenv("AWS_SESSION_TOKEN", "")

	uri, err := getS3URIWithCredentials("s3://bucket/root?access-key=old&secret-access-key=old", &credentials.Value{
		AccessKeyID:     "AKIA_TEST",
		SecretAccessKey: "SECRET_TEST",
		SessionToken:    "TOKEN_TEST",
	})

	require.NoError(t, err)
	require.Equal(t, "s3://bucket/root", uri.String())
	require.Empty(t, uri.RawQuery)
	require.Equal(t, "AKIA_TEST", os.Getenv("AWS_ACCESS_KEY_ID"))
	require.Equal(t, "SECRET_TEST", os.Getenv("AWS_SECRET_ACCESS_KEY"))
	require.Equal(t, "TOKEN_TEST", os.Getenv("AWS_SESSION_TOKEN"))
}

func TestIsLocalAPIAddress(t *testing.T) {
	require.True(t, isLocalAPIAddress("127.0.0.1:8185"))
	require.True(t, isLocalAPIAddress("localhost:8185"))
	require.False(t, isLocalAPIAddress("0.0.0.0:8185"))
	require.False(t, isLocalAPIAddress("192.0.2.10:8185"))
}
