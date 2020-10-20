package scanner

import (
	"testing"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/dynamodb"

	"github.com/stretchr/testify/require"

	"github.com/cortexproject/cortex/pkg/chunk"
)

func TestParseDynamoDbRow(t *testing.T) {
	tcs := map[string]struct {
		row map[string]*dynamodb.AttributeValue

		table string

		expectedEntries []chunk.IndexEntry
		expectedError   string
	}{
		"newStorageClientV1 format": {
			row: map[string]*dynamodb.AttributeValue{
				"h": &dynamodb.AttributeValue{
					S: aws.String("testuser:d18446:ebYAZVS9yuA8K4VQnZU1ldJL5gUnOVF5PDe4i/6AFW0"),
				},
				"r": &dynamodb.AttributeValue{
					B: []byte("MDMwODY4ZTYAADAvZGNkMzg2MGRiY2JkYWUyNjoxNzMxM2I4NGYxNjoxNzMxNTAxNzBlNjplMmMwY2VkMgAzAA=="),
				},
				"c": &dynamodb.AttributeValue{
					B: []byte("LQ=="),
				},
			},
			table: "test",
			expectedEntries: []chunk.IndexEntry{
				{
					TableName:  "test",
					HashValue:  "testuser:d18446:ebYAZVS9yuA8K4VQnZU1ldJL5gUnOVF5PDe4i/6AFW0",
					RangeValue: []byte("MDMwODY4ZTYAADAvZGNkMzg2MGRiY2JkYWUyNjoxNzMxM2I4NGYxNjoxNzMxNTAxNzBlNjplMmMwY2VkMgAzAA=="),
					Value:      []byte("LQ=="),
				},
			},
		},
	}

	for name, tc := range tcs {
		t.Run(name, func(t *testing.T) {
			entries, err := parseDynamoDbRow(tc.row, tc.table)

			if tc.expectedError != "" {
				require.EqualError(t, err, tc.expectedError)
				require.Nil(t, entries)
			} else {
				require.NoError(t, err)
				require.Equal(t, tc.expectedEntries, entries)
			}
		})
	}
}
