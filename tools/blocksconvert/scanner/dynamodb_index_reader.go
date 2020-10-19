package scanner

import (
	"context"
	"fmt"
	"net"
	"net/http"
	"net/url"
	"sort"
	"strings"
	"time"

	awssdk "github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/client"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/dynamodb"
	"github.com/aws/aws-sdk-go/service/dynamodb/dynamodbiface"
	"github.com/go-kit/kit/log"
	"github.com/go-kit/kit/log/level"
	"github.com/pkg/errors"
	"github.com/prometheus/client_golang/prometheus"
	awscommon "github.com/weaveworks/common/aws"
	"golang.org/x/sync/errgroup"

	"github.com/cortexproject/cortex/pkg/chunk"
	"github.com/cortexproject/cortex/pkg/chunk/aws"
)

type dynamoDbIndexReader struct {
	log      log.Logger
	awsCfg aws.StorageConfig

	rowsRead                  prometheus.Counter
	parsedIndexEntries        prometheus.Counter
	currentTableRanges        prometheus.Gauge
	currentTableScannedRanges prometheus.Gauge
}

func newDynamoDbIndexReader(awsCfg aws.StorageConfig, l log.Logger, rowsRead prometheus.Counter, parsedIndexEntries prometheus.Counter, currentTableRanges, scannedRanges prometheus.Gauge) *dynamoDbIndexReader {
	return &dynamoDbIndexReader{
		log:      l,
		awsCfg: awsCfg,

		rowsRead:                  rowsRead,
		parsedIndexEntries:        parsedIndexEntries,
		currentTableRanges:        currentTableRanges,
		currentTableScannedRanges: scannedRanges,
	}
}

func (r *dynamoDbIndexReader) IndexTableNames(ctx context.Context) ([]string, error) {
	level.Info(r.log).Log("msg", "aws dynamodb url", "url", r.awsCfg.DynamoDB.URL)
	client, err := dynamoClientFromURL(r.awsCfg.DynamoDB.URL)
	if err != nil {
		return nil, errors.Wrap(err, "create dynamodb client failed")
	}

	result, err := client.ListTables(&dynamodb.ListTablesInput{})

	tables := make([]string, len(result.TableNames) -1)
	for _, table := range result.TableNames {
		tables = append(tables, *table)
	}

	return tables, nil
}

func (r *dynamoDbIndexReader) ReadIndexEntries(ctx context.Context, tableName string, processors []IndexEntryProcessor) error {
	client, err := dynamoClientFromURL(r.awsCfg.DynamoDB.URL)
	if err != nil {
		return errors.Wrap(err, "create dynamodb client failed")
	}

	var segmentsPerProcessor int64 = 1

	g, gctx := errgroup.WithContext(ctx)

	for ix := range processors {
		p := processors[ix]

		g.Go(func() error {
			var currentSegment int64 = 0

			level.Info(r.log).Log("msg", "reading rows", "segment", currentSegment)

			var result *dynamodb.ScanOutput

			for {
				scanInput := &dynamodb.ScanInput{
					Segment:                   awssdk.Int64(currentSegment),
					TableName:                 awssdk.String(tableName),
					TotalSegments:             awssdk.Int64(segmentsPerProcessor),
				}

				if result != nil && result.LastEvaluatedKey != nil {
					scanInput.ExclusiveStartKey = result.LastEvaluatedKey
				}

				result, err := client.ScanWithContext(gctx, scanInput)
				if err != nil {
					return err
				}

				for _, row := range result.Items {
					entries, err := parseDynamoDbRow(row, tableName)
					if err != nil {
						return errors.Wrapf(err, "failed to parse row: %s", *row["h"].S)
					}

					for _, e := range entries {
						err := p.ProcessIndexEntry(e)
						if err != nil {
							return errors.Wrap(err, "processor error")
						}
					}

					r.parsedIndexEntries.Add(float64(len(entries)))
					r.rowsRead.Inc()
				}

				if result != nil && result.LastEvaluatedKey == nil {
					break
				}
			}

			return p.Flush()
		})
	}

	return g.Wait()
}

func parseDynamoDbRow(row map[string]*dynamodb.AttributeValue, tableName string) ([]chunk.IndexEntry, error) {
	var entries []chunk.IndexEntry

	rowKey := row["h"].S
	hashValue := row["c"].B
	rangeValue := row["r"].B

	entry := chunk.IndexEntry{
		TableName:  tableName,
		HashValue:  *rowKey,
		RangeValue: rangeValue,
		Value:      hashValue,
	}

	entries = append(entries, entry)

	if len(entries) > 1 {
		// Sort entries by RangeValue. This is done to support `newStorageClientColumnKey` version properly:
		// all index entries with same hashValue are in the same row, but map iteration over columns may
		// have returned them in wrong order.

		sort.Sort(sortableIndexEntries(entries))
	}

	return entries, nil
}

// Taken from pkg/chunk/aws/dynamodb_storage_client.go

// dynamoClientFromURL creates a new DynamoDB client from a URL.
func dynamoClientFromURL(awsURL *url.URL) (dynamodbiface.DynamoDBAPI, error) {
	dynamoDBSession, err := awsSessionFromURL(awsURL)
	if err != nil {
		return nil, err
	}
	return dynamodb.New(dynamoDBSession), nil
}

// awsSessionFromURL creates a new aws session from a URL.
func awsSessionFromURL(awsURL *url.URL) (client.ConfigProvider, error) {
	if awsURL == nil {
		return nil, fmt.Errorf("no URL specified for DynamoDB")
	}
	path := strings.TrimPrefix(awsURL.Path, "/")
	if len(path) > 0 {
		return nil, errors.Errorf("ignoring DynamoDB URL path %s", path)
	}
	config, err := awscommon.ConfigFromURL(awsURL)
	if err != nil {
		return nil, err
	}
	config = config.WithMaxRetries(0) // We do our own retries, so we can monitor them
	config = config.WithHTTPClient(&http.Client{Transport: defaultTransport})
	return session.NewSession(config)
}

// Copy-pasted http.DefaultTransport
var defaultTransport http.RoundTripper = &http.Transport{
	Proxy: http.ProxyFromEnvironment,
	DialContext: (&net.Dialer{
		Timeout:   30 * time.Second,
		KeepAlive: 30 * time.Second,
	}).DialContext,
	ForceAttemptHTTP2: true,
	MaxIdleConns:      100,
	// We will connect many times in parallel to the same DynamoDB server,
	// see https://github.com/golang/go/issues/13801
	MaxIdleConnsPerHost:   100,
	IdleConnTimeout:       90 * time.Second,
	TLSHandshakeTimeout:   10 * time.Second,
	ExpectContinueTimeout: 1 * time.Second,
}
