package main

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"github.com/yandex-cloud/ydb-go-sdk"
	"github.com/yandex-cloud/ydb-go-sdk/table"
	"io/ioutil"
	"log"
	"os"
)

type YdbClient struct {
	ctx     context.Context
	config  YdbConfig
	driver  ydb.Driver
	tc      table.Client
	txc     *table.TransactionControl
	session *table.Session
}

func CreateYdbClient(ctx context.Context, config YdbConfig) (*YdbClient, error) {
	log.Printf("Endpoint: %s", config.Endpoint)

	cert, err := ioutil.ReadFile(os.Getenv("YDB_SSL_ROOT_CERTIFICATES_FILE"))
	if err != nil {
		log.Fatal(err)
	}
	rootCerts := x509.NewCertPool()
	if ok := rootCerts.AppendCertsFromPEM(cert); !ok {
		log.Fatal(err)
	}

	/*credentials, err := iam.NewClient(
		iam.WithServiceFile(os.Getenv("SA_SERVICE_FILE")),
		iam.WithDefaultEndpoint(),
		iam.WithSystemCertPool(),
	)
	if err != nil {
		log.Fatal(err)
	}*/

	credentials := ydb.AuthTokenCredentials{
		AuthToken: os.Getenv("YDB_TOKEN"),
	}

	dialer := &ydb.Dialer{
		DriverConfig: &ydb.DriverConfig{
			Database:    "/ru-central1/b1g3ltcmontdpo0d9pfl/etn01rv8hgolt2ie4q4g",
			Credentials: credentials,
			Trace: ydb.DriverTrace{
				DialStart:           func(d ydb.DialStartInfo) { log.Printf("DialStart %v", d) },
				DialDone:            func(d ydb.DialDoneInfo) { log.Printf("DialDone %v", d) },
				GetConnStart:        func(d ydb.GetConnStartInfo) { log.Printf("GetConnStart %v", d) },
				GetConnDone:         func(d ydb.GetConnDoneInfo) { log.Printf("GetConnDone %v", d) },
				TrackConnStart:      func(d ydb.TrackConnStartInfo) { log.Printf("TrackConnStart %v", d) },
				TrackConnDone:       func(d ydb.TrackConnDoneInfo) { log.Printf("TrackConnDone %v", d) },
				GetCredentialsStart: func(d ydb.GetCredentialsStartInfo) { log.Printf("GetCredentialsStart %v", d) },
				GetCredentialsDone:  func(d ydb.GetCredentialsDoneInfo) { log.Printf("GetCredentialsDone %v", d) },
				DiscoveryStart:      func(d ydb.DiscoveryStartInfo) { log.Printf("DiscoveryStart %v", d) },
				DiscoveryDone:       func(d ydb.DiscoveryDoneInfo) { log.Printf("DiscoveryDone %v", d) },
				OperationStart:      func(d ydb.OperationStartInfo) { log.Printf("OperationStart %v", d) },
				OperationWait:       func(d ydb.OperationWaitInfo) { log.Printf("OperationWait %v", d) },
				OperationDone:       func(d ydb.OperationDoneInfo) { log.Printf("OperationDone %v", d) },
				StreamStart:         func(d ydb.StreamStartInfo) { log.Printf("StreamStart %v", d) },
				StreamRecvStart:     func(d ydb.StreamRecvStartInfo) { log.Printf("StreamRecvStart %v", d) },
				StreamRecvDone:      func(d ydb.StreamRecvDoneInfo) { log.Printf("StreamRecvDone %v", d) },
				StreamDone:          func(d ydb.StreamDoneInfo) { log.Printf("StreamDone %v", d) },
			},
		},
		TLSConfig: &tls.Config{
			RootCAs: rootCerts,
		},
	}
	driver, err := dialer.Dial(ctx, config.Endpoint)
	if err != nil {
		return nil, fmt.Errorf("dial error: %v", err)
	}
	tc := table.Client{
		Driver: driver,
	}
	s, err := tc.CreateSession(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to create session: %v", err)
	}
	txc := table.TxControl(
		table.BeginTx(table.WithSerializableReadWrite()),
		table.CommitTx(),
	)

	return &YdbClient{
		ctx:     ctx,
		config:  config,
		driver:  driver,
		tc:      tc,
		txc:     txc,
		session: s,
	}, nil
}

func (c YdbClient) Execute(query string, params *table.QueryParameters, opt ...table.ExecuteDataQueryOption) (txr *table.Transaction, r *table.Result, err error) {
	return c.session.Execute(c.ctx, c.txc, query, params, opt...)
}

func (c YdbClient) Close() {
	c.session.Close(c.ctx)
	c.driver.Close()
}
