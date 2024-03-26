// SPDX-License-Identifier: Apache-2.0
//
// The OpenSearch Contributors require contributions made to
// this file be licensed under the Apache-2.0 license or a
// compatible open source license.

package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"os"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/opensearch-project/opensearch-go/v3"
	"github.com/opensearch-project/opensearch-go/v3/opensearchapi"
	"github.com/opensearch-project/opensearch-go/v3/opensearchutil"
	requestsigner "github.com/opensearch-project/opensearch-go/v3/signer/awsv2"
)

func mainWithError() error {
	ctx := context.Background()
	cfg, _ := config.LoadDefaultConfig(ctx)

	endpoint, endpoint_present := os.LookupEnv("ENDPOINT")
	if !endpoint_present {
		return errors.New("ENDPOINT missing")
	}

	service, service_present := os.LookupEnv("SERVICE")
	if !service_present {
		service = "es"
	}

	signer, err := requestsigner.NewSignerWithService(cfg, service)
	if err != nil {
		return err
	}

	client, err := opensearchapi.NewClient(
		opensearchapi.Config{
			Client: opensearch.Config{
				Addresses: []string{endpoint},
				Signer:    signer,
			},
		},
	)

	if err != nil {
		return err
	}

	// TODO: remove when OpenSearch Serverless adds /
	if service == "es" {
		if info, err := client.Info(ctx, &opensearchapi.InfoReq{}); err != nil {
			return err
		} else {
			fmt.Printf("%s: %s\n", info.Version.Distribution, info.Version.Number)
		}
	}

	index_name := "movies"

	if _, err := client.Indices.Create(
		ctx,
		opensearchapi.IndicesCreateReq{
			Index:  index_name,
			Body:   strings.NewReader(`{"settings": {"number_of_shards": 1, "number_of_replicas": 0}}`),
			Params: opensearchapi.IndicesCreateParams{WaitForActiveShards: "1"},
		},
	); err != nil {
		var opensearchError opensearchapi.Error
		if errors.As(err, &opensearchError) {
			if opensearchError.Err.Type != "resource_already_exists_exception" {
				return err
			}
		} else {
			return err
		}
	}

	// index a document
	type Movie struct {
		Title    string `json:"title"`
		Director string `json:"director"`
		Year     string `json:"year"`
	}

	document := Movie{
		Title:    "Moneyball",
		Director: "Bennett Miller",
		Year:     "2011",
	}

	if err != nil {
		return err
	}

	if _, err := client.Index(ctx, opensearchapi.IndexReq{
		Index:      index_name,
		DocumentID: "1",
		Body:       opensearchutil.NewJSONReader(document),
	}); err != nil {
		return err
	}

	// wait for the document to index
	time.Sleep(3 * time.Second)

	// search for the document
	query, err := json.Marshal(map[string]interface{}{
		"query": map[string]interface{}{
			"multi_match": map[string]interface{}{
				"query":  "miller",
				"fields": []string{"title^2", "director"},
			},
		},
	})

	if err != nil {
		return err
	}

	resp, err := client.Search(ctx, &opensearchapi.SearchReq{
		Indices: []string{index_name},
		Body:    strings.NewReader(string(query)),
	})

	if err != nil {
		return err
	}

	if resp.Hits.Total.Value > 0 {
		for _, hit := range resp.Hits.Hits {
			var movie Movie
			if err := json.Unmarshal([]byte(hit.Source), &movie); err != nil {
				return err
			}

			fmt.Printf("%s: %s\n", hit.ID, movie)
		}
	}

	if _, err := client.Document.Delete(ctx, opensearchapi.DocumentDeleteReq{
		Index:      index_name,
		DocumentID: "1",
	}); err != nil {
		return err
	}

	if _, err := client.Indices.Delete(ctx, opensearchapi.IndicesDeleteReq{
		Indices: []string{index_name},
		Params:  opensearchapi.IndicesDeleteParams{IgnoreUnavailable: opensearchapi.ToPointer(true)},
	}); err != nil {
		return err
	}

	return nil
}

func main() {
	if err := mainWithError(); err != nil {
		log.Fatal(err.Error())
	}

	os.Exit(0)
}
