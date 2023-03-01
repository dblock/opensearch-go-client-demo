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
	"io"
	"log"
	"os"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/opensearch-project/opensearch-go/v2"
	"github.com/opensearch-project/opensearch-go/v2/opensearchapi"
	requestsigner "github.com/opensearch-project/opensearch-go/v2/signer/awsv2"
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

	client, err := opensearch.NewClient(opensearch.Config{
		Addresses: []string{endpoint},
		Signer:    signer,
	})

	if err != nil {
		return err
	}

	// TODO: remove when OpenSearch Serverless adds /
	if service == "es" {
		if info, err := client.Info(); err != nil {
			return err
		} else {
			var r map[string]interface{}
			json.NewDecoder(info.Body).Decode(&r)
			version := r["version"].(map[string]interface{})
			fmt.Printf("%s: %s\n", version["distribution"], version["number"])
		}
	}

	index_name := "movies"

	if _, err := opensearchDo(ctx, client, &opensearchapi.IndicesCreateRequest{
		Index:               index_name,
		WaitForActiveShards: "1",
	}); err != nil {
		if !strings.Contains(err.Error(), "resource_already_exists_exception") {
			return err
		}
	}

	// index a document
	document, err := json.Marshal(map[string]interface{}{
		"title":    "Moneyball",
		"director": "Bennett Miller",
		"year":     "2011",
	})

	if err != nil {
		return err
	}

	if _, err := opensearchDo(ctx, client, &opensearchapi.IndexRequest{
		Index:      index_name,
		Body:       strings.NewReader(string(document)),
		DocumentID: "1",
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

	resp, err := opensearchDo(ctx, client, &opensearchapi.SearchRequest{
		Body: strings.NewReader(string(query)),
	})

	if err != nil {
		return err
	}

	var r map[string]interface{}
	json.NewDecoder(resp.Body).Decode(&r)

	if r != nil {
		hits := r["hits"].(map[string]interface{})["hits"]
		fmt.Println(hits)
	}

	if _, err := opensearchDo(ctx, client, &opensearchapi.DeleteRequest{
		Index:      index_name,
		DocumentID: "1",
	}); err != nil {
		return err
	}

	if _, err := opensearchDo(ctx, client, &opensearchapi.IndicesDeleteRequest{
		Index:             []string{index_name},
		IgnoreUnavailable: opensearchapi.BoolPtr(true),
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

func opensearchDo(ctx context.Context, osClient *opensearch.Client, req opensearchapi.Request) (*opensearchapi.Response, error) {
	resp, err := req.Do(ctx, osClient)
	if err != nil {
		return resp, err
	}
	if resp.IsError() {
		body, err := io.ReadAll(resp.Body)
		if err != nil {
			return resp, err
		}
		// TODO: parse JSON error into a structure
		return nil, fmt.Errorf("status: %v, msg: %s", resp.StatusCode, string(body))
	}
	return resp, nil
}
