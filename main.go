// SPDX-License-Identifier: Apache-2.0
//
// The OpenSearch Contributors require contributions made to
// this file be licensed under the Apache-2.0 license or a
// compatible open source license.

package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/opensearch-project/opensearch-go/v2"
	requestsigner "github.com/opensearch-project/opensearch-go/v2/signer/awsv2"
)

func main() {
	ctx := context.Background()
	cfg, _ := config.LoadDefaultConfig(ctx)
	signer, _ := requestsigner.NewSigner(cfg)

	endpoint, present := os.LookupEnv("OPENSEARCH_ENDPOINT")
	if !present {
		log.Fatal("OPENSEARCH_ENDPOINT missing")
	}

	client, _ := opensearch.NewClient(opensearch.Config{
		Addresses: []string{endpoint},
		Signer:    signer,
	})

	if info, err := client.Info(); err != nil {
		log.Fatal("info", err)
	} else {
		var r map[string]interface{}
		json.NewDecoder(info.Body).Decode(&r)
		version := r["version"].(map[string]interface{})
		fmt.Printf("%s: %s\n", version["distribution"], version["number"])
	}

	index_name := "movies"

	// create an index
	if _, err := client.Indices.Create(index_name, client.Indices.Create.WithWaitForActiveShards("1")); err != nil {
		log.Fatal("indices.create", err)
	}

	// index a document
	document, _ := json.Marshal(map[string]interface{}{
		"title":    "Moneyball",
		"director": "Bennett Miller",
		"year":     "2011",
	})

	if _, err := client.Index(index_name, strings.NewReader(string(document)), client.Index.WithDocumentID(("1"))); err != nil {
		log.Fatal("index", err)
	}

	// wait for the document to index
	time.Sleep(1 * time.Second)

	// search for the document
	query, _ := json.Marshal(map[string]interface{}{
		"query": map[string]interface{}{
			"multi_match": map[string]interface{}{
				"query":  "miller",
				"fields": []string{"title^2", "director"},
			},
		},
	})

	if resp, err := client.Search(client.Search.WithBody(strings.NewReader(string(query)))); err != nil {
		log.Fatal("index", err)
	} else {
		var r map[string]interface{}
		json.NewDecoder(resp.Body).Decode(&r)
		hits := r["hits"].(map[string]interface{})["hits"]
		fmt.Println(hits)
	}

	// delete the document
	if _, err := client.Delete(index_name, "1"); err != nil {
		log.Fatal("delete", err)
	}

	// delete the index
	if _, err := client.Indices.Delete([]string{index_name}, client.Indices.Delete.WithIgnoreUnavailable(true)); err != nil {
		log.Fatal("indices.delete", err)
	}
}
