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

	endpoint, endpoint_present := os.LookupEnv("ENDPOINT")
	if !endpoint_present {
		log.Fatal("ENDPOINT missing")
	}

	service, service_present := os.LookupEnv("SERVICE")
	if !service_present {
		service = "es"
	}

	signer, err := requestsigner.NewSignerWithService(cfg, service)
	if err != nil {
		log.Fatal("signer: ", err)
	}

	client, err := opensearch.NewClient(opensearch.Config{
		Addresses: []string{endpoint},
		Signer:    signer,
	})

	if err != nil {
		log.Fatal("client: ", err)
	}

	// TODO: remove when OpenSearch Serverless adds /
	if service == "es" {
		if info, err := client.Info(); err != nil {
			log.Fatal("info", err)
		} else {
			var r map[string]interface{}
			json.NewDecoder(info.Body).Decode(&r)
			version := r["version"].(map[string]interface{})
			fmt.Printf("%s: %s\n", version["distribution"], version["number"])
		}
	}

	index_name := "movies"

	// create an index
	if resp, err := client.Indices.Create(index_name, client.Indices.Create.WithWaitForActiveShards("1")); err != nil {
		log.Fatal("indices.create: ", err)
	} else {
		log.Print(resp)
	}

	// index a document
	document, err := json.Marshal(map[string]interface{}{
		"title":    "Moneyball",
		"director": "Bennett Miller",
		"year":     "2011",
	})

	if err != nil {
		log.Fatal("json: ", err)
	}

	if resp, err := client.Index(index_name, strings.NewReader(string(document)), client.Index.WithDocumentID(("1"))); err != nil {
		log.Fatal("index: ", err)
	} else {
		log.Print(resp)
	}

	// wait for the document to index
	time.Sleep(1 * time.Second)

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
		log.Fatal("json: ", err)
	}

	if resp, err := client.Search(client.Search.WithBody(strings.NewReader(string(query)))); err != nil {
		log.Fatal("index", err)
	} else {
		var r map[string]interface{}
		json.NewDecoder(resp.Body).Decode(&r)
		hits := r["hits"].(map[string]interface{})["hits"]
		fmt.Println(hits)
	}

	// delete the document
	if resp, err := client.Delete(index_name, "1"); err != nil {
		log.Fatal("delete: ", err)
	} else {
		log.Print(resp)
	}

	// delete the index
	if resp, err := client.Indices.Delete([]string{index_name}, client.Indices.Delete.WithIgnoreUnavailable(true)); err != nil {
		log.Fatal("indices.delete: ", err)
	} else {
		log.Print(resp)
	}
}
