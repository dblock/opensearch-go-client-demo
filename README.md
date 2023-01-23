# OpenSearch Go Client Demo

Makes requests to Amazon OpenSearch using the [OpenSearch Go Client](https://github.com/opensearch-project/opensearch-go).

## Prerequisites

### Go

Install [Go](https://go.dev/doc/install). YMMV.

```
$ go version
go version go1.19.4 darwin/arm64
```

## Running

Create an OpenSearch domain in (AWS) which support IAM based AuthN/AuthZ.

```
export AWS_ACCESS_KEY_ID=
export AWS_SECRET_ACCESS_KEY=
export AWS_SESSION_TOKEN=
export AWS_REGION=us-west2

export ENDPOINT=https://....us-west-2.es.amazonaws.com

$ go run main.go
```

This will output the version of OpenSearch and a search result.

```
opensearch: 2.3.0
[map[_id:1 _index:movies _score:0.18232156 _source:map[director:Bennett Miller title:Moneyball year:2011]]]
```

The [code](main.go) will create an index, add a document to it, search, then cleanup.

## License 

This project is licensed under the [Apache v2.0 License](LICENSE.txt).

## Copyright

Copyright OpenSearch Contributors. See [NOTICE](NOTICE.txt) for details.
