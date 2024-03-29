package main

import (
	"context"
	"github.com/codenotary/immudb/pkg/api/schema"
	immuclient "github.com/codenotary/immudb/pkg/client"
	"log"
	"strconv"
)

type kv struct {
	Key   string `json:"key"`
	Value string `json:"value"`
}

func buildKv(client immuclient.ImmuClient, ctx context.Context, tx *schema.Tx) []kv {
	var ret []kv
	for _, k := range tx.Entries {
		log.Printf("Fetching key %v @ %d", strconv.Quote(string(k.Key)), tx.Header.Id)
		entry, err := client.GetAt(ctx, k.Key, tx.Header.Id)
		if err != nil {
			log.Printf("Error: %s", err.Error())
			continue
		}
		ret = append(ret, kv{Key: strconv.Quote(string(k.Key)), Value: strconv.Quote(string(entry.Value))})
	}
	return ret
}
