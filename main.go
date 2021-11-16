package main

import (
	"context"
	"log"

	"encoding/json"
	"github.com/codenotary/immudb/pkg/api/schema"
	immuclient "github.com/codenotary/immudb/pkg/client"
	"google.golang.org/grpc/metadata"
	"immudumper/zfile"
	"flag"
)
var config struct {
        Address   string
        Port      int
        Username  string
        Password  string
        DBName    string
}

func init() {
        flag.StringVar(&config.Address, "address", "", "IP address of immudb server")
        flag.IntVar(&config.Port, "port", 3322, "Port number of immudb server")
        flag.StringVar(&config.Username, "user", "immudb", "Username for authenticating to immudb")
        flag.StringVar(&config.Password, "pass", "immudb", "Password for authenticating to immudb")
        flag.StringVar(&config.DBName, "db", "defaultdb", "Name of the database to use")
        flag.Parse()
}

func connect() (immuclient.ImmuClient, context.Context) {
        opts := immuclient.DefaultOptions().WithAddress(config.Address).WithPort(config.Port)
	client, err := immuclient.NewImmuClient(opts)
	if err != nil {
		log.Fatal(err)
	}
	ctx := context.Background()
	lr, err := client.Login(ctx, []byte(config.Username), []byte(config.Password))

	if err != nil {
		log.Fatal(err)
	}
	md := metadata.Pairs("authorization", lr.Token)
	ctx = metadata.NewOutgoingContext(ctx, md)
	udr, err := client.UseDatabase(ctx, &schema.Database{DatabaseName: config.DBName})
	if err != nil {
		log.Fatal(err)
	}
	// Recollect the token that we get when using/switching the database.
	md = metadata.Pairs("authorization", udr.Token)
	ctx = metadata.NewOutgoingContext(ctx, md)

	return client, ctx
}

func getSize(client immuclient.ImmuClient, ctx context.Context) uint64 {
	s, err := client.CurrentState(ctx)
	if err != nil {
		log.Fatal(err)
	}
	log.Print(s)
	return s.TxId
}

func main() {
	client, ctx := connect()
	var i uint64
	size := getSize(client, ctx)
	f_out := zfile.CreateZFile("dump.json.gz")
	defer f_out.Close()
	f_out.WriteString("[\n")
	for i = 1; i <= size; i++ {
		if i > 1 {
			f_out.WriteString(",\n")
		}
		tx, err := client.TxByID(ctx, i)
		if err != nil {
			log.Fatal(err)
		}
		s := buildStruct(tx)
		j, err := json.Marshal(s)
		if err != nil {
			log.Fatal(err)
		}
		f_out.Write(j)
	}
	f_out.WriteString("\n]\n")
}
