package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"strings"

	"github.com/urfave/cli/v2"

	"github.com/bluesky-social/indigo/xrpc"

	"bsky.watch/utils/pagination"
	"bsky.watch/utils/xrpcauth"
)

func main() {
	app := &cli.App{
		Flags: []cli.Flag{
			&cli.StringFlag{
				Name:  "host",
				Usage: "API host name",
				Value: "bsky.social"},
			&cli.PathFlag{
				Name:  "auth-file",
				Usage: "path to the file with auth info"},
		},
		Commands: []*cli.Command{
			{
				Name:    "query",
				Aliases: []string{"get"},
				Action:  runGet,
				Flags: []cli.Flag{
					&cli.BoolFlag{
						Name:  "all",
						Usage: "automatically re-issue the request with a returned cursor to fetch all the results",
						Value: true},
				},
			},
			{
				Name:    "procedure",
				Aliases: []string{"post", "do"},
				Action:  runPost,
				Flags: []cli.Flag{
					&cli.PathFlag{
						Name:  "input-file",
						Usage: "read input from file instead of stdin",
					},
					&cli.BoolFlag{
						Name:  "no-input",
						Usage: "set to true to send empty request body",
					},
					&cli.StringFlag{
						Name:  "json",
						Usage: "raw json to send in the request body",
					},
				},
			},
		},
	}
	if err := app.Run(os.Args); err != nil {
		log.Fatal(err)
	}
}

func createClient(cCtx *cli.Context) *xrpc.Client {
	f := cCtx.Path("auth-file")
	var r *xrpc.Client
	if f == "" {
		r = xrpcauth.NewAnonymousClient(context.Background())
	} else {
		r = xrpcauth.NewClient(context.Background(), f)
	}
	host := cCtx.String("host")

	if !strings.HasPrefix(host, "http://") && !strings.HasPrefix(host, "https://") {
		host = "https://" + host
	}
	r.Host = host

	return r
}

func runGet(cCtx *cli.Context) error {
	method := cCtx.Args().First()

	params := map[string]interface{}{}

	for _, arg := range cCtx.Args().Tail() {
		parts := strings.SplitN(arg, "=", 2)
		if len(parts) != 2 {
			return fmt.Errorf("argument must be a =-separated key-value pair")
		}
		params[parts[0]] = parts[1]
	}

	client := createClient(cCtx)

	encoder := json.NewEncoder(os.Stdout)

	_, err := pagination.Collect(func(cursor string) (interface{}, string, error) {
		if cursor != "" {
			params["cursor"] = cursor
		}

		var resp interface{}
		err := client.Do(context.Background(), xrpc.Query, "", method, params, nil, &resp)
		if err != nil {
			return nil, "", err
		}

		if err := encoder.Encode(resp); err != nil {
			return nil, "", err
		}

		if !cCtx.Bool("all") {
			return nil, "", nil
		}

		cursor = ""
		if m, ok := resp.(map[string]interface{}); ok && m["cursor"] != nil {
			if s, ok := m["cursor"].(string); ok {
				cursor = s
			}
		}

		return nil, cursor, nil
	})
	return err
}

func runPost(cCtx *cli.Context) error {
	method := cCtx.Args().First()

	params := map[string]interface{}{}

	for _, arg := range cCtx.Args().Tail() {
		parts := strings.SplitN(arg, "=", 2)
		if len(parts) != 2 {
			return fmt.Errorf("argument must be a =-separated key-value pair")
		}
		params[parts[0]] = parts[1]
	}

	client := createClient(cCtx)

	var body interface{}
	if !cCtx.Bool("no-input") {
		switch {
		case cCtx.String("json") != "":
			body = strings.NewReader(cCtx.String("json"))
		case cCtx.Path("input-file") != "":
			f, err := os.Open(cCtx.Path("input-file"))
			if err != nil {
				return fmt.Errorf("opening %q: %w", cCtx.Path("input-file"), err)
			}
			defer f.Close()
			body = f
		default:
			body = os.Stdin
		}
	}

	resp := bytes.NewBuffer(nil)
	err := client.Do(context.Background(), xrpc.Procedure, "application/json", method, params, body, &resp)
	if err != nil {
		return err
	}

	_, err = os.Stdout.Write(resp.Bytes())
	return err
}
