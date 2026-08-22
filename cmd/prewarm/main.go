// Command prewarm pulls the suite's pinned execution images into the
// sandman containerd namespace, so the first container runs in CI pay no
// pull latency. It is a CI helper, not part of the sandman product.
package main

import (
	"context"
	"fmt"
	"os"

	containerd "github.com/containerd/containerd/v2/client"
)

func main() {
	ctx := context.Background()
	cli, err := containerd.New("/run/containerd/containerd.sock",
		containerd.WithDefaultNamespace("sandman"))
	if err != nil {
		fmt.Fprintln(os.Stderr, "prewarm: connect:", err)
		os.Exit(1)
	}
	defer cli.Close()
	for _, ref := range []string{"alpine:3.21", "python:3.12-alpine"} {
		fmt.Printf("prewarm: pulling %s\n", ref)
		if _, err := cli.Pull(ctx, ref, containerd.WithPullUnpack); err != nil {
			fmt.Fprintln(os.Stderr, "prewarm:", ref, err)
			os.Exit(1)
		}
	}
}
