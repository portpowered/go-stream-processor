package main

import (
	"context"
	"fmt"
	"github.com/portpowered/go-stream-processor/nodes"
	sp "github.com/portpowered/go-stream-processor/stream_processor"
	"log"
	"time"
)

func main() {
	source := nodes.SourceFunc(func(ctx context.Context, emit nodes.EmitFunc) error {
		for _, v := range []int{1, 2, 3} {
			if err := emit("", v); err != nil {
				return err
			}
		}
		return nil
	})
	pipeline, err := sp.NewGraphBuilder("example").
		AddSource("source", source).
		AddOperator("double", nodes.Map(func(v int) int { return v * 2 })).
		AddSink("print", nodes.SinkFunc(func(v int) error { fmt.Println(v); return nil })).
		Connect("source", "double").Connect("double", "print").Build()
	if err != nil {
		log.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	pipeline.Start(ctx)
	if err := pipeline.Wait(); err != nil {
		log.Fatal(err)
	}
}
