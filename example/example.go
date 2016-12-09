package main

import (
	"flag"
	"log"
	"os"
	"os/signal"
	"time"

	"github.com/jboelter/pipeline"
	"github.com/jboelter/pipeline/example/delay"
	"github.com/jboelter/pipeline/example/echo"
	"github.com/jboelter/pipeline/example/generator"
	"github.com/jboelter/pipeline/example/hash"
	"github.com/jboelter/pipeline/example/terminus"
)

var flagBuffer bool
var flagConcurrent bool
var flagVerbose bool

func init() {
	flag.BoolVar(&flagConcurrent, "concurrent", true, "set the pipeline concurrency")
	flag.BoolVar(&flagBuffer, "buffer", true, "set the pipeline buffering")
	flag.BoolVar(&flagVerbose, "verbose", false, "enable verbose logging")

	flag.Parse()
}

func main() {

	logger := log.New(os.Stdout, "", 0)
	cfg := pipeline.DefaultConfig()
	cfg.Logger = logger
	// for debugging/understanding -- eliminate concurrency and buffering
	cfg.NoConcurrency = !flagConcurrent
	cfg.Buffered = flagBuffer
	cfg.Depth = 0
	cfg.Verbose = flagVerbose
	p := pipeline.NewWithConfig(cfg)

	generator.Generator.Initialize(os.Getenv("GOPATH"), `.*\.go$`, logger)
	p.SetGenerator(generator.Generator)

	hash.Stage.SetLogger(logger)
	p.AddStage(hash.Stage)

	delay.Stage.SetLogger(logger)
	p.AddStage(delay.Stage)

	echo.Stage.SetLogger(logger)
	p.AddStage(echo.Stage)

	terminus.Stage.SetLogger(logger)
	p.AddStage(terminus.Stage)

	//example; stop the generator after 5 seconds
	time.AfterFunc(time.Second*5, func() {
		p.Abort()
	})

	// or stop the generator on Ctrl+C
	c := make(chan os.Signal, 1)
	signal.Notify(c, os.Interrupt)
	go func() {
		for sig := range c {
			sig.Signal()
			p.Abort()
		}
	}()

	// blocks until complete
	p.Run()
}
