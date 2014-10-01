package main

import (
	"time"

	"github.com/jboelter/pipeline"

	"github.com/jboelter/pipeline/example/delay"
	"github.com/jboelter/pipeline/example/echo"
	"github.com/jboelter/pipeline/example/generator"
	"github.com/jboelter/pipeline/example/terminus"

	"log"
	"os"
	"os/signal"
)

func main() {

	logger := log.New(os.Stdout, "", 0)

	logger = log.New(os.Stdout, "", 0)
	cfg := pipeline.DefaultConfig()
	cfg.Logger = logger
	// for debugging/understanding -- eliminate concurrency and buffering
	cfg.NoConcurrency = true
	cfg.Depth = 0
	p := pipeline.NewWithConfig(cfg)

	generator.Generator.Initialize(os.Getenv("GOPATH"), `.*\.go$`, logger)
	p.AddGenerator(generator.Generator)

	delay.Stage.SetLogger(logger)
	p.AddStage(delay.Stage)

	echo.Stage.SetLogger(logger)
	p.AddStage(echo.Stage)

	terminus.Stage.SetLogger(logger)
	p.AddStage(terminus.Stage)

	// example; stop the generator after 1 second
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
