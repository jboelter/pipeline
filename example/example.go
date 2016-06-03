// The MIT License (MIT)
//
// Copyright (c) 2014-2016 Joshua Boelter
//
// Permission is hereby granted, free of charge, to any person obtaining a copy
// of this software and associated documentation files (the "Software"), to deal
// in the Software without restriction, including without limitation the rights
// to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
// copies of the Software, and to permit persons to whom the Software is
// furnished to do so, subject to the following conditions:
//
// The above copyright notice and this permission notice shall be included in all
// copies or substantial portions of the Software.
//
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
// IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
// FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
// AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
// LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
// OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
// SOFTWARE.
package main

import (
	"flag"
	"time"

	"github.com/jboelter/pipeline"

	"github.com/jboelter/pipeline/example/delay"
	"github.com/jboelter/pipeline/example/echo"
	"github.com/jboelter/pipeline/example/generator"
	"github.com/jboelter/pipeline/example/hash"
	"github.com/jboelter/pipeline/example/terminus"

	"log"
	"os"
	"os/signal"
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
