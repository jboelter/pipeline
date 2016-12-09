// The MIT License (MIT)
//
// Copyright (c) 2014 Joshua Boelter
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

package pipeline

import (
	"errors"
	"log"
	"sync"
)

// ErrNilGenerator is returned when the pipeline has a nil generator.
// Set a generator by calling SetGenerator.
var ErrNilGenerator = errors.New("pipeline: the generator cannot be nil")

// ErrNoStages is returned when the pipeline has no stages.  Call AddStage
// to add one more more stages to the pipeline.
var ErrNoStages = errors.New("pipeline: there are no stages defined")

// Generator defines an interface that creates 'jobs' to be processed by the pipeline
type Generator interface {
	Name() string
	Next() interface{}
	Abort()
}

// Stage defines a stage in the pipeline
type Stage interface {
	Name() string
	Concurrency() int
	Process(interface{})
}

// Pipeline defines the container for the generator and stages
type Pipeline struct {
	_         struct{}
	generator Generator
	stages    []Stage
	channels  []chan interface{}
	config    Config
}

// Config defines the configuration for a Pipeline
type Config struct {
	_             struct{}
	Logger        *log.Logger
	Depth         int
	Buffered      bool
	NoConcurrency bool
	Verbose       bool
}

// DefaultConfig provides a default configuration with buffering
func DefaultConfig() Config {
	return Config{
		Buffered: true,
		Depth:    10,
	}
}

// NewWithConfig creates a new Pipeline with the provided configuration
func NewWithConfig(cfg Config) *Pipeline {
	return &Pipeline{
		config: cfg,
	}
}

// New creates a new Pipeline with the default configuration
func New() *Pipeline {
	return &Pipeline{
		config: DefaultConfig(),
	}
}

// Abort gracefully terminates a Pipeline by calling Abort on the generator
func (p *Pipeline) Abort() error {

	if p.generator == nil {
		if p.config.Logger != nil {
			p.config.Logger.Println("source=pipeline, error=generator cannot be nil") //TODO print the error itself
		}
		return ErrNilGenerator
	}
	p.generator.Abort()
	return nil
}

// Run will pull work from the generator and pass it through the pipeline. This
// call will block until the pipeline has completed.
func (p *Pipeline) Run() error {

	if p.generator == nil {
		if p.config.Logger != nil {
			p.config.Logger.Println("source=pipeline, error=generator cannot be nil")
		}
		return ErrNilGenerator
	}

	if len(p.stages) == 0 {
		if p.config.Logger != nil {
			p.config.Logger.Println("source=pipeline, error=there are no stages defined")
		}
		return ErrNoStages
	}

	if p.config.Logger != nil {
		p.config.Logger.Printf("source=pipeline, notice=config, generator=%v, buffered=%v, concurrency=%v, verbose=%v\n", p.generator.Name(), p.config.Buffered, !p.config.NoConcurrency, p.config.Verbose)
		for _, s := range p.stages {
			p.config.Logger.Printf("source=pipeline, notice=config, stage=%v, concurrency=%v\n", s.Name(), p.concurrency(s))
		}
	}

	if p.config.Logger != nil && p.config.Verbose {
		p.config.Logger.Printf("source=pipeline, action=starting")
	}

	go func() {
		defer close(p.channels[0])
		for {
			job := p.generator.Next()
			if job != nil {
				// if p.config.Logger != nil && p.config.Verbose {
				// 	p.config.Logger.Printf("source=pipeline, action=enqueing, job=%+v\n", job)
				// }
				p.channels[0] <- job
			} else {
				if p.config.Logger != nil && p.config.Verbose {
					p.config.Logger.Println("source=pipeline, action=closing")
				}
				break // will execute the deferred close of the channel
			}
		}
	}()

	// launch all the stages
	// read from the previous stage
	// write to the next
	for idx, s := range p.stages {
		if p.config.Logger != nil && p.config.Verbose {
			p.config.Logger.Printf("source=pipeline, action=launching, stage=%v, concurrency=%v\n", s.Name(), p.concurrency(s))
		}

		wg := &sync.WaitGroup{}
		// set cfg.NoConcurrency = true if you want 1 goroutine each; helps w/ debugging
		for id := 0; id < p.concurrency(s); id++ {
			wg.Add(1)
			go stage(p.channels[idx], p.channels[idx+1], id, wg, s, p.config.Logger, p.config.Verbose)
		}
	}

	// drain the last channel
	for range p.channels[len(p.channels)-1] {
	}

	if p.config.Logger != nil && p.config.Verbose {
		p.config.Logger.Println("source=pipeline, action=terminating")
	}

	return nil
}

// SetGenerator sets the generator for the Pipeline
func (p *Pipeline) SetGenerator(generator Generator) {
	c := make(chan interface{}, 1)
	p.channels = append(p.channels, c)
	p.generator = generator
}

// AddStage adds 1 or more stages to the pipeline.  Jobs are passed through the
// stages in the order they are added.
func (p *Pipeline) AddStage(stages ...Stage) {
	// creates the next channel in the list
	// reads from the upstream channel
	// writes to the channel created here

	for _, s := range stages {
		var c chan interface{}

		if p.config.Buffered {
			c = make(chan interface{}, p.concurrency(s)*p.config.Depth)
		} else {
			c = make(chan interface{})
		}

		p.channels = append(p.channels, c)
		p.stages = append(p.stages, s)
	}
}

func stage(in chan interface{}, out chan interface{}, id int, wg *sync.WaitGroup, s Stage, logger *log.Logger, verbose bool) {

	// a channel can only be closed once; let goroutine[0] close it; but only after all the goroutines have exited
	if id == 0 {
		defer func() {
			if logger != nil && verbose {
				logger.Printf("source=pipeline, stage=%v:%v, action=wait\n", s.Name(), id)
			}
			wg.Wait()
			if logger != nil && verbose {
				logger.Printf("source=pipeline, stage=%v:%v, action=closing channel\n", s.Name(), id)
			}
			close(out)
		}()
	}

	// defer the waitgroup notification
	defer func() {
		if logger != nil && verbose {
			logger.Printf("source=pipeline, stage=%v:%v, action=done\n", s.Name(), id)
		}
		wg.Done()
	}()

	if logger != nil {
		logger.Printf("source=pipeline, stage=%v:%v, action=ready\n", s.Name(), id)
	}

	for job := range in {
		if logger != nil && verbose {
			logger.Printf("source=pipeline, stage=%v:%v, action=processing\n", s.Name(), id)
		}

		s.Process(job)

		// send it to the next stage
		out <- job
	}
}

func (p *Pipeline) concurrency(s Stage) int {
	if p.config.NoConcurrency {
		return 1
	}
	return s.Concurrency()
}
