package pipeline

import (
	"errors"
	"log"
	"sync"
)

// Generator defines an interace that creates 'jobs' to be processed by the pipeline
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
	generator Generator
	stages    []Stage
	channels  []chan interface{}
	config    PipelineConfig
}

type PipelineConfig struct {
	Logger        *log.Logger
	Buffered      bool
	Depth         int
	NoConcurrency bool
	Verbose       bool
}

func DefaultConfig() PipelineConfig {
	var cfg PipelineConfig
	cfg.Buffered = true
	cfg.Depth = 10
	return cfg
}

func NewWithConfig(cfg PipelineConfig) *Pipeline {
	return &Pipeline{
		config: cfg,
	}
}

func New() *Pipeline {
	return &Pipeline{}
}

func (p *Pipeline) Abort() error {

	if p.generator == nil {
		if p.config.Logger != nil {
			p.config.Logger.Println("[PIPELINE] The generator cannot be nil")
		}
		return errors.New("[PIPELINE] The generator cannot be nil")
	}
	p.generator.Abort()
	return nil
}

/*
This call will block until the pipeline has completed.
*/
func (p *Pipeline) Run() error {

	if p.generator == nil {
		if p.config.Logger != nil {
			p.config.Logger.Println("[PIPELINE] The generator cannot be nil")
		}
		return errors.New("[PIPELINE] The generator cannot be nil")
	}

	if len(p.stages) == 0 {
		if p.config.Logger != nil {
			p.config.Logger.Println("[PIPELINE] There are no stages defined")
		}
		return errors.New("[PIPELINE] There are no stages defined")
	}

	if p.config.Logger != nil {
		p.config.Logger.Printf("[PIPELINE] ----------------------------------------\n")
		p.config.Logger.Printf("[PIPELINE] Pipeline\n")
		p.config.Logger.Printf("[PIPELINE] Generator: %v\n", p.generator.Name())
		for _, s := range p.stages {
			p.config.Logger.Printf("[PIPELINE] Stage: %v x %v\n", s.Name(), p.concurrency(s))
		}
		p.config.Logger.Printf("[PIPELINE] ----------------------------------------\n")
	}

	if p.config.Logger != nil && p.config.Verbose {
		p.config.Logger.Printf("[PIPELINE] launching the generator %v\n", p.generator.Name())
	}

	go func() {
		defer close(p.channels[0])
		for {
			job := p.generator.Next()
			if job != nil {
				if p.config.Logger != nil && p.config.Verbose {
					p.config.Logger.Printf("[PIPELINE] generator is enqueing %+v\n", job)
				}
				p.channels[0] <- job
			} else {
				if p.config.Logger != nil && p.config.Verbose {
					p.config.Logger.Println("[PIPELINE] The generator is done")
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
			p.config.Logger.Printf("[PIPELINE] go stage %v with concurrency %v\n", s.Name(), p.concurrency(s))
		}

		wg := &sync.WaitGroup{}
		// for id := 0; id < 1; id++ { // if you want 1 goroutine each; helps w/ debugging
		for id := 0; id < p.concurrency(s); id++ {
			wg.Add(1)
			go stage(p.channels[idx], p.channels[idx+1], id, wg, s, p.config.Logger, p.config.Verbose)
		}
	}

	// drain the last channel
	for _ = range p.channels[len(p.channels)-1] {
	}

	return nil
}

func (p *Pipeline) AddGenerator(generator Generator) {
	c := make(chan interface{}, 1)
	p.channels = append(p.channels, c)
	p.generator = generator
}

/*
	Add a stage to the pipeline.  Jobs are passed through the
	stages are executed in the order they are added.
*/
func (p *Pipeline) AddStage(s Stage) {
	// creates the next channel in the list
	// reads from the upstream channel
	// writes to the channel created here

	var c chan interface{}

	if p.config.Buffered {
		c = make(chan interface{}, p.concurrency(s)*p.config.Depth)
	} else {
		c = make(chan interface{})
	}

	p.channels = append(p.channels, c)
	p.stages = append(p.stages, s)
}

func stage(in chan interface{}, out chan interface{}, id int, wg *sync.WaitGroup, s Stage, logger *log.Logger, verbose bool) {

	// a channel can only be closed once; let goroutine[0] close it; but only after all the goroutines have exited
	if id == 0 {
		defer func() {
			if logger != nil && verbose {
				logger.Printf("[PIPELINE] %v:%v doing wg.Wait()\n", s.Name(), id)
			}
			wg.Wait()
			if logger != nil && verbose {
				logger.Printf("[PIPELINE] %v:%v closing out channel\n", s.Name(), id)
			}
			close(out)
		}()
	}

	// defer the waitgroup notification
	defer func() {
		if logger != nil && verbose {
			logger.Printf("[PIPELINE] %v:%v doing wg.Done()\n", s.Name(), id)
		}
		wg.Done()
	}()

	if logger != nil {
		logger.Printf("[PIPELINE] %v:%v stage is ready for work\n", s.Name(), id)
	}

	for job := range in {
		if logger != nil && verbose {
			logger.Printf("[PIPELINE] %v:%v processing\n", s.Name(), id)
		}

		s.Process(job)

		// send it to the next stage
		out <- job
	}

	if logger != nil {
		logger.Printf("[PIPELINE] %v:%v stage has no more work\n", s.Name(), id)
	}
}

func (p *Pipeline) concurrency(s Stage) int {
	if p.config.NoConcurrency {
		return 1
	} else {
		return s.Concurrency()
	}
}
