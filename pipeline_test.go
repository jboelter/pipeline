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

package pipeline_test

import (
	"fmt"
	"log"
	"os"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/jboelter/pipeline"
)

func TestNoGenerator(t *testing.T) {
	p := pipeline.New()

	err := p.Run()

	fmt.Println(err)

	if err != pipeline.ErrNilGenerator {
		t.Errorf(`the pipeline generator should be nil`)
	}

	err = p.Abort()
	if err != pipeline.ErrNilGenerator {
		t.Errorf(`the pipeline generator should be nil`)
	}
}

func TestNoGeneratorWithLogger(t *testing.T) {
	logger := log.New(os.Stdout, "", 0)
	cfg := pipeline.DefaultConfig()
	cfg.Logger, cfg.Verbose = logger, true
	p := pipeline.NewWithConfig(cfg)

	err := p.Run()
	if err != pipeline.ErrNilGenerator {
		t.Errorf(`the pipeline generator should be nil`)
	}

	err = p.Abort()
	if err != pipeline.ErrNilGenerator {
		t.Errorf(`the pipeline generator should be nil`)
	}
}

func TestNoStages(t *testing.T) {
	p := pipeline.New()
	generator := &EmptyGenerator{}
	p.SetGenerator(generator)

	err := p.Run()
	if err == nil {
		t.Errorf(`error should not be nil`)
	}

	if err != pipeline.ErrNoStages {
		t.Errorf(`the pipeline should have no stages`)
	}

	if generator.NextCount != 0 {
		t.Errorf("expected generator.NextCount == 0")
	}

	if generator.AbortCount != 0 {
		t.Errorf("expected generator.AbortCount == 0")
	}
}

func TestNoStagesWithLogger(t *testing.T) {
	logger := log.New(os.Stdout, "", 0)
	cfg := pipeline.DefaultConfig()
	cfg.Logger, cfg.Verbose = logger, true
	p := pipeline.NewWithConfig(cfg)

	generator := &EmptyGenerator{}
	p.SetGenerator(generator)

	err := p.Run()
	if err == nil {
		t.Errorf(`error should be nil`)
	}

	if err != pipeline.ErrNoStages {
		t.Errorf(`the pipeline should have no stages`)
	}

	if generator.NextCount != 0 {
		t.Errorf("expected generator.NextCount == 0")
	}

	if generator.AbortCount != 0 {
		t.Errorf("expected generator.AbortCount == 0")
	}
}

func TestOneStageNoJobs(t *testing.T) {
	p := pipeline.New()
	generator := &EmptyGenerator{}
	p.SetGenerator(generator)

	stage := &CountingStage{}
	p.AddStage(stage)

	err := p.Run()
	if err != nil {
		t.Errorf(`error should be nil`)
	}

	if generator.NextCount != 1 {
		t.Errorf("expected generator.NextCount == 1")
	}

	if generator.AbortCount != 0 {
		t.Errorf("expected generator.AbortCount == 0")
	}

	if stage.ProcessCount != 0 {
		t.Errorf("expected stage.ProcessCount == 0")
	}

	if p.Abort() != nil {
		t.Errorf("expected p.Abort() == nil")
	}

	if generator.NextCount != 1 {
		t.Errorf("expected generator.NextCount == 1")
	}

	if generator.AbortCount != 1 {
		t.Errorf("expected generator.AbortCount == 1")
	}

	if stage.ProcessCount != 0 {
		t.Errorf("expected stage.ProcessCount == 0")
	}
}

func TestOneStageAbortAfterOne(t *testing.T) {
	p := pipeline.New()
	generator := &AbortableGenerator{}
	generator.QuitChan = make(chan struct{})
	p.SetGenerator(generator)

	stage := &CountingStage{}
	p.AddStage(stage)

	go func() {
		if p.Abort() != nil {
			t.Errorf("expected p.Abort() == nil")
		}

	}()

	err := p.Run()
	if err != nil {
		t.Errorf(`error should be nil`)
	}

	if generator.NextCount != 2 {
		t.Errorf("expected generator.NextCount == 2")
	}

	if generator.AbortCount != 1 {
		t.Errorf("expected generator.AbortCount == 1")
	}

	if stage.ProcessCount != 1 {
		t.Errorf("expected stage.ProcessCount == 1")
	}
}

func TestOneStageCountToTen(t *testing.T) {
	p := pipeline.New()
	generator := &CountsToTenGenerator{}
	p.SetGenerator(generator)

	stage1 := &CountingStage{}

	stage2 := &ConcurrentStage{}
	stage2.Waiter = &sync.WaitGroup{}
	stage2.Waiter.Add(10)

	p.AddStage(stage1, stage2)

	err := p.Run()
	if err != nil {
		t.Errorf(`error should be nil`)
	}

	if generator.NextCount != 11 {
		t.Errorf("expected generator.NextCount == 11")
	}

	if stage1.ProcessCount != 10 {
		t.Errorf("expected stage1.ProcessCount == 10")
	}

	if stage2.ProcessCount != 10 {
		t.Errorf("expected stage2.ProcessCount == 10")
	}
}

func TestOneStageWithLogger(t *testing.T) {
	//boost our code coverage of logging output

	logger := log.New(os.Stdout, "", 0)
	cfg := pipeline.DefaultConfig()
	cfg.Logger, cfg.Verbose = logger, true
	p := pipeline.NewWithConfig(cfg)
	generator := &CountsToTenGenerator{}
	p.SetGenerator(generator)

	stage := &CountingStage{}
	p.AddStage(stage)

	err := p.Run()
	if err != nil {
		t.Errorf(`error should be nil`)
	}
	if generator.NextCount != 11 {
		t.Errorf("expected generator.NextCount == 11")
	}

	if stage.ProcessCount != 10 {
		t.Errorf("expected stage.ProcessCount == 10")
	}
}

func TestOneStageWithLoggerNoConcurrency(t *testing.T) {
	//boost our code coverage of logging output

	logger := log.New(os.Stdout, "", 0)
	cfg := pipeline.DefaultConfig()
	cfg.Logger, cfg.Verbose, cfg.NoConcurrency = logger, true, true
	p := pipeline.NewWithConfig(cfg)
	generator := &CountsToTenGenerator{}
	p.SetGenerator(generator)

	stage := &CountingStage{}
	p.AddStage(stage)

	err := p.Run()
	if err != nil {
		t.Errorf(`error should be nil`)
	}
	if generator.NextCount != 11 {
		t.Errorf("expected generator.NextCount == 11")
	}

	if stage.ProcessCount != 10 {
		t.Errorf("expected stage.ProcessCount == 10")
	}
}

func TestOneStageWithLoggerNoBuffer(t *testing.T) {
	//boost our code coverage of logging output

	logger := log.New(os.Stdout, "", 0)
	cfg := pipeline.DefaultConfig()
	cfg.Logger, cfg.Verbose, cfg.Buffered = logger, true, false
	p := pipeline.NewWithConfig(cfg)
	generator := &CountsToTenGenerator{}
	p.SetGenerator(generator)

	stage := &CountingStage{}
	p.AddStage(stage)

	err := p.Run()
	if err != nil {
		t.Errorf(`error should be nil`)
	}
	if generator.NextCount != 11 {
		t.Errorf("expected generator.NextCount == 11")
	}

	if stage.ProcessCount != 10 {
		t.Errorf("expected stage.ProcessCount == 10")
	}
}

/* test generator */
type EmptyGenerator struct {
	NextCount  int
	AbortCount int
}

func (g *EmptyGenerator) Name() string {
	return "EmptyGenerator"
}

func (g *EmptyGenerator) Next() interface{} {
	g.NextCount++
	return nil
}

func (g *EmptyGenerator) Abort() {
	g.AbortCount++
}

/* test generator */
type AbortableGenerator struct {
	NextCount  int
	AbortCount int
	QuitChan   chan struct{}
}

func (g *AbortableGenerator) Name() string {
	return "AbortableGenerator"
}

func (g *AbortableGenerator) Next() interface{} {
	g.NextCount++
	if g.NextCount == 1 {
		return g.NextCount
	}

	// block while waiting for an abort signal
	<-g.QuitChan
	return nil
}

func (g *AbortableGenerator) Abort() {
	g.AbortCount++
	close(g.QuitChan)
}

/* test generator */
type CountsToTenGenerator struct {
	NextCount int
}

func (g *CountsToTenGenerator) Name() string {
	return "CountsToTenGenerator"
}

func (g *CountsToTenGenerator) Next() interface{} {
	g.NextCount++
	if g.NextCount <= 10 {
		return g.NextCount
	}
	return nil
}

func (g *CountsToTenGenerator) Abort() {
}

/* test stage */
type CountingStage struct {
	ProcessCount int
}

func (s *CountingStage) Name() string {
	return "CountingStage"
}

func (s *CountingStage) Concurrency() int {
	return 1
}

func (s *CountingStage) Process(interface{}) {
	s.ProcessCount++
}

/* test stage */
type ConcurrentStage struct {
	ProcessCount int32
	Waiter       *sync.WaitGroup
}

func (s *ConcurrentStage) Name() string {
	return "ConcurrentStage"
}

func (s *ConcurrentStage) Concurrency() int {
	return 10
}

func (s *ConcurrentStage) Process(interface{}) {
	s.Waiter.Done() // decrement
	s.Waiter.Wait() // wait for everybody; assumes all goroutines got a job
	atomic.AddInt32(&s.ProcessCount, 1)
}
