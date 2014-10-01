package pipeline_test

import (
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

	if err.Error() != `[PIPELINE] The generator cannot be nil` {
		t.Errorf(`[PIPELINE] The generator cannot be nil`)
	}

	err = p.Abort()
	if err.Error() != `[PIPELINE] The generator cannot be nil` {
		t.Errorf(`[PIPELINE] The generator cannot be nil`)
	}
}

func TestNoGeneratorWithLogger(t *testing.T) {
	logger := log.New(os.Stdout, "", 0)
	cfg := pipeline.DefaultConfig()
	cfg.Logger, cfg.Verbose = logger, true
	p := pipeline.NewWithConfig(cfg)

	err := p.Run()
	if err.Error() != `[PIPELINE] The generator cannot be nil` {
		t.Errorf(`[PIPELINE] The generator cannot be nil`)
	}

	err = p.Abort()
	if err.Error() != `[PIPELINE] The generator cannot be nil` {
		t.Errorf(`[PIPELINE] The generator cannot be nil`)
	}
}

func TestNoStages(t *testing.T) {
	p := pipeline.New()
	generator := &EmptyGenerator{}
	p.AddGenerator(generator)

	err := p.Run()
	if err == nil {
		t.Errorf(`error should not be nil`)
	}

	if err.Error() != `[PIPELINE] There are no stages defined` {
		t.Errorf(`[PIPELINE] There are no stages defined`)
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
	p.AddGenerator(generator)

	err := p.Run()
	if err == nil {
		t.Errorf(`error should be nil`)
	}

	if err.Error() != `[PIPELINE] There are no stages defined` {
		t.Errorf(`[PIPELINE] There are no stages defined`)
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
	p.AddGenerator(generator)

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

	p.Abort()

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
	p.AddGenerator(generator)

	stage := &CountingStage{}
	p.AddStage(stage)

	go func() {
		p.Abort()
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
	p.AddGenerator(generator)

	stage1 := &CountingStage{}
	p.AddStage(stage1)

	stage2 := &ConcurrentStage{}
	stage2.Waiter = &sync.WaitGroup{}
	stage2.Waiter.Add(10)
	p.AddStage(stage2)

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
	p.AddGenerator(generator)

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
	} else {
		// block while waiting for an abort signal
		<-g.QuitChan
		return nil
	}
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
