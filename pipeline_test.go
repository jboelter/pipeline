package pipeline_test

import (
	"log"
	"os"
	"sync"
	"sync/atomic"
	"testing"

	. "gopkg.in/check.v1"

	"github.com/joshuaboelter/pipeline"
)

func Test(t *testing.T) { TestingT(t) }

type TestSuite struct{}

var _ = Suite(&TestSuite{})

func (s *TestSuite) TestNoGenerator(c *C) {
	p := pipeline.New()

	err := p.Run()

	c.Assert(err, ErrorMatches, `\[PIPELINE\] The generator cannot be nil`)

	err = p.Abort()
	c.Assert(err, ErrorMatches, `\[PIPELINE\] The generator cannot be nil`)
}

func (s *TestSuite) TestNoGeneratorWithLogger(c *C) {
	logger := log.New(os.Stdout, "", 0)
	p := pipeline.NewWithLogger(logger, true)

	err := p.Run()

	c.Assert(err, ErrorMatches, `\[PIPELINE\] The generator cannot be nil`)

	err = p.Abort()
	c.Assert(err, ErrorMatches, `\[PIPELINE\] The generator cannot be nil`)
}

func (s *TestSuite) TestNoStages(c *C) {
	p := pipeline.New()
	generator := &EmptyGenerator{}
	p.AddGenerator(generator)

	err := p.Run()
	c.Assert(err, ErrorMatches, `\[PIPELINE\] There are no stages defined`)

	c.Assert(generator.NextCount, Equals, 0)
	c.Assert(generator.AbortCount, Equals, 0)
}

func (s *TestSuite) TestNoStagesWithLogger(c *C) {
	logger := log.New(os.Stdout, "", 0)
	p := pipeline.NewWithLogger(logger, true)

	generator := &EmptyGenerator{}
	p.AddGenerator(generator)

	err := p.Run()
	c.Assert(err, ErrorMatches, `\[PIPELINE\] There are no stages defined`)

	c.Assert(generator.NextCount, Equals, 0)
	c.Assert(generator.AbortCount, Equals, 0)
}

func (s *TestSuite) TestOneStageNoJobs(c *C) {
	p := pipeline.New()
	generator := &EmptyGenerator{}
	p.AddGenerator(generator)

	stage := &CountingStage{}
	p.AddStage(stage)

	err := p.Run()
	c.Assert(err, Equals, nil)

	c.Assert(generator.NextCount, Equals, 1)
	c.Assert(generator.AbortCount, Equals, 0)

	c.Assert(stage.ProcessCount, Equals, 0)

	p.Abort()

	c.Assert(generator.NextCount, Equals, 1)
	c.Assert(generator.AbortCount, Equals, 1)

	c.Assert(stage.ProcessCount, Equals, 0)
}

func (s *TestSuite) TestOneStageAbortAfterOne(c *C) {
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
	c.Assert(err, Equals, nil)

	c.Assert(generator.NextCount, Equals, 2)
	c.Assert(generator.AbortCount, Equals, 1)
	c.Assert(stage.ProcessCount, Equals, 1)
}

func (s *TestSuite) TestOneStageCountToTen(c *C) {
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
	c.Assert(err, Equals, nil)

	c.Assert(generator.NextCount, Equals, 11)
	c.Assert(stage1.ProcessCount, Equals, 10)
	c.Assert(stage2.ProcessCount, Equals, int32(10))
}

func (s *TestSuite) TestOneStageWithLogger(c *C) {
	//boost our code coverage of logging output

	logger := log.New(os.Stdout, "", 0)
	p := pipeline.NewWithLogger(logger, true)
	generator := &CountsToTenGenerator{}
	p.AddGenerator(generator)

	stage := &CountingStage{}
	p.AddStage(stage)

	err := p.Run()
	c.Assert(err, Equals, nil)

	c.Assert(generator.NextCount, Equals, 11)

	c.Assert(stage.ProcessCount, Equals, 10)
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
