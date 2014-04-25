/*
Package pipeline allows a 'job' to be processed through a sequence of stages.
The stages can be developed and unit tested independent of each other and
composed together into different processing flows.

Basics

A typical pipeline may look like the following. A user defined generator creates a job
that is moved through the pipeline. A job is a user defined type with no restrictions.

	p := pipeline.New()

	p.AddGenerator(work.Generator)

	p.AddStage(fetch.Stage)
	p.AddStage(parse.Stage)
	p.AddStage(store.Stage)
	p.AddStage(log.Stage)

	p.Run()

The pipeline retrieves a job by calling the Next() function on the Generator.  The
job is then moved sequentially through the pipeline to the terminal stage. The Run()
function blocks until there is no more work to do, as signaled by the Generator
returning a nil value from Next(). The Generator and each Stage may reside in
their own packages and must implement the pipeline.Generator and pipeline.Stage
interfaces respectively.


How it works

The pipeline creates and manages the channels used internally for moving work
between stages. The pipeline does not monitor the stages or jobs for error states,
panics or other failures.  By design, the pipeline does not expose the channels to the
developer.

A job (a user defined structure) is retrieved from the Generator Next() call (as an
interface{}) which is then passed to each stage via the Process() call.

Types

The Generator interface requires a Next() function that is called to retreive the
next job. Returning a nil value will shutdown the pipeline, flushing all jobs
in the process.

	type Generator interface {
		Name() string
		Next() interface{}
		Abort()
	}

The Stage interface specifies the concurrency of the stage the determines how many
goroutines are launched for this stage. The Process() function is called on each
job as the job moves through the pipeline.  A given job (represented by the interface{})
will never be operated on concurrently by multiple stages in the pipeline.

	type Stage interface {
		Name() string
		Concurrency() int
		Process(interface{})
	}

Generator

A typical generator design uses an internal channel to pass work to the Next()
function. User must implement the function to fill the Generator.jobs. See
examples for more details and patterns including generators that can run forever
or be aborted. The Generator may further have an Initialize(), or New() function that can
be called to setup the generator.

	package mygenerator

	type MyGenerator {
		jobs    chan *job.Job
		// other local state goes here
	}

	// the generator instance; passed to the pipeline
	// p.AddGenerator(mygenerator.Generator)
	var Generator = &MyGenerator{}

	// Name() required by the Generator interface
	func (g *MyGenerator) Name() string {
		return "MyGenerator"
	}

	// Next() required by the Generator interface
	// Called by the pipeline to get the next job to process in the pipeline.
	// Only called sequentially.
	func (g *LocalGenerator) Next() interface{} {
		j, ok := <-g.jobs

		if ok {
			// return the job if there was one
			return j
		} else {
			// no more jobs
			return nil
		}
	}

Stage

A typical stage is implemented as its own package to allow for ease of testing
and encourage isolation of stages.

	package mystage

	import (
		"myjobtype/job"
		"log"
	)

	type MyStage struct {
	}

	var Stage = &MyStage{}

	func (s *MyStage) Name() string {
		return "MyStage"
	}

	func (s *MyStage) Concurrency() int {
		return 1
	}

	func (s *MyStage) Process(i interface{}) {

		j := i.(*job.Job)

		// do something in the stage
		// any errors would be conveyed in the job object to the next stage

		// j.Err = errors.New(...)
	}



*/
package pipeline
