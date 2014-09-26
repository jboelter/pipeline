# pipeline #

Package pipeline allows a 'job' to be processed through a sequence of stages.
The stages can be developed and unit tested independent of each other and
composed together into different processing flows.

## Overview ##

A more detailed example is provided in the /examples directory.  At it's core, the pipeline hides the
complexity of managing the channels necessary to move a job through a series of stages for processing.


	p := pipeline.New()

	p.AddGenerator(work.Generator)

	p.AddStage(fetch.Stage)
	p.AddStage(parse.Stage)
	p.AddStage(store.Stage)
	p.AddStage(log.Stage)

	p.Run()

The Next() function on the generator is called by the pipeline to retrieve a new job to pass through the pipeline.

	type Generator interface {
		Name() string
		Next() interface{}
		Abort()
	}

The Process() function on the Stage is called by the pipeline for each job. A job is never processed by concurrent
stages.  However, each stage may be instantiated in a number of goroutines defined by Concurrency().

	type Stage interface {
		Name() string
		Concurrency() int
		Process(interface{})
	}


## Installation ##

```
	go get github.com/jboelter/pipeline
```

## Documentation ##

[http://godoc.org/github.com/jboelter/pipeline](http://godoc.org/github.com/jboelter/pipeline)
