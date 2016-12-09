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

package generator

import (
	"errors"
	"log"
	"os"
	"path/filepath"
	"regexp"
	"sync/atomic"

	"github.com/jboelter/pipeline/example/job"
)

type FsGenerator struct {
	jobs      chan *job.Job
	path      string
	fileregex string
	quit      int32
	logger    *log.Logger
}

var Generator = &FsGenerator{}

func (g *FsGenerator) Name() string {
	return "FsGenerator"
}

func (g *FsGenerator) Next() interface{} {

	q := atomic.LoadInt32(&g.quit)
	if q > 0 {
		g.logger.Printf("source=generator, name=fsgenerator, action=aborting")
		return nil
	}

	// return the next job
	j, ok := <-g.jobs

	if ok {
		return j
	}
	return nil
}

func (g *FsGenerator) Abort() {
	g.logger.Printf("source=generator, name=fsgenerator, action=signal_abort")
	atomic.StoreInt32(&g.quit, 1)
}

func (g *FsGenerator) Initialize(path string, fileregex string, l *log.Logger) {
	g.logger = l
	g.fileregex = fileregex
	g.path = path

	g.jobs = make(chan *job.Job, 10) // can be a deeper queue if the time to generate jobs jitters

	go g.generate()
}

func (g *FsGenerator) generate() {
	g.logger.Printf("source=generator, name=fsgenerator, action=starting")

	defer close(g.jobs)

	filepath.Walk(g.path, func(p string, f os.FileInfo, err error) error {
		q := atomic.LoadInt32(&g.quit)
		if q > 0 {
			g.logger.Printf("source=generator, name=fsgenerator, action=abort_walk")
			// return an error to terminate the walk function
			return errors.New("source=generator, name=fsgenerator, action=abort_walk")
		}

		if !f.IsDir() {
			matched, err := regexp.MatchString(g.fileregex, f.Name())
			if err != nil {
				panic(err)
			}
			if matched {
				j := job.New(p)
				g.logger.Printf("source=generator, name=fsgenerator, action=generate, id=%v, file='%v'", j.ID, j.Path)
				g.jobs <- j
			}
		}
		return nil
	})
}
