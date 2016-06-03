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

package terminus

import (
	"log"

	"github.com/jboelter/pipeline/example/job"
)

type Terminus struct {
	log *log.Logger
}

var Stage = &Terminus{}

func (s *Terminus) SetLogger(l *log.Logger) {
	s.log = l
}

func (s *Terminus) Name() string {
	return "Terminus"
}

func (s *Terminus) Concurrency() int {
	return 1
}

func (s *Terminus) Process(i interface{}) {

	j := i.(*job.Job)

	if j.Err != nil {
		s.log.Printf("source=stage, name=terminus, id=%v, result=%v", j.Id, j.Err)
	} else {
		s.log.Printf("source=stage, name=terminus, id=%v, result=success", j.Id)
	}
}
