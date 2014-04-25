package echo

import (
	"github.com/joshuaboelter/pipeline/example/job"

	"log"
)

type Echo struct {
	log *log.Logger
}

var Stage = &Echo{}

func (s *Echo) SetLogger(l *log.Logger) {
	s.log = l
}

func (s *Echo) Name() string {
	return "Echo"
}

func (s *Echo) Concurrency() int {
	return 3
}

func (s *Echo) Process(i interface{}) {

	j := i.(*job.Job)

	s.log.Println("[ECHO]    ", j.Id, j.Path)
}
