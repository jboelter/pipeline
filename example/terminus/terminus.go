package terminus

import (
	"github.com/jboelter/pipeline/example/job"
	"log"
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
		s.log.Println("[TERMINUS]", j.Id, " ERROR", j.Err)
	} else {
		s.log.Println("[TERMINUS]", j.Id, " SUCCESS")
	}
}
