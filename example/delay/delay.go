package delay

import (
	"math/rand"
	"time"

	"github.com/jboelter/pipeline/example/job"

	"log"
)

type Delay struct {
	log *log.Logger
}

var Stage = &Delay{}

func (s *Delay) SetLogger(l *log.Logger) {
	s.log = l
}

func (s *Delay) Name() string {
	return "Delay"
}

func (s *Delay) Concurrency() int {
	return 8
}

func (s *Delay) Process(i interface{}) {

	j := i.(*job.Job)

	// a dummy step that delays...
	d := rand.Intn(100)
	time.Sleep(time.Millisecond * time.Duration(d))

	s.log.Println("[DELAY]   ", j.Id, d, "ms")
}
