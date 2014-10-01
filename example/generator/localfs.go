package generator

import (
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
		g.logger.Println("[FSGenerator] aborted the generator")
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
	g.logger.Println("[FSGenerator] aborting the generator")
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
	g.logger.Println("[FSGenerator] starting the generator")

	defer close(g.jobs)

	filepath.Walk(g.path, func(p string, f os.FileInfo, err error) error {
		if !f.IsDir() {
			matched, err := regexp.MatchString(g.fileregex, f.Name())
			if err != nil {
				panic(err)
			}
			if matched {
				g.logger.Printf("[FSGenerator] found file %v\n", p+"/"+f.Name())
				g.jobs <- job.New(p + "/" + f.Name())
			}
		}
		return nil
	})
}
