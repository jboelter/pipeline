package generator

import (
	"io/ioutil"
	"log"
	"regexp"

	"github.com/jboelter/pipeline/example/job"
)

type FsGenerator struct {
	jobs      chan *job.Job
	path      string
	fileregex string
	log       *log.Logger
}

var Generator = &FsGenerator{}

func (g *FsGenerator) Name() string {
	return "FsGenerator"
}

func (g *FsGenerator) Next() interface{} {

	j, ok := <-g.jobs

	if ok {
		return j

	} else {
		return nil
	}
}

func (g *FsGenerator) Abort() {

	g.log.Println("Ignoring abort request")

	// called from anywhere that knows it can/should stop feeding the pipeline
	//	close(g.quit)
}

func (g *FsGenerator) Initialize(path string, fileregex string, l *log.Logger) {
	g.log = l
	g.fileregex = fileregex
	g.path = path

	g.jobs = make(chan *job.Job, 1)
	//	g.quit = make(chan struct{}, 1)

	go g.generate()
}

func (g *FsGenerator) generate() {

	defer close(g.jobs)

	g.enumerate(g.path)
}

func (g *FsGenerator) enumerate(path string) {
	files, err := ioutil.ReadDir(path)
	if err != nil {
		panic(err)
	}

	for _, f := range files {

		if f.IsDir() {
			g.enumerate(path + "/" + f.Name())
		}

		matched, err := regexp.MatchString(g.fileregex, f.Name())
		if err != nil {
			panic(err)
		}

		if !matched {
			continue
		}

		g.jobs <- job.New(g.path + "/" + f.Name())
	}
}
