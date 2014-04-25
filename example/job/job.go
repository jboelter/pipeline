package job

import (
	"time"
)

type Job struct {
	// filled in by the generator
	Id   int64
	Path string

	Err error // error state
}

func New(path string) *Job {
	return &Job{
		Id:   time.Now().UTC().UnixNano(),
		Path: path,
	}
}
