package main

import (
	"github.com/jboelter/pipeline"

	"github.com/jboelter/pipeline/example/echo"
	"github.com/jboelter/pipeline/example/generator"
	"github.com/jboelter/pipeline/example/terminus"

	"log"
	"os"
)

func main() {

	logger := log.New(os.Stdout, "", 0)

	p := pipeline.NewWithLogger(logger, true)

	generator.Generator.Initialize(os.Getenv("GOPATH"), `.*\.go`, logger)
	p.AddGenerator(generator.Generator)

	echo.Stage.SetLogger(logger)
	p.AddStage(echo.Stage)

	terminus.Stage.SetLogger(logger)
	p.AddStage(terminus.Stage)

	p.Run()

}
