package main

import (
	"github.com/joshuaboelter/pipeline"

	"github.com/joshuaboelter/pipeline/example/echo"
	"github.com/joshuaboelter/pipeline/example/generator"
	"github.com/joshuaboelter/pipeline/example/terminus"

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
