package main

import (
	"net/http"

	"github.com/danielgtaylor/huma/v2"
	"github.com/danielgtaylor/huma/v2/adapters/humago"
	"github.com/sponez/job-manager/internal/apiserver"
	"github.com/sponez/job-manager/internal/application/job"
	"github.com/sponez/job-manager/internal/infrastructure/handler"
	"github.com/sponez/job-manager/internal/infrastructure/repository/memory"
)

func main() {
	mux := http.NewServeMux()

	apiConfig := huma.DefaultConfig("Job Manager API", "1.0.0")
	apiConfig.DocsRenderer = huma.DocsRendererSwaggerUI

	api := humago.New(mux, apiConfig)
	handlers := handlers()
	server := apiserver.New(handlers)

	server.Register(api)
	http.ListenAndServe(":8080", mux)
}

func handlers() []handler.Handler {
	jobHandler := createJobHandler()

	return []handler.Handler{
		jobHandler,
	}
}

func createJobHandler() *handler.JobHandler {
	jobRepository := memory.New()
	jobService := job.New(jobRepository)
	jobHandler := handler.NewJobHandler(jobService)

	return jobHandler
}
