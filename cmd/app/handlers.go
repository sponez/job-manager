package main

import (
	"github.com/sponez/job-manager/internal/application/job"
	"github.com/sponez/job-manager/internal/infrastructure/apiserver"
	"github.com/sponez/job-manager/internal/infrastructure/handler"
)

func buildHandlers(jobRepository job.JobRepository) []apiserver.Handler {
	jobService := job.New(jobRepository)
	jobHandler := handler.NewJobHandler(jobService)

	return []apiserver.Handler{
		jobHandler,
	}
}
