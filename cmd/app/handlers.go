package main

import (
	"github.com/sponez/job-manager/internal/application/job"
	"github.com/sponez/job-manager/internal/infrastructure/apiserver"
	"github.com/sponez/job-manager/internal/infrastructure/handler"
)

func buildHandlers(jobRepository job.JobRepository, queue handler.TaskQueue) []apiserver.Handler {
	jobService := job.New(jobRepository)
	jobHandler := handler.NewJobHandler(jobService, queue)

	return []apiserver.Handler{
		jobHandler,
	}
}
