package main

import (
	"fmt"
	"math/rand"

	"github.com/sponez/job-manager/internal/domain/job"
)

func main() {
	var jobs []job.Job
	var jobsNum = rand.Intn(10)

	for i := 0; i < jobsNum; i++ {
		job := job.Job{
			ID:     i + 1,
			Name:   fmt.Sprintf("Job %v", i+1),
			Status: job.StatusPending,
		}

		jobs = append(jobs, job)
	}

	for _, job := range jobs {
		fmt.Printf(
			"%v | %v | %v\n",
			job.ID,
			job.Name,
			job.Status,
		)
	}
}
