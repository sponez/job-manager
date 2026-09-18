package dtos

type JobResponse struct {
	ID     string `json:"id"`
	Name   string `json:"name"`
	Status string `json:"status"`
}

// Create job
type CreateJobInputBody struct {
	Name string `json:"name"`
}

type CreateJobInput struct {
	Body CreateJobInputBody
}

type CreateJobOutput struct {
	Body JobResponse
}

// Get job
type GetJobInput struct {
	ID string `path:"id"`
}

type GetJobOutput struct {
	Body JobResponse
}

// Complete job
type CompleteJobInput struct {
	ID string `path:"id"`
}

type CompleteJobOutput struct{}

// Get jobs
type GetJobsInput struct{}

type GetJobsOutput struct {
	Body []JobResponse
}
