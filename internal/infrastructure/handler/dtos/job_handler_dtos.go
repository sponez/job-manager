package dtos

type JobResponse struct {
	ID     string `json:"id"`
	Name   string `json:"name"`
	Status string `json:"status"`
}

// Create job
type CreateJobInputBody struct {
	Name string `json:"name" example:"Send email" doc:"Job type: Send email or Get page"`
}

type CreateJobInput struct {
	Body CreateJobInputBody
}

type CreateJobOutput struct {
	Location string `header:"Location"`
	Body     JobResponse
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

type GetJobsOutputBody struct {
	Jobs     []JobResponse `json:"jobs"`
	Partial  bool          `json:"partial" doc:"True when some jobs could not be read"`
	Warnings []string      `json:"warnings"`
}

type GetJobsOutput struct {
	Body GetJobsOutputBody
}
