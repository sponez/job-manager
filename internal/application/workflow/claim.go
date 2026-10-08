package workflow

import (
	"context"
	"uuid"
)

func (s *Service) stepReserve(onlyIDs, excludeIDs []uuid.UUID, limit int) StepReserve {
	definitions := make([]DefinitionRef, 0, len(s.definitions))
	for _, definition := range s.definitions {
		definitions = append(definitions, DefinitionRef{Type: definition.Type, Version: definition.Version})
	}
	return StepReserve{
		StepStatuses: []string{StatusPending, StatusRunning},
		WorkflowStates: []StepWorkflowStatus{
			{Kind: KindForward, WorkflowStatus: StatusRunning},
			{Kind: KindCompensation, WorkflowStatus: StatusCompensating},
		},
		Definitions: definitions,
		OnlyIDs:     onlyIDs,
		ExcludeIDs:  excludeIDs,
		Limit:       limit,
		NewStatus:   StatusRunning,
		Lease:       stepLease,
	}
}

func (s *Service) claimStep(ctx context.Context, id uuid.UUID) (StepRecord, bool, error) {
	if len(s.definitions) == 0 {
		return StepRecord{}, false, nil
	}
	reserved, err := s.repo.ReserveSteps(ctx, s.stepReserve([]uuid.UUID{id}, nil, 1))
	if err != nil {
		return StepRecord{}, false, err
	}
	if len(reserved) == 0 {
		return StepRecord{}, false, nil
	}
	return reserved[0].Step, true, nil
}
