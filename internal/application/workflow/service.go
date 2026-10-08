package workflow

import (
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"strconv"
	"sync"
	"uuid"
)

type Service struct {
	repo        Repository
	definitions map[string]Definition
	latest      map[string]Definition
	logger      *slog.Logger
	inFlight    sync.Map
}

func NewService(repo Repository, logger *slog.Logger, definitions ...Definition) (*Service, error) {
	if repo == nil {
		return nil, errors.New("workflow repository is required")
	}
	if logger == nil {
		logger = slog.Default()
	}
	s := &Service{repo: repo, logger: logger, definitions: make(map[string]Definition), latest: make(map[string]Definition)}
	for _, definition := range definitions {
		if definition.Type == "" || definition.Version <= 0 || definition.First == nil || len(definition.Steps) == 0 {
			return nil, errors.New("invalid workflow definition")
		}
		key := definitionKey(definition.Type, definition.Version)
		if _, exists := s.definitions[key]; exists {
			return nil, errors.New("duplicate workflow definition")
		}
		s.definitions[key] = definition
		if current, ok := s.latest[definition.Type]; !ok || current.Version < definition.Version {
			s.latest[definition.Type] = definition
		}
	}
	return s, nil
}

func definitionKey(kind string, version int) string { return kind + ":" + strconv.Itoa(version) }

func (s *Service) Create(ctx context.Context, kind string, payload json.RawMessage, idempotencyKey string) (Workflow, error) {
	definition, ok := s.latest[kind]
	if !ok {
		return Workflow{}, ErrUnknownType
	}
	first, err := definition.First(payload)
	if err != nil {
		return Workflow{}, err
	}
	if _, ok := definition.Steps[first.Type]; !ok {
		return Workflow{}, errors.New("workflow definition has unknown first step")
	}
	return s.repo.Create(ctx, Workflow{
		ID: uuid.New(), Type: kind, DefinitionVersion: definition.Version,
		Status: StatusPending, Payload: payload, IdempotencyKey: idempotencyKey,
	})
}

func (s *Service) Get(ctx context.Context, id uuid.UUID) (Workflow, error) {
	return s.repo.Get(ctx, id)
}
