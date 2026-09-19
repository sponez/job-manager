package job

import (
	"errors"
	"testing"
	"uuid"
)

func TestNewJob(t *testing.T) {
	id := uuid.New()
	tests := []struct {
		name            string
		kind            Kind
		status          Status
		wantKindError   bool
		wantStatusError bool
	}{
		{
			name:   "pending email",
			kind:   KindSendEmail,
			status: StatusPending,
		},
		{
			name:   "completed email",
			kind:   KindSendEmail,
			status: StatusDone,
		},
		{
			name:   "pending page",
			kind:   KindGetPage,
			status: StatusPending,
		},
		{
			name:   "completed page",
			kind:   KindGetPage,
			status: StatusDone,
		},
		{
			name:          "invalid kind",
			kind:          "any",
			status:        StatusPending,
			wantKindError: true,
		},
		{
			name:            "invalid status",
			kind:            KindSendEmail,
			status:          "any",
			wantStatusError: true,
		},
		{
			name:            "invalid kind and status",
			kind:            "any",
			status:          "any",
			wantKindError:   true,
			wantStatusError: true,
		},
		{
			name:          "empty kind",
			kind:          "",
			status:        StatusPending,
			wantKindError: true,
		},
		{
			name:            "empty status",
			kind:            KindSendEmail,
			status:          "",
			wantStatusError: true,
		},
		{
			name:            "empty kind and status",
			kind:            "",
			status:          "",
			wantKindError:   true,
			wantStatusError: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := New(id, tt.kind, tt.status)

			if hasKindError := errors.Is(err, ErrKindIsNotValid); hasKindError != tt.wantKindError {
				t.Errorf("New() error = %v; contains ErrKindIsNotValid = %v, want %v", err, hasKindError, tt.wantKindError)
			}
			if hasStatusError := errors.Is(err, ErrStatusIsNotValid); hasStatusError != tt.wantStatusError {
				t.Errorf("New() error = %v; contains ErrStatusIsNotValid = %v, want %v", err, hasStatusError, tt.wantStatusError)
			}

			if tt.wantKindError || tt.wantStatusError {
				if got != nil {
					t.Errorf("New() = %+v, want nil for invalid input", got)
				}
				return
			}

			if err != nil {
				t.Fatalf("New() unexpected error: %v", err)
			}
			if got == nil {
				t.Fatal("New() returned nil without an error")
			}

			if got.ID() != id {
				t.Errorf("ID() = %v, want %v", got.ID(), id)
			}
			if got.Kind() != tt.kind {
				t.Errorf("Kind() = %v, want %v", got.Kind(), tt.kind)
			}
			if got.Status() != tt.status {
				t.Errorf("Status() = %v, want %v", got.Status(), tt.status)
			}
		})
	}
}
