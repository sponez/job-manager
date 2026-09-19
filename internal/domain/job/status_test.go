package job

import "testing"

func TestStatusValidation(t *testing.T) {
	tests := []struct {
		name   string
		status Status
		want   bool
	}{
		{
			name:   "pending",
			status: StatusPending,
			want:   true,
		},
		{
			name:   "done",
			status: StatusDone,
			want:   true,
		},
		{
			name:   "unknown",
			status: "any",
			want:   false,
		},
		{
			name:   "empty",
			status: "",
			want:   false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := tt.status.Valid(); got != tt.want {
				t.Errorf("Status(%q).Valid() = %v, want %v", tt.status, got, tt.want)
			}
		})
	}
}
