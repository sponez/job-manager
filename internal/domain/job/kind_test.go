package job

import "testing"

func TestKindValidation(t *testing.T) {
	tests := []struct {
		name string
		kind Kind
		want bool
	}{
		{
			name: "send email",
			kind: KindSendEmail,
			want: true,
		},
		{
			name: "get page",
			kind: KindGetPage,
			want: true,
		},
		{
			name: "unknown",
			kind: "any",
			want: false,
		},
		{
			name: "empty",
			kind: "",
			want: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := tt.kind.Valid(); got != tt.want {
				t.Errorf("Kind(%q).Valid() = %v, want %v", tt.kind, got, tt.want)
			}
		})
	}
}
