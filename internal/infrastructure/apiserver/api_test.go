package apiserver

import (
	"testing"

	"github.com/danielgtaylor/huma/v2"
)

type HandlerStub struct {
	t        *testing.T
	register func(api huma.API)
}

func (h *HandlerStub) Register(api huma.API) {
	h.t.Helper()
	if h.register == nil {
		h.t.Fatal("unexpected handler Register call")
	}
	h.register(api)
}

func TestNewServer(t *testing.T) {
	h := HandlerStub{
		t:        t,
		register: func(api huma.API) {},
	}
	s := New([]Handler{&h})

	if len(s.handlers) != 1 {
		t.Fatalf("New() stored %d handlers, want 1", len(s.handlers))
	}
	if s.handlers[0] != &h {
		t.Error("New() stored an unexpected handler")
	}
}

func TestRegister(t *testing.T) {
	var api huma.API
	called := 0
	h := HandlerStub{
		t:        t,
		register: func(api huma.API) { called++ },
	}
	s := New([]Handler{&h})

	s.Register(api)

	if called != 1 {
		t.Error("Register() does not call handlers")
	}
}
