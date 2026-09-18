package apiserver

import (
	"github.com/danielgtaylor/huma/v2"
	"github.com/sponez/job-manager/internal/infrastructure/handler"
)

type Server struct {
	handlers []handler.Handler
}

func New(handlers []handler.Handler) *Server {
	return &Server{handlers}
}

func (s *Server) Register(api huma.API) {
	for _, h := range s.handlers {
		h.Register(api)
	}
}
