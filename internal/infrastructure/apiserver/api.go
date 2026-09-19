package apiserver

import (
	"github.com/danielgtaylor/huma/v2"
)

type Handler interface {
	Register(api huma.API)
}

type Server struct {
	handlers []Handler
}

func New(handlers []Handler) *Server {
	return &Server{handlers: handlers}
}

func (s *Server) Register(api huma.API) {
	for _, h := range s.handlers {
		h.Register(api)
	}
}
