package handler

import "github.com/danielgtaylor/huma/v2"

type Handler interface {
	Register(api huma.API)
}
