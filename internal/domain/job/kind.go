package job

import "slices"

type Kind string

const (
	KindSendEmail Kind = "Send email"
	KindGetPage   Kind = "Get page"
)

var allKinds = []Kind{
	KindSendEmail,
	KindGetPage,
}

func (k *Kind) Valid() bool {
	return slices.Contains(allKinds, *k)
}
