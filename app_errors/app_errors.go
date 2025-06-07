package app_errors

import (
	"fmt"
)

type ErrType byte

const (
	BadReq ErrType = iota
	NotFound
	Forbidden
	Conflict
)

type AppErr struct {
	errType ErrType
	message string
}

func NewAppErr(errType ErrType, message ...any) error {
	return &AppErr{
		errType: errType,
		message: fmt.Sprint(message...),
	}
}

func (e *AppErr) Type() ErrType {
	return e.errType
}

func (e *AppErr) Error() string {
	return e.message
}

var _errTypeToStatus = map[ErrType]int{
	BadReq:    400,
	Forbidden: 403,
	NotFound:  404,
	Conflict:  409,
}

func GetStatus(errType ErrType) int {
	return _errTypeToStatus[errType]
}
