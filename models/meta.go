package models

import (
	"errors"
	"fmt"
	"strconv"
	"strings"
	"sync"
)

type Meta struct {
	ID        uint64 `msgpack:"-"`
	Version   uint64 `msgpack:"-"`
	TimeRange TimeRange
	LogsLen   int
	Offsets   *Offsets    `msgpack:",omitempty"`
	IsDeleted bool        `msgpack:",omitempty"`
	Mx        *sync.Mutex `msgpack:"-"`
}

func NewMeta(id uint64, timestamp int64) *Meta {
	return &Meta{
		ID:        id,
		Version:   1,
		TimeRange: TimeRange{Start: timestamp, End: timestamp},
		Offsets:   &Offsets{},
		Mx:        &sync.Mutex{},
	}
}

func NewMetaEmpty(chunkName string) (meta *Meta, err error) {
	parts := strings.Split(chunkName, "_")

	if len(parts) != 2 {
		return nil, errors.New("incorrect chunk name: " + chunkName)
	}
	meta = &Meta{Mx: &sync.Mutex{}}

	meta.ID, err = strconv.ParseUint(parts[0], 10, 64)

	if err != nil {
		return nil, errors.New("incorrect chunk ID: " + parts[0])
	}

	meta.Version, err = strconv.ParseUint(parts[1], 10, 64)

	if err != nil {
		return nil, errors.New("incorrect chunk version: " + parts[1])
	}

	return meta, nil
}

func (cm *Meta) Copy() *Meta {
	meta := *cm
	return &meta
}

func (cm *Meta) Name() string {
	return fmt.Sprintf("%d_%d", cm.ID, cm.Version)
}
