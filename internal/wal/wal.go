package wal

import "kv-engine/internal/model"

// TODO: implementirati segmentirani WAL
type WAL struct{}

func New() *WAL { return &WAL{} }

func (w *WAL) Append(_ model.Record) error { return nil }

func (w *WAL) Replay(_ func(model.Record) error) error { return nil }
