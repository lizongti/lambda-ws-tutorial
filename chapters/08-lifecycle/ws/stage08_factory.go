package ws

import "io"

type Factory struct{ options *Options }

func NewFactory(options *Options) *Factory { return &Factory{options: options} }
func (f *Factory) Build() (*Engine, error) { return &Engine{}, nil }
func (f *Factory) Close() error            { return nil }

var _ io.Closer = (*Factory)(nil)
