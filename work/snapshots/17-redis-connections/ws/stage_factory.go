package ws

type Factory struct{ options *Options }

func NewFactory(options *Options) *Factory { return &Factory{options: options} }
func (f *Factory) Build() (*Engine, error) { return NewEngine(f.options) }
func (f *Factory) Close() error            { return nil }
