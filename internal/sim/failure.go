package sim

type FailureInjector struct {
	paused map[string]bool
	killed map[string]bool
}

func NewFailureInjector() *FailureInjector {
	return &FailureInjector{paused: map[string]bool{}, killed: map[string]bool{}}
}

func (f *FailureInjector) Pause(node string)   { f.paused[node] = true }
func (f *FailureInjector) Resume(node string)  { delete(f.paused, node) }
func (f *FailureInjector) Kill(node string)    { f.killed[node] = true }
func (f *FailureInjector) Restart(node string) { delete(f.killed, node) }

func (f *FailureInjector) Available(node string) bool {
	return !f.paused[node] && !f.killed[node]
}
