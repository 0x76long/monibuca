package task

import (
	"fmt"
	"time"
)

type ChannelTask struct {
	Task
	SignalChan any
}

func (*ChannelTask) GetTaskType() TaskType {
	return TASK_TYPE_CHANNEL
}

func (t *ChannelTask) GetSignal() any {
	return t.SignalChan
}

func (t *ChannelTask) Tick(any) {
}

type TickTask struct {
	ChannelTask
	Ticker *time.Ticker
}

func (t *TickTask) GetTickInterval() time.Duration {
	return time.Second
}

func (t *TickTask) Start() (err error) {
	interval := t.handler.(interface{ GetTickInterval() time.Duration }).GetTickInterval()
	if interval <= 0 {
		return fmt.Errorf("tick interval must be greater than 0")
	}
	t.Ticker = time.NewTicker(interval)
	t.SignalChan = t.Ticker.C
	return
}

func (t *TickTask) Dispose() {
	if t.Ticker != nil {
		t.Ticker.Stop()
	}
}
