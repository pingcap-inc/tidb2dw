package taskruntime

import (
	"fmt"
	"sync"
)

type TaskStatus string

const (
	TaskStatusCreated TaskStatus = "created"
	TaskStatusRunning TaskStatus = "running"
	TaskStatusStopped TaskStatus = "stopped"
	TaskStatusFailed  TaskStatus = "failed"
)

type TaskError struct {
	Class     string
	Message   string
	Retryable bool
}

type TaskRecord struct {
	TenantID      string
	TaskID        string
	CorrelationID string
	Status        TaskStatus
	LastError     *TaskError
}

type ControllerOptions struct {
	MaxRunningTotal     int
	MaxRunningPerTenant int
}

type Controller struct {
	mu      sync.Mutex
	opts    ControllerOptions
	records map[string]TaskRecord
}

func NewController(opts ControllerOptions) *Controller {
	return &Controller{
		opts:    opts,
		records: make(map[string]TaskRecord),
	}
}

func (c *Controller) Create(plan RuntimePlan) (TaskRecord, error) {
	c.mu.Lock()
	defer c.mu.Unlock()

	key := taskKey(plan.TenantID, plan.TaskID)
	if _, ok := c.records[key]; ok {
		return TaskRecord{}, fmt.Errorf("task %s already exists", key)
	}
	record := TaskRecord{
		TenantID:      plan.TenantID,
		TaskID:        plan.TaskID,
		CorrelationID: plan.CorrelationID,
		Status:        TaskStatusCreated,
	}
	c.records[key] = record
	return record, nil
}

func (c *Controller) Start(tenantID, taskID string) (TaskRecord, error) {
	return c.transitionToRunning(tenantID, taskID, false)
}

func (c *Controller) Resume(tenantID, taskID string) (TaskRecord, error) {
	return c.transitionToRunning(tenantID, taskID, true)
}

func (c *Controller) Stop(tenantID, taskID string) (TaskRecord, error) {
	c.mu.Lock()
	defer c.mu.Unlock()

	key := taskKey(tenantID, taskID)
	record, ok := c.records[key]
	if !ok {
		return TaskRecord{}, fmt.Errorf("task %s not found", key)
	}
	if record.Status != TaskStatusRunning {
		return TaskRecord{}, fmt.Errorf("task %s cannot stop from %s", key, record.Status)
	}
	record.Status = TaskStatusStopped
	c.records[key] = record
	return record, nil
}

func (c *Controller) Fail(tenantID, taskID string, taskErr TaskError) (TaskRecord, error) {
	c.mu.Lock()
	defer c.mu.Unlock()

	key := taskKey(tenantID, taskID)
	record, ok := c.records[key]
	if !ok {
		return TaskRecord{}, fmt.Errorf("task %s not found", key)
	}
	record.Status = TaskStatusFailed
	record.LastError = &taskErr
	c.records[key] = record
	return record, nil
}

func (c *Controller) Status(tenantID, taskID string) (TaskRecord, error) {
	c.mu.Lock()
	defer c.mu.Unlock()

	key := taskKey(tenantID, taskID)
	record, ok := c.records[key]
	if !ok {
		return TaskRecord{}, fmt.Errorf("task %s not found", key)
	}
	return record, nil
}

func (c *Controller) transitionToRunning(tenantID, taskID string, resume bool) (TaskRecord, error) {
	c.mu.Lock()
	defer c.mu.Unlock()

	key := taskKey(tenantID, taskID)
	record, ok := c.records[key]
	if !ok {
		return TaskRecord{}, fmt.Errorf("task %s not found", key)
	}
	if resume {
		if record.Status != TaskStatusStopped && record.Status != TaskStatusFailed {
			return TaskRecord{}, fmt.Errorf("task %s cannot resume from %s", key, record.Status)
		}
	} else if record.Status != TaskStatusCreated {
		return TaskRecord{}, fmt.Errorf("task %s cannot start from %s", key, record.Status)
	}

	if err := c.checkConcurrency(tenantID); err != nil {
		return TaskRecord{}, err
	}
	record.Status = TaskStatusRunning
	record.LastError = nil
	c.records[key] = record
	return record, nil
}

func (c *Controller) checkConcurrency(tenantID string) error {
	total := 0
	tenantTotal := 0
	for _, record := range c.records {
		if record.Status != TaskStatusRunning {
			continue
		}
		total++
		if record.TenantID == tenantID {
			tenantTotal++
		}
	}
	if c.opts.MaxRunningTotal > 0 && total >= c.opts.MaxRunningTotal {
		return fmt.Errorf("global running task limit %d reached", c.opts.MaxRunningTotal)
	}
	if c.opts.MaxRunningPerTenant > 0 && tenantTotal >= c.opts.MaxRunningPerTenant {
		return fmt.Errorf("tenant %s running task limit %d reached", tenantID, c.opts.MaxRunningPerTenant)
	}
	return nil
}

func taskKey(tenantID, taskID string) string {
	return tenantID + "/" + taskID
}
