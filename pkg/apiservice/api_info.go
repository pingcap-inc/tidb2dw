package apiservice

import (
	"net/http"
	"sync"

	"github.com/gin-gonic/gin"
	"github.com/pingcap/log"
	"go.uber.org/zap"
)

type ServiceStatus string

const (
	ServiceStatusRunning ServiceStatus = "running"

	// For example, when connect S3 failed, all table replication will be failed
	ServiceStatusFatalError ServiceStatus = "fatal_error"
)

type TableStatus string

const (
	TableStatusRunning TableStatus = "running"

	// For example, when encounter unsupported DDL, this table replication will be failed
	TableStatusFatalError TableStatus = "fatal_error"
)

type APIInfo struct {
	status       ServiceStatus
	errorMessage string

	statusByTable       map[string]TableStatus
	errorMessageByTable map[string]string

	mu sync.Mutex
}

func NewAPIInfo() *APIInfo {
	return &APIInfo{
		status:       ServiceStatusRunning,
		errorMessage: "",

		statusByTable:       make(map[string]TableStatus),
		errorMessageByTable: make(map[string]string),
	}
}

func (s *APIInfo) registerRouter(router *gin.Engine) {
	router.GET("/info", func(c *gin.Context) {
		s.mu.Lock()
		defer s.mu.Unlock()

		c.JSON(http.StatusOK, gin.H{
			"status":                 s.status,
			"error_message":          s.errorMessage,
			"status_by_table":        s.statusByTable,
			"error_message_by_table": s.errorMessageByTable,
		})
	})
}

func (s *APIInfo) SetTableStatusFatalError(table string, err error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if status, ok := s.statusByTable[table]; ok && status == TableStatusFatalError {
		log.Warn("Ignored new fatal errors of table", zap.String("table", table), zap.Error(err))
		return
	}
	s.statusByTable[table] = TableStatusFatalError
	s.errorMessageByTable[table] = err.Error()
}

func (s *APIInfo) SetServiceStatusFatalError(err error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.status == ServiceStatusFatalError {
		log.Warn("Ignored new fatal errors", zap.Error(err))
		return
	}
	s.status = ServiceStatusFatalError
	s.errorMessage = err.Error()
}
