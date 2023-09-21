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
	ServiceStatusIdle    ServiceStatus = "idle"

	// For example, when connect S3 failed, all table replication will be failed
	ServiceStatusFatalError ServiceStatus = "fatal_error"
)

type TableStatus string

const (
	TableStatusRunning TableStatus = "running"

	// For example, when encounter unsupported DDL, this table replication will be failed
	TableStatusFatalError TableStatus = "fatal_error"
)

type InfoResponse struct {
	Status              ServiceStatus          `json:"status"`
	ErrorMessage        string                 `json:"error_message"`
	StatusByTable       map[string]TableStatus `json:"status_by_table"`
	ErrorMessageByTable map[string]string      `json:"error_message_by_table"`
}

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

		c.JSON(http.StatusOK, InfoResponse{
			Status:              s.status,
			ErrorMessage:        s.errorMessage,
			StatusByTable:       s.statusByTable,
			ErrorMessageByTable: s.errorMessageByTable,
		})
	})
}

func (s *APIInfo) SetTableStatusFatalError(table string, err error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if status, ok := s.statusByTable[table]; ok && status == TableStatusFatalError {
		log.Warn("Ignored setting table status to fatal error", zap.String("table", table), zap.Error(err))
		return
	}
	s.statusByTable[table] = TableStatusFatalError
	s.errorMessageByTable[table] = err.Error()
}

func (s *APIInfo) SetServiceStatusIdle() {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.status == ServiceStatusFatalError {
		log.Warn("Ignored setting status to idle", zap.String("current_error", s.errorMessage))
		return
	}

	s.status = ServiceStatusIdle
	s.errorMessage = ""
}

func (s *APIInfo) SetServiceStatusFatalError(err error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.status == ServiceStatusFatalError {
		log.Warn("Ignored setting status to fatal error", zap.Error(err))
		return
	}

	s.status = ServiceStatusFatalError
	s.errorMessage = err.Error()
}
