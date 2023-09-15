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
	ServiceStatusRunning    ServiceStatus = "running"
	ServiceStatusFatalError ServiceStatus = "fatal_error"
)

type APIInfo struct {
	tables             []string
	statuses           map[string]ServiceStatus
	errorMessages      map[string]string
	globalStatus       ServiceStatus
	globalErrorMessage string
	mu                 sync.Mutex
}

func NewAPIInfo(tables []string) *APIInfo {
	return &APIInfo{
		tables:             tables,
		statuses:           make(map[string]ServiceStatus),
		errorMessages:      make(map[string]string),
		globalStatus:       ServiceStatusRunning,
		globalErrorMessage: "",
	}
}

func (s *APIInfo) registerRouter(router *gin.Engine) {
	router.GET("/info", func(c *gin.Context) {
		s.mu.Lock()
		defer s.mu.Unlock()
		if s.globalStatus == ServiceStatusFatalError {
			c.JSON(http.StatusOK, gin.H{
				"status":        s.globalStatus,
				"error_message": s.globalErrorMessage,
			})
		} else {
			c.JSON(http.StatusOK, gin.H{
				"status":        s.statuses,
				"error_message": s.errorMessages,
			})
		}
	})
}

func (s *APIInfo) SetStatusFatalError(table string, err error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.statuses[table] == ServiceStatusFatalError {
		log.Warn("Ignored new fatal errors", zap.Error(err))
		return
	}
	s.statuses[table] = ServiceStatusFatalError
	s.errorMessages[table] = err.Error()
}

func (s *APIInfo) SetGlobalStatusFatalError(err error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.globalStatus == ServiceStatusFatalError {
		log.Warn("Ignored new fatal errors", zap.Error(err))
		return
	}
	s.globalStatus = ServiceStatusFatalError
	s.globalErrorMessage = err.Error()
}
