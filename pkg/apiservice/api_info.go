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
	status       ServiceStatus
	errorMessage string
	mu           sync.Mutex
}

func NewAPIInfo() *APIInfo {
	return &APIInfo{
		status:       ServiceStatusRunning,
		errorMessage: "",
	}
}

func (s *APIInfo) registerRouter(router *gin.Engine) {
	router.GET("/info", func(c *gin.Context) {
		s.mu.Lock()
		defer s.mu.Unlock()
		c.JSON(http.StatusOK, gin.H{
			"status":        s.status,
			"error_message": s.errorMessage,
		})
	})
}

func (s *APIInfo) SetStatusFatalError(err error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.status == ServiceStatusFatalError {
		log.Warn("Ignored new fatal errors", zap.Error(err))
		return
	}
	s.status = ServiceStatusFatalError
	s.errorMessage = err.Error()
}
