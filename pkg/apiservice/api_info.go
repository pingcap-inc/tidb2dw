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

type TableStage string

const (
	TableStageUnknown            TableStage = "unknown"
	TableStageLoadingSnapshot    TableStage = "loading_snapshot"
	TableStageLoadingIncremental TableStage = "loading_incremental"
	TableStageFinished           TableStage = "finished"
)

type TableStatus string

const (
	TableStatusNormal     TableStatus = "normal"
	TableStatusFatalError TableStatus = "fatal_error"
)

type TableInfo struct {
	Stage        TableStage  `json:"stage,omitempty"`
	Status       TableStatus `json:"status,omitempty"`
	ErrorMessage string      `json:"error_message,omitempty"`
}

type InfoResponse struct {
	Status       ServiceStatus         `json:"status,omitempty"`
	ErrorMessage string                `json:"error_message,omitempty"`
	TablesInfo   map[string]*TableInfo `json:"tables_info,omitempty"`
}

type APIInfo struct {
	r  InfoResponse
	mu sync.Mutex
}

func NewAPIInfo() *APIInfo {
	return &APIInfo{
		r: InfoResponse{
			Status:       ServiceStatusRunning,
			ErrorMessage: "",
			TablesInfo:   make(map[string]*TableInfo),
		},
	}
}

func (s *APIInfo) registerRouter(router *gin.Engine) {
	router.GET("/info", func(c *gin.Context) {
		s.mu.Lock()
		defer s.mu.Unlock()

		c.JSON(http.StatusOK, s.r)
	})
}

func (s *APIInfo) initTableInfoIfNotExist(table string) {
	if _, ok := s.r.TablesInfo[table]; !ok {
		s.r.TablesInfo[table] = &TableInfo{
			Stage:        TableStageUnknown,
			Status:       TableStatusNormal,
			ErrorMessage: "",
		}
	}
}

func (s *APIInfo) SetTableFatalError(table string, err error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.initTableInfoIfNotExist(table)
	if s.r.TablesInfo[table].Status == TableStatusFatalError {
		log.Warn("Ignored setting table to fatal error", zap.String("table", table), zap.Error(err))
		return
	}
	s.r.TablesInfo[table].Status = TableStatusFatalError
	s.r.TablesInfo[table].ErrorMessage = err.Error()
}

func (s *APIInfo) SetTableStage(table string, stage TableStage) {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.initTableInfoIfNotExist(table)
	s.r.TablesInfo[table].Stage = stage
}

func (s *APIInfo) SetServiceStatusIdle() {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.r.Status == ServiceStatusFatalError {
		log.Warn("Ignored setting status to idle", zap.String("current_error", s.r.ErrorMessage))
		return
	}

	s.r.Status = ServiceStatusIdle
	s.r.ErrorMessage = ""
}

func (s *APIInfo) SetServiceStatusFatalError(err error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.r.Status == ServiceStatusFatalError {
		log.Warn("Ignored setting status to fatal error", zap.Error(err))
		return
	}

	s.r.Status = ServiceStatusFatalError
	s.r.ErrorMessage = err.Error()
}
