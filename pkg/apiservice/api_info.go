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
	Stage        TableStage  `json:"stage"`
	Status       TableStatus `json:"status"`
	ErrorMessage string      `json:"error_message"`
}

type InfoResponse struct {
	Status       ServiceStatus         `json:"status"`
	ErrorMessage string                `json:"error_message"`
	TablesInfo   map[string]*TableInfo `json:"tables_info"`
}

type APIInfo struct {
	status       ServiceStatus
	errorMessage string

	tablesInfo map[string]*TableInfo

	mu sync.Mutex
}

func NewAPIInfo() *APIInfo {
	return &APIInfo{
		status:       ServiceStatusRunning,
		errorMessage: "",

		tablesInfo: make(map[string]*TableInfo),
	}
}

func (s *APIInfo) registerRouter(router *gin.Engine) {
	router.GET("/info", func(c *gin.Context) {
		s.mu.Lock()
		defer s.mu.Unlock()

		c.JSON(http.StatusOK, InfoResponse{
			Status:       s.status,
			ErrorMessage: s.errorMessage,
			TablesInfo:   s.tablesInfo,
		})
	})
}

func (s *APIInfo) initTableInfoIfNotExist(table string) {
	if _, ok := s.tablesInfo[table]; ok {
		s.tablesInfo[table] = &TableInfo{
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
	if s.tablesInfo[table].Status == TableStatusFatalError {
		log.Warn("Ignored setting table to fatal error", zap.String("table", table), zap.Error(err))
		return
	}
	s.tablesInfo[table].Status = TableStatusFatalError
	s.tablesInfo[table].ErrorMessage = err.Error()
}

func (s *APIInfo) SetTableStage(table string, stage TableStage) {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.initTableInfoIfNotExist(table)
	s.tablesInfo[table].Stage = stage
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
