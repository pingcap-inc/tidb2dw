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
	TableStageLoadingSnapshot    TableStage = "loading_snapshot"
	TableStageLoadingIncremental TableStage = "loading_incremental"
	TableStageFinished           TableStage = "finished"
)

type TableStatus string

const (
	TableStatusNormal     TableStatus = "normal"
	TableStatusFatalError TableStatus = "fatal_error"
)

type InfoResponse struct {
	Status              ServiceStatus          `json:"status"`
	ErrorMessage        string                 `json:"error_message"`
	StageByTable        map[string]TableStage  `json:"stage_by_table"`
	StatusByTable       map[string]TableStatus `json:"status_by_table"`
	ErrorMessageByTable map[string]string      `json:"error_message_by_table"`
}

type APIInfo struct {
	status       ServiceStatus
	errorMessage string

	stageByTable        map[string]TableStage
	statusByTable       map[string]TableStatus
	errorMessageByTable map[string]string

	mu sync.Mutex
}

func NewAPIInfo() *APIInfo {
	return &APIInfo{
		status:       ServiceStatusRunning,
		errorMessage: "",

		stageByTable:        make(map[string]TableStage),
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
			StageByTable:        s.stageByTable,
			StatusByTable:       s.statusByTable,
			ErrorMessageByTable: s.errorMessageByTable,
		})
	})
}

func (s *APIInfo) SetTableFatalError(table string, err error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.statusByTable[table] == TableStatusFatalError {
		log.Warn("Ignored setting table to fatal error", zap.String("table", table), zap.Error(err))
		return
	}
	s.errorMessageByTable[table] = err.Error()
	s.statusByTable[table] = TableStatusFatalError
}

func (s *APIInfo) SetTableStage(table string, stage TableStage) {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.stageByTable[table] = stage
	if _, ok := s.statusByTable[table]; !ok {
		s.statusByTable[table] = TableStatusNormal
		s.errorMessageByTable[table] = ""
	}
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
