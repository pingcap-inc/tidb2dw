package apiservice

import (
	"net"
	"os"
	"os/signal"
	"syscall"

	"github.com/gin-gonic/gin"
	"github.com/pingcap/log"
	"go.uber.org/zap"
)

type APIService struct {
	APIInfo *APIInfo
	router  *gin.Engine
}

func New(tables []string) *APIService {
	gin.SetMode(gin.ReleaseMode)

	r := gin.New()
	r.Use(gin.Recovery())

	apiInfo := NewAPIInfo(tables)
	apiInfo.registerRouter(r)

	return &APIService{
		APIInfo: apiInfo,
		router:  r,
	}
}

func (service *APIService) Serve(l net.Listener) {
	go func() {
		if err := service.router.RunListener(l); err != nil {
			log.Panic("Serve failed", zap.Error(err))
		}
	}()

	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)
	s := <-quit
	log.Info("Received exit signal, shutting down API service ...", zap.String("signal", s.String()))

	_ = l.Close()
}
