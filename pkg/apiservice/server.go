package apiservice

import (
	"net"
	"os"
	"os/signal"
	"syscall"

	"github.com/gin-gonic/gin"
	"github.com/pingcap-inc/tidb2dw/pkg/metrics"
	"github.com/pingcap/log"
	"github.com/pingcap/tidb/util/promutil"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"go.uber.org/zap"
)

type APIService struct {
	APIInfo *APIInfo
	Metric  *metrics.Metrics
	router  *gin.Engine
}

func New() *APIService {
	gin.SetMode(gin.ReleaseMode)

	r := gin.New()
	r.Use(gin.Recovery())

	apiInfo := NewAPIInfo()
	apiInfo.registerRouter(r)

	metric := RegisterMetric(r)

	return &APIService{
		APIInfo: apiInfo,
		Metric:  metric,
		router:  r,
	}
}

// RegisterMetric registers the metric handler.
func RegisterMetric(router *gin.Engine) *metrics.Metrics {
	metric := metrics.NewMetrics(promutil.NewDefaultFactory())
	registry := promutil.NewDefaultRegistry()
	metric.RegisterTo(registry)
	metric.UnregisterFrom(registry)

	router.GET("/metrics", func(c *gin.Context) {
		promhttp.Handler().ServeHTTP(c.Writer, c.Request)
	})
	return metric
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
