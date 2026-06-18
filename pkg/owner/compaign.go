package owner

import (
	"context"
	"fmt"
	"sync/atomic"
	"time"

	"github.com/pingcap-inc/tidb2dw/pkg/owner/config"
	"github.com/pingcap-inc/tidb2dw/pkg/owner/meta"
	"golang.org/x/sync/errgroup"
)

type Campaign struct {
	cfg *config.OwnerConfig

	backend meta.Backend

	isOwner atomic.Bool
}

func NewCampaign(cfg *config.OwnerConfig, backend meta.Backend) *Campaign {
	return &Campaign{
		cfg:     cfg,
		backend: backend,
		isOwner: atomic.Bool{},
	}
}

func (c *Campaign) Start(eg *errgroup.Group, ctx context.Context) {
	eg.Go(func() error {
		serverId := fmt.Sprintf("%s:%d", c.cfg.Host, c.cfg.Port)

		ticker := time.NewTicker(time.Duration(c.cfg.LeaseRenewInterval) * time.Second)
		defer ticker.Stop()
		for {
			select {
			case <-ticker.C:
				if c.IsOwner() {
					success, err := c.backend.RenewOwnerLease(serverId, c.cfg.LeaseDuration)
					if err != nil {
						// log error
						continue
					}
					if !success {
						// Someone else has taken the owner
						c.isOwner.Store(false)
					}
				} else {
					// Try to campaign owner
					success, err := c.backend.TryCampaignOwner(serverId, c.cfg.LeaseDuration)
					if err != nil {
						// log error
						continue
					}
					if success {
						c.isOwner.Store(true)
					}
				}
			case <-ctx.Done():
				return nil
			}
		}
	})
}

func (c *Campaign) IsOwner() bool {
	return c.isOwner.Load()
}
