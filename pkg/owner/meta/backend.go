package meta

import (
	"database/sql"
	"fmt"

	_ "github.com/go-sql-driver/mysql"
	"github.com/pingcap-inc/tidb2dw/pkg/owner/config"
)

type Backend interface {
	// Bootstrap initializes the schema of the table.
	Bootstrap() error

	// GetOwner returns the active owner of the tidb2dw service.
	GetOwner() (string, error)

	// TryCampaignOwner tries to campaign the owner of the tidb2dw service.
	TryCampaignOwner(who string, leaseDurSec int) (bool, error)

	// RenewOwnerLease renews the lease of the owner of the tidb2dw service.
	RenewOwnerLease(who string, leaseDurSec int) (bool, error)
}

// RdsBackend is the backend implementation for MySQL RDS.
type RdsBackend struct {
	cfg *config.MetaDBConfig

	db *sql.DB
}

// NewRdsBackend creates a new RdsBackend instance.
func NewRdsBackend(cfg *config.MetaDBConfig) (*RdsBackend, error) {
	dbUrl := fmt.Sprintf("%s:%s@tcp(%s:%d)/%s?charset=utf8mb4", cfg.User, cfg.Password, cfg.Host, cfg.Port, cfg.Db)
	db, err := sql.Open("mysql", dbUrl)
	if err != nil {
		return nil, err
	}

	db.SetMaxOpenConns(cfg.MaxOpenConns)
	db.SetMaxIdleConns(cfg.MaxIdleConns)

	return &RdsBackend{
		cfg: cfg,
		db:  db,
	}, nil
}

// Bootstrap initializes the schema of the table.
func (b *RdsBackend) Bootstrap() error {
	// Create the owner table if not exists.
	// owner {
	//	id INT PRIMARY KEY,
	// 	who VARCHAR(255),
	// 	lease_expire TIMESTAMP,
	// }
	// There is just one row in the owner table which id is 1.
	_, err := b.db.Exec("CREATE TABLE IF NOT EXISTS owner (id INT PRIMARY KEY, who VARCHAR(255), lease_expire TIMESTAMP)")
	if err != nil {
		return err
	}

	// Create the task table if not exists.
	// task {
	//	id INT PRIMARY KEY,
	// 	src VARCHAR(255), # srouce storage address
	//  src_bucket VARCHAR(255), # source storage bucket
	//  src_key VARCHAR(255), # source storage key
	// 	dst VARCHAR(255),
	// 	created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
	// 	updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP),
	//  unique key (src, dst),
	// }
	_, err = b.db.Exec("CREATE TABLE IF NOT EXISTS task (id INT PRIMARY KEY, src VARCHAR(255)," +
		"src_bucket VARCHAR(255), src_key VARCHAR(255), dst VARCHAR(255)," +
		"created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP," +
		"updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP," +
		"UNIQUE KEY (src, dst))")
	if err != nil {
		return err
	}

	// Insert the initial owner row.
	_, err = b.db.Exec("INSERT INTO owner (id, who, lease_expire) VALUES (1, '', '1970-01-01 00:00:00')")
	if err != nil {
		return err
	}

	return nil
}

// GetOwner returns the active owner of the tidb2dw service.
func (b *RdsBackend) GetOwner() (string, error) {
	var who string
	err := b.db.QueryRow("SELECT who FROM owner WHERE id = 1").Scan(&who)
	if err != nil {
		return "", err
	}
	return who, nil
}

// TryCampaignOwner tries to campaign the owner of the tidb2dw service.
func (b *RdsBackend) TryCampaignOwner(who string, leaseDurSec int) (bool, error) {
	res, err := b.db.Exec("UPDATE owner SET who = ?, lease_expire = DATE_ADD(NOW(), INTERVAL ? SECOND) WHERE id = 1 AND lease_expire < NOW()", who, leaseDurSec)
	if err != nil {
		return false, err
	}
	affected, err := res.RowsAffected()
	if err != nil {
		return false, err
	}
	return affected > 0, nil
}

// RenewOwnerLease renews the lease of the owner of the tidb2dw service.
func (b *RdsBackend) RenewOwnerLease(who string, leaseDurSec int) (bool, error) {
	res, err := b.db.Exec("UPDATE owner SET lease_expire = DATE_ADD(NOW(), INTERVAL ? SECOND) WHERE id = 1 AND who = ?", leaseDurSec, who)
	if err != nil {
		return false, err
	}
	affected, err := res.RowsAffected()
	if err != nil {
		return false, err
	}
	return affected > 0, nil
}
