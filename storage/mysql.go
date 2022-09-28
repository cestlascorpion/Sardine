package storage

import (
	"context"
	"database/sql"
	"fmt"
	"time"

	"github.com/cestlascorpion/sardine/utils"
	"github.com/jmoiron/sqlx"
	log "github.com/sirupsen/logrus"

	_ "github.com/go-sql-driver/mysql" // mysql driver
)

type MySQL struct {
	client   *sqlx.DB
	cancel   context.CancelFunc
	execSql  string
	querySql string
}

func NewMySQL(ctx context.Context, conf *utils.Config) (*MySQL, error) {
	client, err := sqlx.Open("mysql", conf.GetMySQLSource())
	if err != nil {
		log.Errorf("sqlx open err %+v", err)
		return nil, err
	}

	ctx, cancel := context.WithCancel(ctx)
	go func() {
		ticker := time.NewTicker(time.Minute)
		defer ticker.Stop()

		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				if err := client.PingContext(ctx); err != nil {
					log.Warnf("check ping err %+v", err)
				}
			}
		}
	}()

	return &MySQL{
		client:   client,
		cancel:   cancel,
		execSql:  fmt.Sprintf(`INSERT INTO leaf_alloc_%s (biz_tag, max_id, step) VALUES (?, ?, ?) ON DUPLICATE KEY UPDATE max_id=max_id+step`, conf.GetTable()),
		querySql: fmt.Sprintf(`SELECT biz_tag, max_id, step from leaf_alloc_%s where biz_tag = ?`, conf.GetTable()),
	}, nil
}

func (m *MySQL) UpdateMaxId(ctx context.Context, key string) (int64, error) {
	r := &record{}

	result, err := m.client.ExecContext(ctx, m.execSql, key, utils.DoNotChangeStep, utils.DoNotChangeStep)
	if err != nil {
		log.Errorf("sql exec context err %+v", err)
		return 0, err
	}
	changed, err := result.RowsAffected()
	if err != nil {
		log.Errorf("sql rows affected err %+v", err)
		return 0, err
	}
	if changed == 0 {
		log.Errorf("sql unexpect changed %d", changed)
		return 0, sql.ErrNoRows
	}

	err = m.client.GetContext(ctx, r, m.querySql, key)
	if err != nil {
		log.Errorf("sql get context err %+v", err)
		return 0, err
	}

	return r.MaxId, nil
}

func (m *MySQL) Close(ctx context.Context) error {
	return m.client.Close()
}

// ---------------------------------------------------------------------------------------------------------------------

type record struct {
	BizTag     string    `db:"biz_tag"`
	MaxId      int64     `db:"max_id"`
	Step       int64     `db:"step"`
	UpdateTime time.Time `db:"update_time"`
}

/**
CREATE TABLE `leaf_alloc_test` (
  `biz_tag` varchar(128)  NOT NULL DEFAULT '',
  `max_id` bigint(20) NOT NULL DEFAULT '1',
  `step` int(11) NOT NULL,
  `update_time` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  PRIMARY KEY (`biz_tag`)
) ENGINE=InnoDB;
*/

// ---------------------------------------------------------------------------------------------------------------------
