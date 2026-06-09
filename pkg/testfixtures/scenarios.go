package testfixtures

func scenarioSpecMap() map[string]ScenarioSpec {
	specs := []ScenarioSpec{
		{"DDL-V1-001", "supported", "initial snapshot of orders", nil},
		{"DDL-V1-002", "supported", "create table refunds after CDC starts", []string{"CREATE TABLE refunds (id BIGINT PRIMARY KEY, order_id BIGINT, amount DECIMAL(18,2))"}},
		{"DDL-V1-003", "supported", "drop table orders after pending DML flushes", []string{"DROP TABLE orders"}},
		{"DDL-V1-004", "deferred", "rename table orders to orders_archive", []string{"RENAME TABLE orders TO orders_archive"}},
		{"DDL-V1-005", "supported", "add nullable coupon_code column", []string{"ALTER TABLE orders ADD COLUMN coupon_code VARCHAR(64) NULL"}},
		{"DDL-V1-006", "supported", "add not-null source column with default", []string{"ALTER TABLE orders ADD COLUMN source VARCHAR(16) NOT NULL DEFAULT 'web'"}},
		{"DDL-V1-007", "supported", "drop status column", []string{"ALTER TABLE orders DROP COLUMN status"}},
		{"DDL-V1-008", "supported", "rename amount to gross_amount", []string{"ALTER TABLE orders RENAME COLUMN amount TO gross_amount"}},
		{"DDL-V1-009", "supported", "widen status varchar precision", []string{"ALTER TABLE orders MODIFY COLUMN status VARCHAR(128)"}},
		{"DDL-V1-010", "supported", "widen amount decimal precision and scale", []string{"ALTER TABLE orders MODIFY COLUMN amount DECIMAL(20,4)"}},
		{"DDL-V1-011", "unsupported", "narrow status varchar precision", []string{"ALTER TABLE orders MODIFY COLUMN status VARCHAR(8)"}},
		{"DDL-V1-012", "supported", "set status not null after data is non-null", []string{"ALTER TABLE orders MODIFY COLUMN status VARCHAR(32) NOT NULL"}},
		{"DDL-V1-013", "unsupported", "set status not null while existing rows contain null", []string{"ALTER TABLE orders MODIFY COLUMN status VARCHAR(32) NOT NULL"}},
		{"DDL-V1-014", "supported", "drop status not-null constraint", []string{"ALTER TABLE orders MODIFY COLUMN status VARCHAR(32) NULL"}},
		{"DDL-V1-015", "supported", "drop existing default", []string{"ALTER TABLE orders ALTER COLUMN status DROP DEFAULT"}},
		{"DDL-V1-016", "deferred", "change existing default", []string{"ALTER TABLE orders ALTER COLUMN status SET DEFAULT 'new'"}},
		{"DDL-V1-017", "supported", "secondary index metadata no-op", []string{"ALTER TABLE orders ADD INDEX idx_status (status)"}},
		{"DDL-V1-018", "deferred", "unique key change", []string{"ALTER TABLE orders ADD UNIQUE KEY uk_status (status)"}},
		{"DDL-V1-019", "deferred", "primary key change", []string{"ALTER TABLE orders DROP PRIMARY KEY"}},
		{"DDL-V1-020", "supported", "truncate table followed by inserts", []string{"TRUNCATE TABLE orders"}},
		{"DDL-V1-021", "supported", "empty partition maintenance metadata no-op", []string{"ALTER TABLE orders ADD PARTITION (PARTITION p202606 VALUES LESS THAN (TO_DAYS('2026-07-01')))"}},
		{"DDL-V1-022", "deferred", "partition DDL with data movement or deletion", []string{"ALTER TABLE orders TRUNCATE PARTITION p202606"}},
		{"DDL-V1-023", "supported", "DDL interleaved with high-volume DML", []string{"ALTER TABLE orders ADD COLUMN coupon_code VARCHAR(64) NULL"}},
		{"DDL-V1-024", "unsupported", "DDL arrives before earlier dependent DML files", []string{"ALTER TABLE orders ADD COLUMN late_col VARCHAR(32)"}},
		{"DDL-V1-025", "unsupported", "duplicate or out-of-order schema version replay", []string{"ALTER TABLE orders ADD COLUMN duplicate_col VARCHAR(32)"}},
		{"DDL-V1-026", "unsupported", "unsupported TiDB column type", []string{"ALTER TABLE orders ADD COLUMN payload MEDIUMBLOB"}},
		{"DDL-V1-027", "supported", "drop and recreate same-name table", []string{"DROP TABLE orders", "CREATE TABLE orders (id BIGINT PRIMARY KEY, tenant_id BIGINT NOT NULL, status VARCHAR(32))"}},
		{"DDL-V1-028", "supported", "tenant and task isolation for same unqualified table name", []string{"ALTER TABLE orders ADD COLUMN coupon_code VARCHAR(64) NULL"}},
	}
	result := make(map[string]ScenarioSpec, len(specs))
	for _, spec := range specs {
		result[spec.ID] = spec
	}
	return result
}
