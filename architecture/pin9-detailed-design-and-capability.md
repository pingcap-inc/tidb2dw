# PIN-9 tidb2dw 重构详细设计与能力说明

Issue: PIN-9 / PIN-25
Status: current design and capability document
Last updated: 2026-06-09

相关文档：

- `architecture/pin9-target-architecture.md`
- `architecture/multi-tenant-task-runtime.md`
- `specs/ddl-coverage-contract-v1.md`
- `specs/multi-tenant-task-contract-v1.md`
- `specs/snowflake-s3-integration-e2e-v1.md`
- `docs/pin14-e2e-runbook.md`
- `qa/pin14-test-plan-and-report-template.md`

本文档回答这轮 tidb2dw 重构“改了什么、现在支持什么能力、怎么验证、还有什么风险”。所有测试证据均为脱敏摘要，不包含密钥、账号 URL、bucket、DSN 或原始敏感日志。

## 1. 总体结论

这轮重构把原来的单任务 Snowflake 复制路径改造成以 `tenant_id/task_id` 为隔离边界的运行模型，并把 DDL 支持范围从“笼统支持”收敛成可测试、可报告的 `DDL-V1-*` 场景矩阵。

当前可以给出的能力边界：

- 支持 Dumpling snapshot 文件 + TiCDC cloud-storage CDC 文件回放到 Snowflake 的任务级复制路径。
- 支持按 tenant/task 隔离 S3 snapshot、increment、checkpoint 目录，以及 Snowflake schema/stage/table 命名。
- 支持 v1 DDL 矩阵中的核心表生命周期、列演进、类型扩宽、truncate、metadata-only no-op、DDL/DML 交错和多租户隔离场景。
- 对 unsupported/deferred DDL 给出确定性失败或延后结论，不再把未验证能力写成已支持。
- 提供 deterministic fixture generator、validate/plan/run e2e runner、真实 S3/Snowflake runner、artifact scan、cleanup 边界和复跑入口。

发布建议按 PIN-15 脱敏结论：可以进入受控多租户试点；不建议声明全量 DDL 矩阵已经达到 GA 级真实 Snowflake/S3 覆盖。

## 2. Before / After 架构差异

| 维度 | 重构前 | 重构后 |
| --- | --- | --- |
| 任务边界 | 主要是单 CLI 任务，snapshot/increment 路径和 API 状态偏全局。 | `tenant_id/task_id` 成为任务所有权边界，`RuntimePlan` 统一派生存储、Snowflake、checkpoint、日志和指标标签。 |
| S3 布局 | 默认 `<storage>/snapshot` 和 `<storage>/increment`，多任务容易共享前缀。 | 默认 `<storage>/<prefix>/tenants/<tenant>/tasks/<task>/{snapshot,increment,checkpoints}`；`--legacy-layout` 保留旧布局。 |
| Snowflake stage | stage 名由 source database/table 简单拼接，跨租户/任务可能冲突。 | stage 名包含 tenant/task/table 作用域，snapshot 和 increment stage 分开。 |
| checkpoint | DML checkpoint 与增量数据同目录；没有明确 DDL checkpoint 作用域。 | 非 legacy 任务使用独立 checkpoint root；DML checkpoint 在 `checkpoints/increment/...`，DDL checkpoint 在 `checkpoints/ddl/<schema>/<table>/schema_<version>.checkpoint`。 |
| DDL 声明 | 文档中有较宽泛表述，缺少稳定场景 ID 和不支持边界。 | `specs/ddl-coverage-contract-v1.md` 固化 28 个 DDL 场景和 2 个 ordering/idempotency contract。 |
| 真实验证 | 缺少可复跑的 Snowflake/S3 fixture/e2e runner。 | 新增 `fixturegen`、`pin14-e2e`、wrapper、runbook、artifact scan 和 cleanup summary。 |
| 凭证处理 | S3 credential 曾通过 URI query 或 stage SQL 拼接路径暴露风险更高。 | S3 URI 去 query；主复制路径优先通过环境/secret provider 和 Snowflake storage integration；e2e artifact 必须扫描后才能作为证据。 |
| API 暴露 | cloud mode 默认监听非本地地址，鉴权边界不够明确。 | 默认监听 `127.0.0.1`；非本地地址要求 `TIDB2DW_API_TOKEN`，`/info` 和 `/metrics` 支持 token 检查，错误信息做 redaction。 |

## 3. 核心模块职责

| 模块 | 这轮改动/新增职责 |
| --- | --- |
| `pkg/taskruntime` | 定义 `TaskConfig`、`RuntimePlan`、`Controller`。先校验 tenant/task/source/staging/destination，再派生 S3 root、snapshot/increment/checkpoint URI、Snowflake schema/stage、日志和指标标签。 |
| `cmd/snowflake.go` | 新增 tenant/task/staging/env flags；把 legacy CLI 映射进 `taskruntime.BuildPlan`；用 `RuntimePlan` 创建 snapshot/increment connector，并把 checkpoint URI 传给增量 replay。 |
| `cmd/core.go` / `cmd/env.go` / `main.go` | 环境变量默认值、S3 URI 去 credential query、API token gate、`fixturegen` 和 `pin14-e2e` 命令注册。 |
| `replicate/snapshot.go` | snapshot loadinfo 改成写入当前 snapshot root 下，避免重复拼 `snapshot/`。 |
| `replicate/increment.go` | 增量 replay 支持独立 checkpoint store，增加 DML/DDL checkpoint path，已 checkpoint 的 DDL 可安全跳过并重新初始化 schema。 |
| `pkg/snowsql` | DDL 生成增强：create/drop/truncate/schema、add/drop/rename/modify column、类型扩宽校验、default/nullability、metadata-only no-op、unsupported/deferred deterministic error；统一 Snowflake identifier quoting；stage SQL 支持 storage integration。 |
| `pkg/testfixtures` | 生成 28 个 `DDL-V1-*` 场景的 Dumpling-style snapshot、TiCDC schema JSON、CDC CSV、mapping 和 manifest；默认生成两个 tenant/task identity。 |
| `pkg/pin14e2e` / `cmd/pin14_e2e.go` | 提供 `validate`、`plan`、`run` 三种模式；run mode 上传 fixture 到 S3、创建 Snowflake namespace/stage/table、COPY snapshot/CDC、MERGE、比较 schema/row count/checksum/selected rows、执行 scoped cleanup。 |
| `scripts/pin14-e2e.sh` | 真实 e2e wrapper：先校验配置，生成 fixture，运行 `pin14-e2e --mode run`，写 sanitized summary，并扫描 artifacts。 |
| `pkg/apiservice` | API error redaction，`/info` 和 `/metrics` token auth。 |
| `Makefile` / docs / specs / qa | 增加测试入口、runbook、DDL contract、multi-tenant contract、Snowflake/S3 e2e spec 和 QA 报告模板。 |

## 4. 生产数据流

生产复制路径的逻辑顺序如下：

1. 入口命令读取 CLI/env 配置，包括 source tables、Snowflake、S3、TiDB、TiCDC、tenant/task。
2. `taskruntime.BuildPlan` 校验配置。校验失败时直接返回，不能创建 TiDB/TiCDC/S3/Snowflake 外部副作用。
3. `BuildPlan` 派生任务作用域：
   - `StorageRootURI`
   - `SnapshotURI`
   - `IncrementURI`
   - `CheckpointURI`
   - `SnowflakeSchema`
   - per-table `SnapshotStage` / `IncrementStage`
4. Snapshot 阶段通过 Dumpling 写入 task-scoped snapshot URI，再通过 Snowflake external stage COPY 到目标表。
5. TiCDC cloud-storage sink 写入 task-scoped increment URI。
6. Increment replay 按 TiCDC schema file 和 DML file 的 table version 顺序处理：
   - 初始 schema file 初始化内存 schema。
   - 非空 DDL schema file 先转 Snowflake DDL 并写 DDL checkpoint。
   - DML CSV 通过 MERGE 写入 Snowflake，成功后写 DML checkpoint。
7. 任务日志和指标携带 `tenant_id`、`task_id`、`correlation_id`，便于观测和问题归属。

默认 S3 目录模型：

```text
<storage-uri>/<deployment-prefix>/tenants/<tenant-id>/tasks/<task-id>/
  snapshot/
  increment/
  checkpoints/
    increment/
    ddl/<source-schema>/<source-table>/schema_<table-version>.checkpoint
```

legacy 模式保留旧路径，用于兼容历史 CLI 运行；多租户部署默认不使用 legacy layout。

## 5. DDL 支持矩阵

说明：

- “真实 e2e”表示 PIN-18/PIN-15 中真实 S3/Snowflake run 已通过，且同一场景覆盖两个 tenant/task identity。
- “组件/fixture/plan”表示单元测试、DDL rewrite、fixture generation、validate/plan 已覆盖，但最终真实 S3/Snowflake run 未选择该 ID。
- “unsupported”表示必须确定性失败，不能静默写错数据或跳 checkpoint。
- “deferred”表示 v1 明确延后，不能作为已支持能力宣传。

| Scenario ID | v1 状态 | 场景 | 当前验证结论 |
| --- | --- | --- | --- |
| `DDL-V1-001` | supported | 初始 snapshot 建表与加载 | 真实 e2e PASS；schema、row count、checksum、selected rows 对比通过。 |
| `DDL-V1-002` | supported | CDC 后 `CREATE TABLE` 并写入新表 | 真实 e2e PASS；新表生命周期对比通过。 |
| `DDL-V1-003` | supported | `DROP TABLE` | 组件/fixture/plan PASS；最终真实 e2e 未选择。 |
| `DDL-V1-004` | deferred | `RENAME TABLE` | deferred；需要新任务绑定/表发现策略后再做。 |
| `DDL-V1-005` | supported | add nullable column | 真实 e2e PASS；新增列 schema、历史 NULL、新值对比通过。 |
| `DDL-V1-006` | supported | add NOT NULL column with default | 组件/fixture/plan PASS；最终真实 e2e 未选择。 |
| `DDL-V1-007` | supported | drop column | 组件/fixture/plan PASS；merge SQL 不再引用删除列。 |
| `DDL-V1-008` | supported | rename column | 组件/fixture/plan PASS；历史值保留和新列名覆盖由组件验证。 |
| `DDL-V1-009` | supported | widen varchar/char precision | 组件/fixture/plan PASS；缩窄不算 supported。 |
| `DDL-V1-010` | supported | widen decimal precision/scale | 组件/fixture/plan PASS。 |
| `DDL-V1-011` | unsupported | incompatible/narrowing type change | deterministic unsupported behavior PASS；不能静默截断。 |
| `DDL-V1-012` | supported | set NOT NULL when data valid | 组件/fixture/plan PASS。 |
| `DDL-V1-013` | unsupported | set NOT NULL while existing rows violate it | deterministic unsupported behavior PASS。 |
| `DDL-V1-014` | supported | drop NOT NULL | 组件/fixture/plan PASS。 |
| `DDL-V1-015` | supported | drop existing default | 组件/fixture/plan PASS。 |
| `DDL-V1-016` | deferred | change existing default | deferred；TiCDC emits row values，但 metadata rewrite/compare 未纳入 v1。 |
| `DDL-V1-017` | supported | secondary index metadata no-op | 组件/fixture/plan PASS；Snowflake 不创建物理索引。 |
| `DDL-V1-018` | deferred | unique key change | deferred；Snowflake 约束语义和 TiDB 不一致。 |
| `DDL-V1-019` | deferred | primary key change | deferred；merge key/dedup 依赖稳定 PK。 |
| `DDL-V1-020` | supported | `TRUNCATE TABLE` followed by inserts | 组件/fixture/plan PASS；最终真实 e2e 未选择。 |
| `DDL-V1-021` | supported | metadata-only partition DDL | 组件/fixture/plan PASS；作为 no-op。 |
| `DDL-V1-022` | deferred | destructive/data-moving partition DDL | deferred；需要逐项验证 CDC 输出和 Snowflake delete 语义。 |
| `DDL-V1-023` | supported | DDL interleaved with DML | 真实 e2e PASS；commit-order final state 对比通过。 |
| `DDL-V1-024` | unsupported | DDL before earlier dependent DML | deterministic unsupported behavior PASS；不能提前应用 DDL。 |
| `DDL-V1-025` | unsupported | duplicate/out-of-order DDL replay | deterministic unsupported behavior PASS；不能重复执行破坏性 DDL。 |
| `DDL-V1-026` | unsupported | unsupported TiDB type | deterministic unsupported behavior PASS；失败在 divergent schema 前。 |
| `DDL-V1-027` | supported | drop and recreate same-name table | 真实 e2e PASS；旧表数据不混入新表 identity。 |
| `DDL-V1-028` | supported | multiple tenant/task pairs with same table name | 真实 e2e PASS；schema/storage/checkpoint 隔离通过。 |

Ordering/idempotency 也纳入能力边界：

- `DDL-V1-ORDER-001`：snapshot 先于 CDC，schema file 先于 DML file，同一 flush interval 内按 TiDB commit order 得到最终状态。
- `DDL-V1-ORDER-002`：已完成 DML/DDL 可按 checkpoint 跳过；无法证明已完成的 duplicate/out-of-order DDL 必须失败。

## 6. 多租户 / 多任务支持方式

多租户和多任务不是靠调用方手工改路径，而是由 `RuntimePlan` 统一生成隔离命名：

| 隔离面 | 默认 owner | 当前策略 |
| --- | --- | --- |
| Snapshot files | tenant/task | `<root>/tenants/<tenant>/tasks/<task>/snapshot` |
| Increment files | tenant/task | `<root>/tenants/<tenant>/tasks/<task>/increment` |
| Checkpoints | tenant/task | `<root>/tenants/<tenant>/tasks/<task>/checkpoints` |
| Snowflake schema | tenant/task | 未显式配置时使用 tenant/task 派生 schema；显式配置共享 schema 时 stage 仍 task-scoped。 |
| Snowflake stages | tenant/task/table | snapshot 和 increment stage 分开，stage name 带 tenant/task/table 作用域。 |
| API lifecycle state | tenant/task | `Controller` 以 task record 管理 created/running/stopped/failed。 |
| 日志/指标 | tenant/task/correlation | 每条记录携带 tenant/task/correlation label。 |

`pkg/taskruntime.Controller` 支持状态机：

```text
created -> running -> stopped -> running
created -> running -> failed -> running
```

失败隔离规则：

- 一个 task Snowflake 写失败，只把该 task 标为 failed，不停止其它 running task。
- invalid config 在外部副作用前失败。
- resume 使用同一个 tenant/task checkpoint namespace。
- duplicate target/source table、路径穿越、非法 ID、缺少 credential ref、负并发值会被拒绝。

当前真实 Snowflake/S3 e2e 已验证两个 tenant/task identity。相同 tenant 下多个不同 task ID 的真实外部并发仍是 residual risk，目前主要由 `taskruntime` 组件测试覆盖。

## 7. Fixture 与 e2e runner 验证模型

### 7.1 fixturegen

`fixturegen` 由 `pkg/testfixtures` 实现，用同一个 seed 生成 deterministic 输入和期望输出：

- Dumpling-style snapshot CSV。
- Snapshot loadinfo。
- TiCDC initial schema JSON。
- TiCDC DDL schema JSON。
- TiCDC DML CSV。
- tenant/task mapping。
- 每个 scenario 的 validation manifest。

默认 e2e profile 生成：

- 28 个 `DDL-V1-*` scenario。
- 2 个 tenant/task identity。
- 合计 56 个 manifest。

每个 manifest 包含最终 schema、row count、checksum、selected rows 和 expected error/deferred marker。这样 runner 不需要连接 TiDB 才能验证 Snowflake 最终状态。

### 7.2 pin14-e2e validate/plan/run

`pin14-e2e` 有三个模式：

| 模式 | 外部副作用 | 行为 |
| --- | --- | --- |
| `validate` | 无 | 校验 config 名称、S3 URI 是否无 query/credential、staging prefix 是否安全、Snowflake identifier 是否 clean。 |
| `plan` | 无 | 读取 manifest，检查文件存在和 scenario scope 唯一，生成 S3 object layout 和 Snowflake object plan。 |
| `run` | 有 | 懒加载真实 S3/Snowflake client，上传 fixture，创建 namespace/stage/table，COPY snapshot/CDC，MERGE，比较结果，按 cleanup mode 清理。 |

真实 runner 的执行路径：

1. `scripts/pin14-e2e.sh` 校验必要 env name。
2. wrapper 生成 fixture 到 artifact 目录。
3. wrapper 调 `tidb2dw pin14-e2e --mode run`。
4. `Runner.Run` 再次 validate config 和 manifest。
5. 仅在 validate/plan 成功后构建真实 S3/Snowflake client。
6. S3 client 上传 snapshot、increment、mapping 文件到 scoped scenario root。
7. Snowflake client 创建 database/schema、external stages、目标表和临时表。
8. Snowflake COPY snapshot CSV 到临时表，再插入目标表 common columns。
9. Snowflake COPY CDC CSV 到临时表，再按 id 和 commit_ts 做 MERGE。
10. Runner 读取 Snowflake 最终表，比较 final schema columns、row count、checksum、selected rows。
11. cleanup 删除 scoped S3 prefix 和 task-owned Snowflake stages/tables。
12. wrapper 扫描 artifacts，发现 secret-like material 时失败。

注意：当前真实 e2e 验证的是 generated Dumpling/CDC fixture -> S3 -> Snowflake replay，不是 live TiDB/TiCDC service 链路压测。它覆盖了复制格式、DDL/DML replay、Snowflake 写入、S3/Snowflake cleanup 和隔离，但不等同于完整源端集群运行验证。

## 8. 真实 Snowflake/S3 测试证据摘要

PIN-15 脱敏测试报告给出的最终证据：

| 项目 | 结果 |
| --- | --- |
| `bash -n scripts/pin14-e2e.sh` | PASS |
| `bash -n scripts/pin14-e2e-scan-test.sh` | PASS |
| `scripts/pin14-e2e-scan-test.sh` | PASS |
| `go test ./pkg/pin14e2e ./pkg/testfixtures ./pkg/taskruntime -count=1` | PASS |
| `make build` | PASS |
| `make test` | PASS |
| `fixturegen --profile e2e --seed 140 --scenarios all` | PASS，56 manifests，28 DDL IDs x 2 tenants |
| `pin14-e2e --mode validate --scenarios all` | PASS，56 planned entries，无外部副作用 |
| `pin14-e2e --mode plan --scenarios all` | PASS，56 planned entries，cleanup counts 0 |
| 真实 `scripts/pin14-e2e.sh` | PASS，exit 0 |
| 真实 scenario set | `DDL-V1-001,DDL-V1-002,DDL-V1-005,DDL-V1-023,DDL-V1-027,DDL-V1-028` |
| 真实 scenario results | 12/12 passed，0 failures |
| 真实比较内容 | schema columns、row count、deterministic checksum、selected rows |
| cleanup | S3 objects deleted: 106；Snowflake tables dropped: 4；Snowflake stages dropped: 24 |
| residue check | scoped S3 remaining keys 0；Snowflake test tables 0；stages 0 |
| sanitization | wrapper scan PASS；configured-secret value hits 0 |

真实 e2e 样本结论：

- baseline/lifecycle/isolation final schema: `id,tenant_id,status,amount,created_at`。
- add/interleaved final schema: `id,tenant_id,status,amount,created_at,coupon_code`。
- drop/recreate final schema: `id,tenant_id,status`。
- baseline/add/interleaved/isolation row count: 2997 per tenant。
- drop/recreate row count: 1999 per tenant。

这些证据证明高价值 supported 场景和 tenant/task 隔离可以在真实 Snowflake/S3 上通过。尚未证明每个 supported DDL ID 都有真实外部 run 证据。

## 9. 关键配置项清单

### 9.1 复制命令配置

| 配置 | 说明 |
| --- | --- |
| `--tenant.id` / `TIDB2DW_TENANT_ID` | task owner tenant ID，默认 `default`。 |
| `--task.id` / `TIDB2DW_TASK_ID` | task ID，默认 `default`。 |
| `--staging.prefix` / `TIDB2DW_STAGING_PREFIX` | tenant/task scope 前的部署级前缀。 |
| `--legacy-layout` | 保留旧 `<storage>/snapshot`、`<storage>/increment` 布局；多租户默认不使用。 |
| `--storage` | object storage root；S3 URI 不能包含 credential query。 |
| `--snowflake.storage-integration` / `TIDB2DW_SNOWFLAKE_STORAGE_INTEGRATION` | 生产 Snowflake external stage 推荐使用的 S3 授权方式。 |
| `TIDB2DW_TIDB_*` | TiDB host/port/user/password/SSL CA 的环境变量入口。 |
| `TIDB2DW_CDC_*` | TiCDC host/port 的环境变量入口。 |
| `TIDB2DW_SNOWFLAKE_*` | Snowflake account/user/password/database/schema/warehouse/storage integration 的环境变量入口。 |
| `TIDB2DW_API_TOKEN` | 非本地 API listen 地址必须设置；用于 `/info` 和 `/metrics` 鉴权。 |

### 9.2 PIN-14 e2e 配置

| 配置组 | 只记录名称，不记录值 |
| --- | --- |
| Snowflake | `PIN14_SNOWFLAKE_ACCOUNT_ID`、`PIN14_SNOWFLAKE_USER`、`PIN14_SNOWFLAKE_PASSWORD`、`PIN14_SNOWFLAKE_DATABASE`、`PIN14_SNOWFLAKE_SCHEMA`、`PIN14_SNOWFLAKE_WAREHOUSE`、`PIN14_SNOWFLAKE_STORAGE_INTEGRATION` |
| S3/AWS | `AWS_ACCESS_KEY_ID`、`AWS_SECRET_ACCESS_KEY`、`AWS_SESSION_TOKEN`、`AWS_REGION`、`PIN14_AWS_REGION`、`PIN14_S3_STORAGE_URI`、`PIN14_STAGING_PREFIX` |
| Source labels | `PIN14_TIDB_HOST`、`PIN14_TIDB_PORT`、`PIN14_TIDB_USER`、`PIN14_TIDB_PASSWORD`、`PIN14_CDC_HOST`、`PIN14_CDC_PORT` |
| Run identity | `PIN14_ENV_LABEL`、`PIN14_TENANT_ID`、`PIN14_TASK_ID`、`PIN14_TABLES`、`PIN14_SCENARIOS`、`PIN14_CLEANUP_MODE`、`PIN14_E2E_MODE` |
| Artifacts | `PIN14_ARTIFACT_DIR`、`PIN14_E2E_RUN_ID`、`PIN14_FIXTURE_DIR`、`PIN14_MANIFEST_PATH`、`PIN14_FIXTURE_PROFILE`、`PIN14_FIXTURE_SEED` |

## 10. 运维与复跑入口

本地无外部副作用验证：

```bash
make test
make build
./bin/tidb2dw fixturegen --profile e2e --seed 140 --scenarios all --output-dir /tmp/tidb2dw-pin9-fixtures
./bin/tidb2dw pin14-e2e --mode validate --fixture-dir /tmp/tidb2dw-pin9-fixtures --scenarios all
./bin/tidb2dw pin14-e2e --mode plan --fixture-dir /tmp/tidb2dw-pin9-fixtures --scenarios all
```

真实 Snowflake/S3 复跑：

```bash
make build
PIN14_E2E_MODE=validate scripts/pin14-e2e.sh
PIN14_E2E_MODE=run scripts/pin14-e2e.sh
```

操作约束：

- 真实 secret 只能来自 shell、CI secret 或 approved secret store。
- 不要把 secret、bucket、账号 URL、DSN、presigned URL 写进命令、文档、日志附件或 issue 评论。
- `validate` 必须先跑，且不产生外部副作用。
- `run` 后只有 artifact scan PASS 的摘要和证据可以发布。
- cleanup 只能删除 scoped tenant/task prefix 和 task-owned Snowflake objects，不能扩大到父 prefix、共享 schema 或共享 database。

## 11. 当前已验证能力

已验证为 PASS 的能力：

- `make test` 和 `make build` 在当前测试树通过。
- `pkg/taskruntime` 覆盖 task config validation、path traversal rejection、duplicate target/source rejection、schema/stage/checkpoint naming、state transition、per-tenant/global concurrency、失败隔离。
- `pkg/snowsql` 覆盖 DDL rewrite、unsupported/deferred deterministic errors、identifier quoting、merge SQL 不引用 dropped/renamed stale columns、stage SQL 不把主复制路径 S3 secret 写入 SQL。
- `pkg/testfixtures` 可为 28 个 DDL ID x 2 tenants 生成 manifest。
- `pkg/pin14e2e` 覆盖 validate/plan/run、manifest 文件安全、scenario scope 唯一、真实 client lazy construction、cleanup summary、sanitized error。
- 真实 Snowflake/S3 runner 对 6 个高价值 supported DDL ID x 2 tenants 通过 12/12，并完成 scoped cleanup 与 secret scan。

## 12. 已知限制与 residual risk

| 风险 | 当前状态 | 建议 |
| --- | --- | --- |
| 真实外部覆盖不是全矩阵 | 最终真实 e2e 选择了 6 个 supported ID；其它 supported ID 是组件/fixture/plan 证据。 | 受控试点可以接受；GA 前扩展真实 Snowflake/S3 run 到剩余 supported ID。 |
| tested tree 是 dirty worktree | PIN-15 记录 build label 含 dirty。 | 打包或提交精确 tested worktree，再从 clean commit rerun CI/e2e。 |
| same-tenant 多 task 真实外部运行 | 组件测试覆盖；最终真实 e2e 是两个 tenants 使用同 task ID。 | 补一轮 same-tenant 不同 task ID 的真实 S3/Snowflake isolation run。 |
| live TiDB/TiCDC 源链路 | 当前真实 run 使用 deterministic fixture 模拟 Dumpling/CDC 文件。 | 需要生产前源端链路验证时，增加 live TiDB/TiCDC run 或 CI job。 |
| full hosted multi-task API admission | `Controller` 和 contracts 已具备，CLI 已接入 RuntimePlan；完整 config API/scheduler 仍需按部署形态落地。 | 试点先用受控任务入口；后续补 API config endpoint、per-task status 和 admission control e2e。 |
| deferred DDL | table rename、existing default change、unique/primary key change、destructive partition DDL 不在 v1。 | 文档和用户沟通必须明确；遇到这些 DDL 不得当作成功复制。 |
| unsupported DDL | narrowing/incompatible type、not-null violation、duplicate/out-of-order DDL、unsupported TiDB type。 | 保持 deterministic failure，不写跳过 unresolved work 的 checkpoint。 |
| 早期外部凭证曾出现在 issue 评论 | 本文档不复述；runner/report 不写入这些值。 | 测试凭据应轮换或收敛到 least privilege。 |

## 13. 给用户的能力说明口径

可以说：

- tidb2dw 现在有了明确的 tenant/task 隔离模型，S3、Snowflake stage/schema/checkpoint、日志和指标都按任务作用域设计。
- 已经支持 v1 DDL 矩阵中的核心 supported 场景，特别是建表、删表、删后重建、truncate、加/删/改/重命名列、类型扩宽、合法 nullability/default 变更、metadata-only no-op、DDL/DML 交错、多租户隔离。
- 已经有 deterministic fixture、无副作用 validate/plan、真实 S3/Snowflake run、cleanup 和 artifact scan。
- 最终真实 run 已在两个 tenant/task identity 上通过 12/12 selected scenario results，并确认 cleanup 无残留。

不能说：

- 不能说所有 28 个 DDL ID 都已经在真实 Snowflake/S3 上跑过。
- 不能说 table rename、existing default change、unique/primary key change、destructive partition DDL 已支持。
- 不能说 live TiDB/TiCDC 源端集群链路已完整压测。
- 不能在任何交付物中写 secret、账号 URL、bucket、DSN 或原始日志。

## 14. PIN-15 后续补充位

PIN-15 已经产出最终测试报告和 release readiness decision，当前结论是受控试点 conditional go、全矩阵 GA no-go。后续如果 PIN-15 被更新为 clean commit 复跑或扩展真实场景覆盖，应在本文档补充：

- clean commit / release artifact label。
- 新增真实 Snowflake/S3 scenario IDs。
- same-tenant multi-task 外部隔离结果。
- live TiDB/TiCDC 源链路结果。
- cleanup 和 sanitization 最新摘要。
