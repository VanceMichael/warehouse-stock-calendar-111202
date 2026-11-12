# stock-calendar

库存集市的周期快照核算。旧月表把每天库存量相加当成“月库存”，已停用；本系统把
**月末余额、日均库存、入出库量**作为三个独立指标表达，并保留每个余额的来源解释。

## 语义约定

### 日历边界、业务时区与盘点时刻

- 业务时区为 IANA 时区，库级固定（默认 `Asia/Shanghai`，建库后不可更改）。
- 流水/盘点时刻必须是**带偏移的 ISO-8601**文本；业务日由时刻按业务时区换算，
  不按 UTC 切日（例如 UTC 10-01 17:30 在上海归属 10-02）。
- 日末快照对应业务时区自然日 **24:00（次日 00:00）**的持有状态；无流水的日子
  同样生成持有（held）行，不跳过。
- 盘点（count）是事实，带 `cutoff_time` 与业务日 `cutoff_day`、版本号 `version`；
  盘点数量直接锚定该业务日的日末余额，同日流水仍计入当日入出库量。同一业务日
  存在多个版本时取最大版本，旧版本保留以便追溯。

### 余额、未知与负库存

- 首个有效盘点覆盖之前没有可靠基准：日末余额为 `unknown`（SQL 中为 NULL），
  **不是 0**；当日入出库量照常记录。
- 负库存如实保留，状态标 `negative`，绝不截断为零。
- 盘点相对“上一日余额 + 当日净流水”的差额记为 `count_adjustment`（盘盈为正、
  盘亏为负），并写入当日与月层的来源解释。
- 每行快照带 `basis` JSON：`unknown | count | held | movement`，含涉及的
  `movement_ids`、盘点 `count_id/version/cutoff_time` 与调整额。

### 流水规则

- 库存键为（仓库 `warehouse`、货品 `sku`、批次 `lot`）。
- 流水有唯一 `movement_id`、`event_time`、接收顺序 `receive_seq`、正负数量
  （正为入库、负为出库，数量不可为 0）。
- 同一批次（batch）重传幂等：内容 SHA-256 相同不重复写入，内容不同则报错；
  装载以完整输入包为事务，主键冲突不留下半批记录。
- 冲销记录通过 `reversal_of` 指向原流水：必须符号相反、原流水存在、冲销记录
  不能再被冲销，且累计冲销量（含已落库的历史批次）不得超过原量。

### 影响区间与可恢复重建

- 迟到流水、冲销或更正盘点会改变一段日末快照。规划器按盘点日把每个键的活跃
  跨度切段：下一个有效盘点日作为新段起点，旧段结束于盘点日前一天——因此任何
  变动的影响只重算到**下个有效盘点为止**，盘点之后的段独立承接。
- 重建流程：规划区间 → 建 `rebuild_runs` 任务（记录事实摘要 `fact_digest`）→
  按键分事务写 `daily_staging/monthly_staging` → 单事务整月替换发布表。
- 读取者不会看到只修了一半日期的月份：发布在一个事务内按整月删除并插入。
- 任务中断后保持 `running`，staging 已提交部分保留，可凭 `run_id` 幂等恢复
  （`resume`）；恢复前若事实再次变化（摘要不符）则拒绝，要求重新规划。
- 重建范围默认从最早事实日到事实水印日，可用 `--as-of` 显式指定核算截至日，
  以便把持有状态延伸到最近事实之后。

### 月层半可加指标

月行（`monthly_snapshots`）与日末量分别表达、可交叉核对：

| 字段 | 含义 |
| --- | --- |
| `opening_quantity` | 上月最后活跃日的日末余额；首盘点前为 NULL |
| `eod_quantity` | 本月最后活跃日的日末余额（即月末余额） |
| `avg_daily_quantity` | 活跃日内日末余额的算术平均，6 位小数半进位；未知日不参与分子但计入分母 |
| `inbound / outbound` | 本月日入/出库量之和（半可加：余额不可跨仓库相加，流量可以） |
| `count_adjustment` | 本月各盘点调整额之和 |
| `known_days / unknown_days` | 活跃日中已知/未知余额的天数 |
| `status` | `ok` / `partial`（含未知日）/ `negative`（出现过负库存） |

守恒恒等式（开盘与月末均已知时，`basis.conservation` 为 true）：

```
opening + inbound - outbound + count_adjustment = eod
```

## 数据组织

事实表（对外可装载，结构见 `schema.json`，定义在 `warehouse/definition.py`）：

- `movements(movement_id PK, warehouse, sku, lot, event_time, day, receive_seq UNIQUE, quantity, reversal_of)`
- `counts(count_id PK, warehouse, sku, lot, cutoff_time, cutoff_day, version, quantity, UNIQUE(warehouse, sku, lot, cutoff_day, version))`

内部表（不接受外部装载）：

- `daily_snapshots` / `monthly_snapshots`：已发布的日/月快照
- `daily_staging` / `monthly_staging`：按 `run_id` 隔离的重建暂存
- `rebuild_runs`：重建任务台账（状态、区间计划、事实摘要、游标、时间）
- `settings`：库级业务时区；`ingest_batches`：批次内容摘要

关系数据保存在 SQLite，精确数量用十进制文本；导出对象按内容 SHA-256 命名
（`warehouse/store.py`），读取时重新校验。

## 模块

- `warehouse/clock.py`：业务时区、时刻解析、日历/月份边界
- `warehouse/ledger.py`：装载前的流水与盘点语义校验
- `warehouse/snapshots.py`：区间规划、日/月快照构建、暂存发布与恢复
- `warehouse/domain.py`：查询视图（`overview` 仅为流水净变动，不是余额）
- `warehouse/catalog.py`：事务装载与连接管理

## 环境与验证

Python 3.11 以上，仅标准库（`zoneinfo` 使用系统 IANA 时区库），无第三方依赖。

```bash
python3 -m compileall -q warehouse tests scripts
python3 -m unittest discover -s tests -v
```

测试不只比较流水净额：43 个用例覆盖无交易日持有行、盘点覆盖旧误差、跨月补录、
冲销超量/反向/嵌套拒绝、负库存、失败中断后的恢复守恒、日末量与月指标交叉核对、
业务时区切日等。

## 运行

```bash
# 真实导入、失败注入与恢复、跨月补录、守恒校验、导出往返的完整演示
python3 scripts/demo.py

# 分步调用
python3 -m warehouse import  --db /tmp/stock.sqlite3 --input examples/source.json --batch first
python3 -m warehouse rebuild --db /tmp/stock.sqlite3 --as-of 2026-11-08
python3 -m warehouse inspect --db /tmp/stock.sqlite3 --view daily   --month 2026-11
python3 -m warehouse inspect --db /tmp/stock.sqlite3 --view monthly
python3 -m warehouse inspect --db /tmp/stock.sqlite3 --view runs
python3 -m warehouse export  --db /tmp/stock.sqlite3 --objects /tmp/objects --view monthly

# 业务补录上月流水（新批次，不重跑最近一天）
python3 -m warehouse import  --db /tmp/stock.sqlite3 --input examples/source_backfill.json --batch backfill
python3 -m warehouse rebuild --db /tmp/stock.sqlite3

# 失败后恢复
python3 -m warehouse rebuild --db /tmp/stock.sqlite3 --failure-point midway   # 产生 running 任务
python3 -m warehouse resume  --db /tmp/stock.sqlite3 --run-id run-xxxx
```

## 样例数据

- `examples/source.json`：首批事实（10 月至 11 月流水 + 两次盘点），含同日多条
  流水（按 `receive_seq` 定序）。
- `examples/source_backfill.json`：业务补录的 10 月迟到流水。补录重建后，影响
  止于 11-05 盘点：10 月末余额由 9 变为 11，11 月开盘承接 11，盘点把补录误差
  吸收为调整 `-2`，11-05 之后余额不变，月层守恒恒等式仍闭合。
- `examples/expected.json`：`overview` 视图（流水净变动）的公开样例，仅回答
  净变动，不代表期末余额。
