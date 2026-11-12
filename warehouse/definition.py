"""关系结构定义。

SCHEMA 只登记对外可装载的事实表；快照、暂存、重建台账与设置表由
``catalog.connect`` 自行创建，不接受外部装载。
"""

SCHEMA = {
    'movements': (
        'movement_id TEXT PRIMARY KEY, '
        'warehouse TEXT NOT NULL, '
        'sku TEXT NOT NULL, '
        'lot TEXT NOT NULL, '
        'event_time TEXT NOT NULL, '
        'day TEXT NOT NULL, '
        'receive_seq INTEGER NOT NULL UNIQUE, '
        'quantity TEXT NOT NULL, '
        'reversal_of TEXT REFERENCES movements(movement_id), '
        'CHECK (receive_seq > 0)'
    ),
    'counts': (
        'count_id TEXT PRIMARY KEY, '
        'warehouse TEXT NOT NULL, '
        'sku TEXT NOT NULL, '
        'lot TEXT NOT NULL, '
        'cutoff_time TEXT NOT NULL, '
        'cutoff_day TEXT NOT NULL, '
        'version INTEGER NOT NULL, '
        'quantity TEXT NOT NULL, '
        'CHECK (version >= 1), '
        'UNIQUE (warehouse, sku, lot, cutoff_day, version)'
    ),
}

DAILY_COLUMNS = (
    'warehouse TEXT NOT NULL, '
    'sku TEXT NOT NULL, '
    'lot TEXT NOT NULL, '
    'day TEXT NOT NULL, '
    'eod_quantity TEXT, '
    'inbound TEXT NOT NULL, '
    'outbound TEXT NOT NULL, '
    'status TEXT NOT NULL, '
    'basis TEXT NOT NULL'
)

MONTHLY_COLUMNS = (
    'warehouse TEXT NOT NULL, '
    'sku TEXT NOT NULL, '
    'lot TEXT NOT NULL, '
    'month TEXT NOT NULL, '
    'opening_quantity TEXT, '
    'eod_quantity TEXT, '
    'avg_daily_quantity TEXT, '
    'inbound TEXT NOT NULL, '
    'outbound TEXT NOT NULL, '
    'count_adjustment TEXT, '
    'known_days INTEGER NOT NULL, '
    'unknown_days INTEGER NOT NULL, '
    'status TEXT NOT NULL, '
    'basis TEXT NOT NULL'
)

INTERNAL_TABLES = {
    'daily_snapshots': f'CREATE TABLE IF NOT EXISTS daily_snapshots ({DAILY_COLUMNS}, PRIMARY KEY (warehouse, sku, lot, day))',
    'monthly_snapshots': f'CREATE TABLE IF NOT EXISTS monthly_snapshots ({MONTHLY_COLUMNS}, PRIMARY KEY (warehouse, sku, lot, month))',
    'daily_staging': f'CREATE TABLE IF NOT EXISTS daily_staging (run_id TEXT NOT NULL, {DAILY_COLUMNS}, PRIMARY KEY (run_id, warehouse, sku, lot, day))',
    'monthly_staging': f'CREATE TABLE IF NOT EXISTS monthly_staging (run_id TEXT NOT NULL, {MONTHLY_COLUMNS}, PRIMARY KEY (run_id, warehouse, sku, lot, month))',
    'rebuild_runs': (
        'CREATE TABLE IF NOT EXISTS rebuild_runs ('
        'run_id TEXT PRIMARY KEY, '
        'status TEXT NOT NULL, '
        'plan TEXT NOT NULL, '
        'cursor_key TEXT, '
        'cursor_day TEXT, '
        'fact_digest TEXT NOT NULL, '
        'started_at TEXT NOT NULL, '
        'finished_at TEXT, '
        'detail TEXT)'
    ),
    'settings': 'CREATE TABLE IF NOT EXISTS settings (name TEXT PRIMARY KEY, value TEXT NOT NULL)',
    'ingest_batches': 'CREATE TABLE IF NOT EXISTS ingest_batches (batch TEXT PRIMARY KEY, digest TEXT NOT NULL, rows INTEGER NOT NULL)',
}

INDEXES = (
    'CREATE INDEX IF NOT EXISTS idx_daily_key_day ON daily_snapshots (warehouse, sku, lot, day)',
    'CREATE INDEX IF NOT EXISTS idx_monthly_key ON monthly_snapshots (warehouse, sku, lot, month)',
    'CREATE INDEX IF NOT EXISTS idx_daily_staging_run ON daily_staging (run_id, warehouse, sku, lot, day)',
    'CREATE INDEX IF NOT EXISTS idx_monthly_staging_run ON monthly_staging (run_id, warehouse, sku, lot, month)',
    'CREATE INDEX IF NOT EXISTS idx_movements_key_day ON movements (warehouse, sku, lot, day)',
    'CREATE INDEX IF NOT EXISTS idx_counts_key_day ON counts (warehouse, sku, lot, cutoff_day)',
)
