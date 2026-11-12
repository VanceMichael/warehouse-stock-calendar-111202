"""周期快照引擎：日末余额、月度半可加指标与可恢复的区间重建。

语义约定（与 README 对应）：

1. 业务日由流水/盘点时刻按业务时区换算，日历边界为业务时区自然日，
   日末快照对应自然日 24:00 的持有状态，无流水日同样生成持有记录。
2. 盘点是事实：同一业务日取版本号最大的盘点，盘点数量直接锚定该日
   日末余额；其相对“上一日余额 + 当日净流水”的差额记为盘点调整。
3. 首个有效盘点覆盖之前没有可靠基准，日末余额标 ``unknown``（不是 0）；
   负库存如实保留并标 ``negative``，不截断为零。
4. 迟到流水、冲销或更正盘点只重算“变动起点 → 下个有效盘点日（含）”
   的影响区间；没有下一个盘点则延伸到当前水印日。
5. 重建先按键分事务写 staging，再在单事务内整月替换发布表；读取者
   只能看到全部月份都修订完成的状态。中断后 run 保持 running，可按
   run_id 幂等重试（INSERT OR REPLACE），最后统一发布。
"""

import hashlib
import json
import uuid
from collections import defaultdict
from datetime import date, timedelta
from decimal import Decimal, ROUND_HALF_UP

from .catalog import encode, transaction, utc_now_text
from .clock import iter_days, month_bounds, month_key, parse_day
from .ledger import parse_quantity

ZERO = Decimal('0')
AVG_QUANT = Decimal('0.000001')
_NOT_BUILT = object()
KEY_FIELDS = ('warehouse', 'sku', 'lot')
DAILY_TABLE = 'daily_snapshots'
MONTHLY_TABLE = 'monthly_snapshots'
DAILY_PUBLISHED_COLS = (
    'warehouse, sku, lot, day, eod_quantity, inbound, outbound, status, basis'
)
MONTHLY_PUBLISHED_COLS = (
    'warehouse, sku, lot, month, opening_quantity, eod_quantity, avg_daily_quantity, '
    'inbound, outbound, count_adjustment, known_days, unknown_days, status, basis'
)


def _decimal(text):
    return parse_quantity(text, allow_zero=True)


def _q(value):
    return str(value.quantize(AVG_QUANT, rounding=ROUND_HALF_UP))


def _key_of(row):
    return tuple(row[field] for field in KEY_FIELDS)


def load_facts(connection):
    movements = defaultdict(lambda: defaultdict(list))
    for record in connection.execute('SELECT * FROM movements ORDER BY receive_seq'):
        movements[_key_of(record)][parse_day(record['day'])].append(dict(record))
    counts = defaultdict(lambda: defaultdict(dict))
    for record in connection.execute('SELECT * FROM counts ORDER BY count_id'):
        counts[_key_of(record)][parse_day(record['cutoff_day'])][record['version']] = dict(record)
    return movements, counts


def all_keys(connection):
    keys = set()
    for table in ('movements', 'counts'):
        for record in connection.execute(f'SELECT DISTINCT warehouse, sku, lot FROM {table}'):
            keys.add(_key_of(record))
    return keys


def fact_digest(connection):
    payload = b''
    for table in ('movements', 'counts'):
        rows = [dict(r) for r in connection.execute(f'SELECT * FROM {table} ORDER BY 1')]
        payload += table.encode() + encode(rows)
    return hashlib.sha256(payload).hexdigest()


def _watermark(connection):
    latest = None
    for table, column in (('movements', 'day'), ('counts', 'cutoff_day')):
        record = connection.execute(f'SELECT MAX({column}) AS m FROM {table}').fetchone()
        if record and record['m']:
            candidate = parse_day(record['m'])
            latest = candidate if latest is None or candidate > latest else latest
    return latest


def plan_rebuild(connection, explicit_ranges=None, as_of=None):
    """按盘点日把每个键的活跃跨度切成影响区间。

    每个键从“最早事实日”重建到“核算截至日 as_of”（默认取事实水印）。
    切段规则：下一个有效盘点日作为新段的起点（盘点锚定该日余额），
    旧段结束于盘点日前一天——段内基准始终连续，迟到流水/冲销的影响
    只延伸到下一盘点日之前；更正某盘点日版本时只重写以该日为起点的
    段。explicit_ranges 可把区间进一步收窄到给定窗口（仍受切段约束）。
    """
    movements, counts = load_facts(connection)
    watermark = as_of or _watermark(connection)
    plan = []
    for key in sorted(all_keys(connection)):
        change_days = set(movements.get(key, {})) | set(counts.get(key, {}))
        if not change_days:
            continue
        earliest = min(change_days)
        cutoff_days = sorted(day for day in counts.get(key, {}) if day >= earliest)
        segments = []
        cursor = earliest
        for cutoff_day in cutoff_days:
            if cutoff_day > cursor:
                segments.append((cursor, cutoff_day - timedelta(days=1)))
            cursor = cutoff_day
        segments.append((cursor, watermark))
        window = explicit_ranges.get(key) if explicit_ranges else None
        for start, end in segments:
            if window is not None:
                window_start, window_end = window
                start = max(start, window_start)
                end = min(end, window_end)
            if start <= end:
                plan.append({'key': key, 'start': start, 'end': end})
    return plan


def build_interval(connection, run_id, key, start, end, movements, counts_for_key, built_days):
    """重算一个键在 [start, end] 内逐日的日末快照。

    余额完全在内存中按日滚动；段开始前先把前一日余额（本 run 已构建行
    优先，其次 staging/已发布表）作为种子写入 built_days，因此相邻段、
    跨事务恢复都能连续承接，unknown 日也不会截断对更早基准的回查。
    """
    seed_day = start - timedelta(days=1)
    if seed_day not in built_days:
        seed = _staging_or_published_eod(connection, run_id, key, seed_day)
        built_days[seed_day] = None if seed is None else str(seed)
    rows = []
    previous_day = seed_day
    for day in iter_days(start, end):
        prior = built_days.get(previous_day, _NOT_BUILT)
        prior_eod = None if prior is None or prior is _NOT_BUILT else _decimal(prior)
        day_movements = movements.get(key, {}).get(day, [])
        quantities = [_decimal(row['quantity']) for row in day_movements]
        flow = sum(quantities, ZERO)
        inbound = sum((value for value in quantities if value > 0), ZERO)
        outbound = sum((-value for value in quantities if value < 0), ZERO)
        day_counts = counts_for_key.get(day)
        count_row = max(day_counts.values(), key=lambda row: row['version']) if day_counts else None
        movement_ids = [row['movement_id'] for row in day_movements]

        if count_row is not None:
            # 盘点锚定截止日的日末余额，同日流水仍计入入出库量。
            eod = _decimal(count_row['quantity'])
            derived = prior_eod + flow if prior_eod is not None else None
            adjustment = None if derived is None else eod - derived
            basis = {
                'source': 'count',
                'count_id': count_row['count_id'],
                'version': count_row['version'],
                'cutoff_time': count_row['cutoff_time'],
                'movement_ids': movement_ids,
                'count_adjustment': None if adjustment is None else str(adjustment),
            }
        elif prior_eod is None:
            eod = None
            basis = {
                'source': 'unknown',
                'reason': '首个有效盘点覆盖前无可靠基准',
                'movement_ids': movement_ids,
            }
        else:
            eod = prior_eod + flow
            basis = {
                'source': 'held' if not day_movements else 'movement',
                'prior_day': previous_day.isoformat(),
                'movement_ids': movement_ids,
            }

        if eod is None:
            status = 'unknown'
        elif eod < 0:
            status = 'negative'
        else:
            status = 'ok'

        rows.append({
            'warehouse': key[0],
            'sku': key[1],
            'lot': key[2],
            'day': day.isoformat(),
            'eod_quantity': None if eod is None else str(eod),
            'inbound': str(inbound),
            'outbound': str(outbound),
            'status': status,
            'basis': encode(basis).decode('utf-8'),
        })
        built_days[day] = None if eod is None else str(eod)
        previous_day = day
    return rows


def _staging_or_published_eod(connection, run_id, key, day):
    record = connection.execute(
        'SELECT eod_quantity FROM daily_staging WHERE run_id=? AND warehouse=? AND sku=? AND lot=? AND day=?',
        (run_id, *key, day.isoformat()),
    ).fetchone()
    if record is None:
        record = connection.execute(
            f'SELECT eod_quantity FROM {DAILY_TABLE} WHERE warehouse=? AND sku=? AND lot=? AND day=?',
            (*key, day.isoformat()),
        ).fetchone()
    if record is None or record['eod_quantity'] is None:
        return None
    return _decimal(record['eod_quantity'])


def build_month_rows(connection, run_id, key, month, daily_rows, month_start):
    """用“暂存 + 已发布”合并后的活跃日序列汇总月层。

    daily_rows 只包含该键活跃跨度（首事实日 → 水印日）与本月的交集：
    跨度之外的自然日既不持有也不参与日均；盘点前的活跃日即使没有
    可靠基准也保留行，余额标 unknown。
    """
    inbound = sum((_decimal(row['inbound']) for row in daily_rows), ZERO)
    outbound = sum((_decimal(row['outbound']) for row in daily_rows), ZERO)
    known = [row for row in daily_rows if row['eod_quantity'] is not None]
    unknown_days = len(daily_rows) - len(known)
    avg = sum((_decimal(row['eod_quantity']) for row in known), ZERO) / Decimal(len(daily_rows))
    last_row = daily_rows[-1]
    eod = _decimal(last_row['eod_quantity']) if last_row['eod_quantity'] is not None else None

    opening = _staging_or_published_eod(connection, run_id, key, month_start - timedelta(days=1))
    if opening is None and daily_rows[0]['day'] != month_start.isoformat():
        # 本月活跃跨度从月中才开始：上月末没有该键的任何事实，开盘视为 0。
        opening = ZERO
    adjustment = ZERO
    adjustment_parts = []
    for row in daily_rows:
        basis = json.loads(row['basis'])
        if basis.get('source') == 'count' and basis.get('count_adjustment') is not None:
            adjustment += _decimal(basis['count_adjustment'])
            adjustment_parts.append(
                {'day': row['day'], 'count_id': basis['count_id'], 'amount': basis['count_adjustment']}
            )

    flow = inbound - outbound
    if opening is not None and eod is not None:
        conserved = (opening + flow + adjustment) == eod
    else:
        conserved = None
    basis = {
        'source': 'monthly_rollup',
        'active_days': len(daily_rows),
        'known_days': len(known),
        'opening_source': (
            'previous_month_eod'
            if _staging_or_published_eod(connection, run_id, key, month_start - timedelta(days=1)) is not None
            else ('zero_before_first_fact' if daily_rows[0]['day'] != month_start.isoformat()
                  else 'unknown_before_first_count')
        ),
        'count_adjustments': adjustment_parts,
        'conservation': conserved,
        'identity': 'opening + inbound - outbound + count_adjustment = eod',
    }
    if any(row['status'] == 'negative' for row in daily_rows):
        status = 'negative'
    elif unknown_days:
        status = 'partial'
    else:
        status = 'ok'
    return {
        'warehouse': key[0],
        'sku': key[1],
        'lot': key[2],
        'month': month,
        'opening_quantity': None if opening is None else str(opening),
        'eod_quantity': None if eod is None else str(eod),
        'avg_daily_quantity': _q(avg),
        'inbound': str(inbound),
        'outbound': str(outbound),
        'count_adjustment': str(adjustment),
        'known_days': len(known),
        'unknown_days': unknown_days,
        'status': status,
        'basis': encode(basis).decode('utf-8'),
    }


def _merged_month_daily(connection, run_id, key, month_start, month_end, span_start, span_end):
    rows = {}
    low = max(month_start, span_start)
    high = min(month_end, span_end)
    for record in connection.execute(
        f'SELECT * FROM {DAILY_TABLE} WHERE warehouse=? AND sku=? AND lot=? AND day BETWEEN ? AND ?',
        (*key, low.isoformat(), high.isoformat()),
    ):
        rows[record['day']] = {column: record[column] for column in record.keys()}
    for record in connection.execute(
        'SELECT * FROM daily_staging WHERE run_id=? AND warehouse=? AND sku=? AND lot=? AND day BETWEEN ? AND ?',
        (run_id, *key, low.isoformat(), high.isoformat()),
    ):
        rows[record['day']] = {column: record[column] for column in record.keys()}
    return [rows[day.isoformat()] for day in iter_days(low, high) if day.isoformat() in rows]


def create_run(connection, plan, failure_point=None):
    run_id = 'run-' + uuid.uuid4().hex
    serializable = [
        {'key': list(item['key']), 'start': item['start'].isoformat(), 'end': item['end'].isoformat()}
        for item in plan
    ]
    connection.execute(
        'INSERT INTO rebuild_runs (run_id, status, plan, cursor_key, cursor_day, fact_digest, started_at, finished_at, detail) '
        'VALUES (?, ?, ?, NULL, NULL, ?, ?, NULL, ?)',
        (run_id, 'running', encode(serializable).decode(), fact_digest(connection),
         utc_now_text(), encode({'failure_point': failure_point}).decode()),
    )
    return run_id


def _persist_staging(connection, run_id, table, rows):
    columns = [record['name'] for record in connection.execute(f'PRAGMA table_info({table})')]
    for row in rows:
        payload = {'run_id': run_id, **row}
        names = ', '.join(f'"{column}"' for column in columns)
        placeholders = ', '.join('?' for _ in columns)
        connection.execute(
            f'INSERT OR REPLACE INTO {table} ({names}) VALUES ({placeholders})',
            [payload[column] for column in columns],
        )


def execute_run(connection, run_id, failure_point=None):
    """填充 staging，按键分事务提交；中断后已提交部分保留，可幂等重跑。

    每个键分两遍：先把所有段的日快照写入 staging，再汇总月层——月开盘
    需要读取同一 run 中已暂存的上月末日末值，不能在段处理的当下汇总。
    """
    movements, counts = load_facts(connection)
    run = connection.execute('SELECT * FROM rebuild_runs WHERE run_id=?', (run_id,)).fetchone()
    if run is None:
        raise ValueError(f'重建任务不存在: {run_id}')
    plan = json.loads(run['plan'])
    items_by_key = defaultdict(list)
    for item in plan:
        items_by_key[tuple(item['key'])].append(item)
    staged_months = set()
    for index, (key, items) in enumerate(sorted(items_by_key.items())):
        with transaction(connection):
            span_start = parse_day(min(item['start'] for item in items))
            span_end = parse_day(max(item['end'] for item in items))
            built_days = {}
            for item in items:
                start, end = parse_day(item['start']), parse_day(item['end'])
                daily_rows = build_interval(
                    connection, run_id, key, start, end, movements, counts.get(key, {}), built_days
                )
                for row in daily_rows:
                    built_days[parse_day(row['day'])] = row['eod_quantity']
                _persist_staging(connection, run_id, 'daily_staging', daily_rows)
            months = {month_key(day) for item in items for day in iter_days(span_start, span_end)}
            for month in sorted(months):
                year, month_number = (int(part) for part in month.split('-'))
                month_start, month_end = month_bounds(date(year, month_number, 1))
                merged = _merged_month_daily(
                    connection, run_id, key, month_start, month_end, span_start, span_end
                )
                monthly_row = build_month_rows(connection, run_id, key, month, merged, month_start)
                _persist_staging(connection, run_id, 'monthly_staging', [monthly_row])
                staged_months.add((*key, month))
            connection.execute(
                'UPDATE rebuild_runs SET cursor_key=?, cursor_day=? WHERE run_id=?',
                ('|'.join(key), span_end.isoformat(), run_id),
            )
        # 失败点在首个键事务提交之后：staging 已落库，run 保持 running，
        # 剩余键尚未处理，可凭 run_id 幂等恢复。
        if failure_point == 'midway' and index == 0 and (
            len(items_by_key) > 1 or len(items) > 1
        ):
            raise RuntimeError('模拟在首个键提交后中断，staging 已落库，run 保持 running')
    return staged_months


def publish_run(connection, run_id):
    """单事务整月替换：读者不可能看到只修了一半日期的月份。"""
    with transaction(connection):
        run = connection.execute('SELECT * FROM rebuild_runs WHERE run_id=?', (run_id,)).fetchone()
        if run is None:
            raise ValueError(f'重建任务不存在: {run_id}')
        plan = json.loads(run['plan'])
        for item in plan:
            key = tuple(item['key'])
            start, end = parse_day(item['start']), parse_day(item['end'])
            connection.execute(
                f'DELETE FROM {DAILY_TABLE} WHERE warehouse=? AND sku=? AND lot=? AND day BETWEEN ? AND ?',
                (*key, start.isoformat(), end.isoformat()),
            )
            connection.execute(
                f'INSERT INTO {DAILY_TABLE} ({DAILY_PUBLISHED_COLS}) '
                f'SELECT {DAILY_PUBLISHED_COLS} FROM daily_staging '
                'WHERE run_id=? AND warehouse=? AND sku=? AND lot=? AND day BETWEEN ? AND ?',
                (run_id, *key, start.isoformat(), end.isoformat()),
            )
            months = {month_key(day) for day in iter_days(start, end)}
            for month in months:
                connection.execute(
                    f'DELETE FROM {MONTHLY_TABLE} WHERE warehouse=? AND sku=? AND lot=? AND month=?',
                    (*key, month),
                )
                connection.execute(
                    f'INSERT INTO {MONTHLY_TABLE} ({MONTHLY_PUBLISHED_COLS}) '
                    f'SELECT {MONTHLY_PUBLISHED_COLS} FROM monthly_staging '
                    'WHERE run_id=? AND warehouse=? AND sku=? AND lot=? AND month=?',
                    (run_id, *key, month),
                )
        connection.execute('DELETE FROM daily_staging WHERE run_id=?', (run_id,))
        connection.execute('DELETE FROM monthly_staging WHERE run_id=?', (run_id,))
        connection.execute(
            'UPDATE rebuild_runs SET status=?, finished_at=?, cursor_key=NULL, cursor_day=NULL WHERE run_id=?',
            ('published', utc_now_text(), run_id),
        )


def rebuild(connection, explicit_ranges=None, failure_point=None, as_of=None):
    """端到端重建：规划 → 建任务 → 分键暂存 → 原子发布。"""
    plan = plan_rebuild(connection, explicit_ranges, as_of=as_of)
    with transaction(connection):
        run_id = create_run(connection, plan, failure_point)
    execute_run(connection, run_id, failure_point)
    publish_run(connection, run_id)
    return run_id, plan


def resume_and_publish(connection, run_id):
    run = connection.execute('SELECT * FROM rebuild_runs WHERE run_id=?', (run_id,)).fetchone()
    if run is None or run['status'] != 'running':
        raise ValueError('只能恢复处于 running 的重建任务')
    if run['fact_digest'] != fact_digest(connection):
        raise ValueError('事实已再次变化，请重新规划重建')
    execute_run(connection, run_id)
    publish_run(connection, run_id)
    return run_id


# ------------------------------------------------------------------ 查询

def query_daily(connection, month=None):
    sql = f'SELECT {DAILY_PUBLISHED_COLS} FROM {DAILY_TABLE}'
    parameters = []
    if month:
        sql += ' WHERE substr(day, 1, 7)=?'
        parameters.append(month)
    sql += ' ORDER BY warehouse, sku, lot, day'
    return [dict(record) for record in connection.execute(sql, parameters)]


def query_monthly(connection, month=None):
    sql = f'SELECT {MONTHLY_PUBLISHED_COLS} FROM {MONTHLY_TABLE}'
    parameters = []
    if month:
        sql += ' WHERE month=?'
        parameters.append(month)
    sql += ' ORDER BY warehouse, sku, lot, month'
    return [dict(record) for record in connection.execute(sql, parameters)]


def list_runs(connection):
    return [dict(record) for record in connection.execute(
        'SELECT run_id, status, plan, fact_digest, started_at, finished_at, detail FROM rebuild_runs ORDER BY started_at'
    )]
