import copy
import json
import sqlite3
import tempfile
import unittest
from datetime import date
from decimal import Decimal
from pathlib import Path

from warehouse.catalog import connect, load_bundle, load_file, transaction
from warehouse.clock import parse_instant, zone
from warehouse.domain import overview
from warehouse.snapshots import (
    create_run,
    execute_run,
    fact_digest,
    plan_rebuild,
    publish_run,
    query_daily,
    query_monthly,
    rebuild,
    resume_and_publish,
)
from warehouse.store import ObjectStore

ROOT = Path(__file__).resolve().parents[1]
SOURCE = ROOT / 'examples' / 'source.json'
BACKFILL = ROOT / 'examples' / 'source_backfill.json'
KEY = ('w1', 's1', 'l1')


def movement(movement_id, day, quantity, seq, *, event_time=None, warehouse='w1', sku='s1', lot='l1', reversal_of=None):
    return {
        'movement_id': movement_id,
        'warehouse': warehouse,
        'sku': sku,
        'lot': lot,
        'event_time': event_time or f'{day}T09:00:00+08:00',
        'day': day,
        'receive_seq': seq,
        'quantity': str(quantity),
        'reversal_of': reversal_of,
    }


def count_row(count_id, day, quantity, version=1, *, warehouse='w1', sku='s1', lot='l1'):
    return {
        'count_id': count_id,
        'warehouse': warehouse,
        'sku': sku,
        'lot': lot,
        'cutoff_time': f'{day}T23:59:59+08:00',
        'cutoff_day': day,
        'version': version,
        'quantity': str(quantity),
    }


def bundle(movements=None, counts=None):
    """接受 (movements, counts)，也接受单个混合列表（按字段自动分流）。"""
    if counts is None and movements is not None and movements and all(
        isinstance(row, dict) and ('movement_id' in row or 'count_id' in row) for row in movements
    ) and any('count_id' in row for row in movements):
        counts = [row for row in movements if 'count_id' in row]
        movements = [row for row in movements if 'movement_id' in row]
    return {'movements': movements or [], 'counts': counts or []}


def daily_map(connection):
    return {(row['warehouse'], row['sku'], row['lot'], row['day']): row for row in query_daily(connection)}


def monthly_map(connection):
    return {(row['warehouse'], row['sku'], row['lot'], row['month']): row for row in query_monthly(connection)}


class Harness(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory()
        self.path = Path(self.temporary.name) / 'warehouse.sqlite3'
        self.connection = connect(self.path)

    def tearDown(self):
        self.connection.close()
        self.temporary.cleanup()

    def load(self, data, batch='b'):
        load_bundle(self.connection, data, batch)

    def rebuild(self, as_of=None, **kwargs):
        return rebuild(self.connection, as_of=as_of, **kwargs)

    def dmap(self):
        return daily_map(self.connection)

    def mmap(self):
        return monthly_map(self.connection)


class CatalogTests(Harness):
    def setUp(self):
        super().setUp()
        self.source = json.loads(SOURCE.read_text(encoding='utf-8'))

    def test_replay_same_batch_is_idempotent(self):
        self.assertTrue(load_file(self.connection, SOURCE, 'daily'))
        first = overview(self.connection)
        self.assertFalse(load_file(self.connection, SOURCE, 'daily'))
        self.assertEqual(overview(self.connection), first)
        self.assertEqual(self.connection.execute('SELECT COUNT(*) FROM ingest_batches').fetchone()[0], 1)

    def test_conflicting_batch(self):
        load_bundle(self.connection, self.source, 'daily')
        with self.assertRaises(ValueError):
            load_bundle(self.connection, bundle(), 'daily')

    def test_duplicate_key_rolls_back(self):
        broken = copy.deepcopy(self.source)
        broken['movements'].append(copy.deepcopy(self.source['movements'][0]))
        with self.assertRaises(sqlite3.IntegrityError):
            load_bundle(self.connection, broken, 'broken')
        self.assertEqual(self.connection.execute('SELECT COUNT(*) FROM movements').fetchone()[0], 0)
        self.assertEqual(self.connection.execute('SELECT COUNT(*) FROM ingest_batches').fetchone()[0], 0)

    def test_unknown_table(self):
        with self.assertRaises(ValueError):
            load_bundle(self.connection, {'unknown_table': []}, 'unknown')

    def test_transaction_failure_leaves_nothing(self):
        with self.assertRaises(RuntimeError):
            with transaction(self.connection):
                self.connection.execute(
                    'INSERT INTO ingest_batches VALUES (?, ?, ?)', ('partial', 'abc', 1)
                )
                raise RuntimeError('模拟写入中断')
        self.assertEqual(self.connection.execute('SELECT COUNT(*) FROM ingest_batches').fetchone()[0], 0)

    def test_invalid_record_fields(self):
        broken = copy.deepcopy(self.source)
        broken['movements'][0]['unexpected'] = 'x'
        with self.assertRaises(ValueError):
            load_bundle(self.connection, broken, 'broken')

    def test_invalid_batch(self):
        with self.assertRaises(ValueError):
            load_bundle(self.connection, self.source, '')

    def test_distinct_batch_primary_key_conflict(self):
        load_bundle(self.connection, self.source, 'daily')
        with self.assertRaises(sqlite3.IntegrityError):
            load_bundle(self.connection, self.source, 'different')
        self.assertEqual(self.connection.execute('SELECT COUNT(*) FROM ingest_batches').fetchone()[0], 1)

    def test_persistence_across_reconnect(self):
        load_bundle(self.connection, self.source, 'daily')
        self.rebuild()
        self.connection.close()
        self.connection = connect(self.path)
        self.assertFalse(load_file(self.connection, SOURCE, 'daily'))
        self.assertEqual(len(query_daily(self.connection)), 39)

    def test_empty_bundle(self):
        self.assertTrue(load_bundle(self.connection, bundle(), 'empty'))
        self.assertEqual(self.connection.execute('SELECT rows FROM ingest_batches').fetchone()[0], 0)

    def test_timezone_is_pinned_per_database(self):
        with self.assertRaises(ValueError):
            connect(self.path, 'UTC')

    def test_business_day_mismatch_rejected(self):
        bad = bundle([movement('m1', '2026-10-01', 5, 1, event_time='2026-10-01T07:00:00+08:00'.replace('07:00', '23:30'))])
        bad['movements'][0]['day'] = '2026-10-02'
        with self.assertRaises(ValueError):
            load_bundle(self.connection, bad, 'bad')

    def test_naive_timestamp_rejected(self):
        row = movement('m1', '2026-10-01', 5, 1, event_time='2026-10-01T09:00:00')
        with self.assertRaises(ValueError):
            load_bundle(self.connection, bundle([row]), 'bad')

    def test_zero_quantity_rejected(self):
        with self.assertRaises(ValueError):
            load_bundle(self.connection, bundle([movement('m1', '2026-10-01', 0, 1)]), 'bad')


class ReversalValidationTests(Harness):
    def _base(self):
        self.load(bundle([movement('m1', '2026-10-01', 10, 1)], [count_row('c1', '2026-10-05', 10)]), 'base')

    def test_reversal_must_reference_existing_movement(self):
        with self.assertRaises(ValueError):
            self.load(bundle([movement('r1', '2026-10-02', -3, 2, reversal_of='ghost')]), 'bad')

    def test_reversal_must_have_opposite_sign(self):
        self._base()
        with self.assertRaises(ValueError):
            self.load(bundle([movement('r1', '2026-10-02', 3, 2, reversal_of='m1')]), 'bad')

    def test_cumulative_reversal_cannot_exceed_original(self):
        self._base()
        self.load(bundle([movement('r1', '2026-10-02', -6, 2, reversal_of='m1')]), 'r1')
        with self.assertRaises(ValueError):
            self.load(bundle([movement('r2', '2026-10-03', -5, 3, reversal_of='m1')]), 'r2')

    def test_reversal_cannot_be_reversed(self):
        self._base()
        self.load(bundle([movement('r1', '2026-10-02', -4, 2, reversal_of='m1')]), 'r1')
        with self.assertRaises(ValueError):
            self.load(bundle([movement('r2', '2026-10-03', 2, 3, reversal_of='r1')]), 'r2')

    def test_partial_reversal_conserves_balance(self):
        self._base()
        self.load(bundle([movement('r1', '2026-10-02', -4, 2, reversal_of='m1')]), 'r1')
        self.rebuild(as_of=date(2026, 10, 6))
        dmap = self.dmap()
        self.assertEqual(dmap[(*KEY, '2026-10-01')]['eod_quantity'], None)
        self.assertEqual(dmap[(*KEY, '2026-10-02')]['eod_quantity'], None)
        # 盘点 10-05 锚定 10；之后无流水，冲销已发生在未知区间，不改变盘点后余额
        self.assertEqual(dmap[(*KEY, '2026-10-06')]['eod_quantity'], '10')
        self.assertEqual(dmap[(*KEY, '2026-10-02')]['outbound'], '4')

    def test_reversal_after_count_changes_segment_until_next_count(self):
        self.load(bundle(
            [movement('m1', '2026-10-01', 10, 1)],
            [count_row('c1', '2026-10-05', 10), count_row('c2', '2026-10-10', 6)],
        ), 'base')
        self.rebuild()
        self.assertEqual(self.dmap()[(*KEY, '2026-10-09')]['eod_quantity'], '10')
        # 迟到冲销 10-07：-4，影响止于下一盘点 10-10
        self.load(bundle([movement('r1', '2026-10-07', -4, 2, reversal_of='m1')]), 'late')
        self.rebuild()
        dmap = self.dmap()
        self.assertEqual(dmap[(*KEY, '2026-10-06')]['eod_quantity'], '10')
        self.assertEqual(dmap[(*KEY, '2026-10-07')]['eod_quantity'], '6')
        self.assertEqual(dmap[(*KEY, '2026-10-09')]['eod_quantity'], '6')
        self.assertEqual(dmap[(*KEY, '2026-10-10')]['eod_quantity'], '6')
        basis = json.loads(dmap[(*KEY, '2026-10-10')]['basis'])
        self.assertEqual(basis['source'], 'count')
        self.assertEqual(basis['count_adjustment'], '0')


class SnapshotSemanticsTests(Harness):
    def test_unknown_before_first_count_not_zero(self):
        self.load(bundle(
            [movement('m1', '2026-10-01', 10, 1), movement('m2', '2026-10-03', -3, 2)],
            [count_row('c1', '2026-10-05', 7)],
        ), 'b')
        self.rebuild(as_of=date(2026, 10, 5))
        dmap = self.dmap()
        for day in ('2026-10-01', '2026-10-02', '2026-10-03', '2026-10-04'):
            row = dmap[(*KEY, day)]
            self.assertIsNone(row['eod_quantity'])
            self.assertEqual(row['status'], 'unknown')
            self.assertEqual(json.loads(row['basis'])['source'], 'unknown')
        # 流水量仍然记录
        self.assertEqual(dmap[(*KEY, '2026-10-01')]['inbound'], '10')
        self.assertEqual(dmap[(*KEY, '2026-10-03')]['outbound'], '3')

    def test_holding_rows_generated_on_no_movement_days(self):
        self.load(bundle([
            movement('m1', '2026-10-01', 10, 1),
            count_row('c1', '2026-10-01', 10),
        ]), 'b')
        self.rebuild(as_of=date(2026, 10, 3))
        dmap = self.dmap()
        self.assertEqual(dmap[(*KEY, '2026-10-02')]['eod_quantity'], '10')
        self.assertEqual(dmap[(*KEY, '2026-10-02')]['inbound'], '0')
        self.assertEqual(dmap[(*KEY, '2026-10-02')]['outbound'], '0')
        basis = json.loads(dmap[(*KEY, '2026-10-02')]['basis'])
        self.assertEqual(basis['source'], 'held')

    def test_count_overwrites_derived_error_and_records_adjustment(self):
        self.load(bundle(
            [movement('m1', '2026-10-01', 10, 1)],
            [count_row('c1', '2026-10-05', 10), count_row('c2', '2026-10-10', 7)],
        ), 'b')
        self.rebuild(as_of=date(2026, 10, 12))
        dmap = self.dmap()
        row = dmap[(*KEY, '2026-10-10')]
        self.assertEqual(row['eod_quantity'], '7')
        basis = json.loads(row['basis'])
        self.assertEqual(basis['count_adjustment'], '-3')
        # 盘点之后按 7 持有
        self.assertEqual(dmap[(*KEY, '2026-10-11')]['eod_quantity'], '7')
        monthly = self.mmap()[(*KEY, '2026-10')]
        self.assertEqual(monthly['count_adjustment'], '-3')
        self.assertEqual(monthly['eod_quantity'], '7')

    def test_corrected_count_version_re_anchors_segment(self):
        self.load(bundle(
            [movement('m1', '2026-10-01', 10, 1)],
            [count_row('c1', '2026-10-05', 10, version=1)],
        ), 'b')
        self.rebuild(as_of=date(2026, 10, 7))
        self.assertEqual(self.dmap()[(*KEY, '2026-10-06')]['eod_quantity'], '10')
        # 更正盘点：v2 改为 8，旧版本保留
        self.load(bundle([], [count_row('c1b', '2026-10-05', 8, version=2)]), 'fix')
        self.rebuild(as_of=date(2026, 10, 7))
        dmap = self.dmap()
        self.assertEqual(dmap[(*KEY, '2026-10-05')]['eod_quantity'], '8')
        self.assertEqual(dmap[(*KEY, '2026-10-06')]['eod_quantity'], '8')
        basis = json.loads(dmap[(*KEY, '2026-10-05')]['basis'])
        self.assertEqual(basis['version'], 2)
        self.assertEqual(basis['count_id'], 'c1b')
        versions = self.connection.execute('SELECT COUNT(*) FROM counts').fetchone()[0]
        self.assertEqual(versions, 2)

    def test_negative_inventory_is_kept_and_flagged(self):
        self.load(bundle(
            [movement('m1', '2026-10-01', 10, 1), movement('m2', '2026-10-07', -14, 2)],
            [count_row('c1', '2026-10-05', 10)],
        ), 'b')
        self.rebuild(as_of=date(2026, 10, 8))
        dmap = self.dmap()
        row = dmap[(*KEY, '2026-10-07')]
        self.assertEqual(row['eod_quantity'], '-4')
        self.assertEqual(row['status'], 'negative')
        monthly = self.mmap()[(*KEY, '2026-10')]
        self.assertEqual(monthly['status'], 'negative')
        self.assertEqual(monthly['eod_quantity'], '-4')

    def test_balance_source_basis_is_preserved(self):
        self.load(bundle(
            [movement('m1', '2026-10-01', 10, 1), movement('m2', '2026-10-08', 2, 2)],
            [count_row('c1', '2026-10-05', 10)],
        ), 'b')
        self.rebuild(as_of=date(2026, 10, 9))
        dmap = self.dmap()
        self.assertEqual(json.loads(dmap[(*KEY, '2026-10-05')]['basis'])['source'], 'count')
        self.assertEqual(json.loads(dmap[(*KEY, '2026-10-06')]['basis'])['source'], 'held')
        movement_basis = json.loads(dmap[(*KEY, '2026-10-08')]['basis'])
        self.assertEqual(movement_basis['source'], 'movement')
        self.assertEqual(movement_basis['movement_ids'], ['m2'])


class MonthlyReconciliationTests(Harness):
    def _full_example(self):
        load_file(self.connection, SOURCE, 'first')
        self.rebuild()

    def test_daily_totals_equal_monthly_flows(self):
        self._full_example()
        for month in ('2026-10', '2026-11'):
            rows = [row for row in query_daily(self.connection) if row['day'].startswith(month)]
            monthly = self.mmap()[(*KEY, month)]
            self.assertEqual(
                sum((Decimal(row['inbound']) for row in rows), Decimal('0')),
                Decimal(monthly['inbound']),
            )
            self.assertEqual(
                sum((Decimal(row['outbound']) for row in rows), Decimal('0')),
                Decimal(monthly['outbound']),
            )

    def test_conservation_identity_when_fully_known(self):
        self._full_example()
        monthly = self.mmap()[(*KEY, '2026-11')]
        basis = json.loads(monthly['basis'])
        self.assertTrue(basis['conservation'])
        self.assertEqual(
            Decimal(monthly['opening_quantity'])
            + Decimal(monthly['inbound'])
            - Decimal(monthly['outbound'])
            + Decimal(monthly['count_adjustment']),
            Decimal(monthly['eod_quantity']),
        )

    def test_partial_month_marks_unknown_and_skips_identity(self):
        self._full_example()
        monthly = self.mmap()[(*KEY, '2026-10')]
        self.assertEqual(monthly['status'], 'partial')
        self.assertIsNone(monthly['opening_quantity'])
        self.assertIsNone(json.loads(monthly['basis'])['conservation'])
        self.assertEqual(monthly['known_days'] + monthly['unknown_days'], 31)

    def test_avg_daily_matches_daily_eod_average(self):
        self._full_example()
        rows = [row for row in query_daily(self.connection) if row['day'].startswith('2026-11')]
        average = sum((Decimal(row['eod_quantity']) for row in rows), Decimal('0')) / Decimal(len(rows))
        monthly = self.mmap()[(*KEY, '2026-11')]
        self.assertEqual(Decimal(monthly['avg_daily_quantity']), average.quantize(Decimal('0.000001')))

    def test_month_end_equals_last_business_day_eod(self):
        self._full_example()
        dmap = self.dmap()
        self.assertEqual(self.mmap()[(*KEY, '2026-10')]['eod_quantity'], dmap[(*KEY, '2026-10-31')]['eod_quantity'])
        self.assertEqual(self.mmap()[(*KEY, '2026-11')]['eod_quantity'], dmap[(*KEY, '2026-11-08')]['eod_quantity'])


class BackfillAndRebuildTests(Harness):
    def test_cross_month_backfill_revises_interval_until_next_count(self):
        load_file(self.connection, SOURCE, 'first')
        self.rebuild()
        before_oct = self.mmap()[(*KEY, '2026-10')]
        self.assertEqual(before_oct['inbound'], '15')
        load_file(self.connection, BACKFILL, 'backfill')
        run_id, plan = self.rebuild()
        # 迟到流水 10-09/10-12 的影响止于下一盘点 11-05（含），11-05 之后段独立
        interval_ends = {item['end'].isoformat() for item in plan}
        self.assertIn('2026-11-04', interval_ends)
        dmap = self.dmap()
        self.assertEqual(dmap[(*KEY, '2026-10-09')]['eod_quantity'], '13')
        self.assertEqual(dmap[(*KEY, '2026-10-31')]['eod_quantity'], '11')
        self.assertEqual(dmap[(*KEY, '2026-11-04')]['eod_quantity'], '11')
        # 11-05 盘点把补录误差吸收为调整 -2，11-05 之后余额与补录前一致
        self.assertEqual(dmap[(*KEY, '2026-11-05')]['eod_quantity'], '9')
        self.assertEqual(dmap[(*KEY, '2026-11-08')]['eod_quantity'], '4')
        october = self.mmap()[(*KEY, '2026-10')]
        november = self.mmap()[(*KEY, '2026-11')]
        self.assertEqual(october['inbound'], '19')
        self.assertEqual(october['outbound'], '5')
        self.assertEqual(october['eod_quantity'], '11')
        self.assertEqual(november['opening_quantity'], '11')
        self.assertEqual(november['count_adjustment'], '-2')
        self.assertTrue(json.loads(november['basis'])['conservation'])
        self.assertEqual(
            Decimal('11') + Decimal(november['inbound']) - Decimal(november['outbound']) + Decimal('-2'),
            Decimal(november['eod_quantity']),
        )

    def test_rebuild_is_idempotent_and_deterministic(self):
        load_file(self.connection, SOURCE, 'first')
        self.rebuild()
        snapshot_once = (query_daily(self.connection), query_monthly(self.connection))
        digest_before = fact_digest(self.connection)
        self.rebuild()
        self.assertEqual((query_daily(self.connection), query_monthly(self.connection)), snapshot_once)
        self.assertEqual(fact_digest(self.connection), digest_before)

    def test_failed_run_is_recoverable_and_published_state_is_atomic(self):
        load_file(self.connection, SOURCE, 'first')
        load_bundle(
            self.connection,
            bundle([
                movement('x1', '2026-10-20', 5, 99, warehouse='w2'),
                count_row('cx', '2026-10-25', 5, warehouse='w2'),
            ]),
            'second-key',
        )
        plan = plan_rebuild(self.connection)
        self.assertEqual(len({item['key'] for item in plan}), 2)
        with transaction(self.connection):
            run_id = create_run(self.connection, plan, failure_point='midway')
        with self.assertRaises(RuntimeError):
            execute_run(self.connection, run_id, failure_point='midway')
        # 失败后发布表无任何半成品，run 保持 running，staging 保留已提交键
        self.assertEqual(query_daily(self.connection), [])
        self.assertEqual(query_monthly(self.connection), [])
        run = self.connection.execute('SELECT status FROM rebuild_runs WHERE run_id=?', (run_id,)).fetchone()
        self.assertEqual(run['status'], 'running')
        staged = self.connection.execute(
            'SELECT DISTINCT warehouse FROM daily_staging WHERE run_id=?', (run_id,)
        ).fetchall()
        self.assertEqual(len(staged), 1)
        # 恢复（无失败注入）并发布，结果与一次成功重建完全一致
        resume_and_publish(self.connection, run_id)
        recovered = (query_daily(self.connection), query_monthly(self.connection))
        self.rebuild()
        self.assertEqual((query_daily(self.connection), query_monthly(self.connection)), recovered)
        self.assertEqual(
            self.connection.execute(
                "SELECT status FROM rebuild_runs WHERE run_id=?", (run_id,)
            ).fetchone()['status'],
            'published',
        )
        self.assertEqual(
            self.connection.execute('SELECT COUNT(*) FROM daily_staging').fetchone()[0], 0
        )

    def test_resume_rejected_when_facts_changed(self):
        load_file(self.connection, SOURCE, 'first')
        plan = plan_rebuild(self.connection)
        with transaction(self.connection):
            run_id = create_run(self.connection, plan)
        with self.assertRaises(RuntimeError):
            execute_run(self.connection, run_id, failure_point='midway')
        self.load(bundle([movement('z1', '2026-10-21', 1, 88, warehouse='w9')]), 'changed')
        with self.assertRaises(ValueError):
            resume_and_publish(self.connection, run_id)

    def test_explicit_window_is_bounded_by_count_segmentation(self):
        self.load(bundle([
            movement('m1', '2026-10-01', 10, 1),
            count_row('c1', '2026-10-05', 10),
            count_row('c2', '2026-10-20', 9),
        ]), 'b')
        plan = plan_rebuild(
            self.connection,
            {KEY: (date(2026, 10, 1), date(2026, 10, 31))},
            as_of=date(2026, 10, 31),
        )
        endpoints = [(item['start'].isoformat(), item['end'].isoformat()) for item in plan]
        self.assertIn(('2026-10-01', '2026-10-04'), endpoints)
        self.assertIn(('2026-10-05', '2026-10-19'), endpoints)
        self.assertIn(('2026-10-20', '2026-10-31'), endpoints)


class TimezoneTests(Harness):
    def test_instant_maps_to_business_day(self):
        tz = zone('Asia/Shanghai')
        # UTC 10-01 17:00 == 上海 10-02 01:00，归属 10-02 而非 UTC 当日
        instant = parse_instant('2026-10-01T17:00:00+00:00')
        self.assertEqual(instant.astimezone(tz).date().isoformat(), '2026-10-02')

    def test_utc_evening_event_lands_on_next_business_day(self):
        row = movement('m1', '2026-10-02', 10, 1, event_time='2026-10-01T17:30:00+00:00')
        self.load(bundle([row], [count_row('c1', '2026-10-02', 10)]), 'b')
        self.rebuild()
        dmap = self.dmap()
        self.assertEqual(dmap[(*KEY, '2026-10-02')]['inbound'], '10')
        self.assertNotIn((*KEY, '2026-10-01'), {(k[0], k[1], k[2], k[3]) for k in dmap})


class ObjectStoreTests(unittest.TestCase):
    def test_roundtrip_and_reuse(self):
        with tempfile.TemporaryDirectory() as temporary:
            store = ObjectStore(temporary)
            rows = [{'标识': '合成数据', 'value': None}, {'number': 3}]
            digest = store.put(rows)
            self.assertEqual(store.read(digest), rows)
            self.assertEqual(store.put(rows), digest)
            self.assertEqual(len(list(Path(temporary).glob('*.jsonl'))), 1)

    def test_corrupt_object(self):
        with tempfile.TemporaryDirectory() as temporary:
            store = ObjectStore(temporary)
            digest = store.put([{'value': 1}])
            (Path(temporary) / (digest + '.jsonl')).write_bytes(b'{}')
            with self.assertRaises(ValueError):
                store.read(digest)

    def test_empty_and_invalid_identifier(self):
        with tempfile.TemporaryDirectory() as temporary:
            store = ObjectStore(temporary)
            self.assertEqual(store.read(store.put([])), [])
            with self.assertRaises(ValueError):
                store.read('../outside')

    def test_nonfinite_number(self):
        with tempfile.TemporaryDirectory() as temporary:
            with self.assertRaises(ValueError):
                ObjectStore(temporary).put([{'value': float('nan')}])

    def test_snapshot_export_roundtrip(self):
        with tempfile.TemporaryDirectory() as temporary:
            connection = connect(Path(temporary) / 'db.sqlite3')
            load_file(connection, SOURCE, 'first')
            rebuild(connection)
            rows = query_daily(connection)
            store = ObjectStore(Path(temporary) / 'objects')
            self.assertEqual(store.read(store.put(rows)), rows)
            connection.close()


if __name__ == '__main__':
    unittest.main()
