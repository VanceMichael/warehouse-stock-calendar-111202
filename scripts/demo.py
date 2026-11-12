import json
import subprocess
import sys
import tempfile
from decimal import Decimal
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
from warehouse.store import ObjectStore


def execute(*arguments, expect_failure=False):
    command = [sys.executable, '-m', 'warehouse', *arguments]
    print('$ ' + ' '.join(command), flush=True)
    result = subprocess.run(command, cwd=ROOT, text=True, capture_output=True)
    if expect_failure:
        if result.returncode == 0:
            raise RuntimeError('预期失败的命令却成功了')
        print('(预期失败) ' + result.stderr.strip().splitlines()[-1], flush=True)
        return None
    if result.returncode != 0:
        raise RuntimeError(result.stderr)
    print(result.stdout.strip(), flush=True)
    return json.loads(result.stdout)


def show(rows, title):
    print(f'\n== {title} ==', flush=True)
    for row in rows:
        print(json.dumps(row, ensure_ascii=False, sort_keys=True), flush=True)


def main():
    with tempfile.TemporaryDirectory() as temporary:
        database = str(Path(temporary) / 'warehouse.sqlite3')
        objects = str(Path(temporary) / 'objects')
        source = str(ROOT / 'examples' / 'source.json')
        backfill = str(ROOT / 'examples' / 'source_backfill.json')

        execute('import', '--db', database, '--input', source, '--batch', 'first')
        repeated = execute('import', '--db', database, '--input', source, '--batch', 'first')
        if repeated['changed']:
            raise RuntimeError('重复导入发生额外写入')

        # 首次重建：含三个盘点切段，注入中途失败，随后凭 run_id 恢复。
        failed = execute(
            'rebuild', '--db', database, '--failure-point', 'midway', expect_failure=True
        )
        runs = execute('inspect', '--db', database, '--view', 'runs')
        running = [row for row in runs if row['status'] == 'running']
        if len(running) != 1:
            raise RuntimeError('失败后应恰好保留一个 running 重建任务')
        run_id = running[0]['run_id']
        # 发布表在恢复前不得暴露半成品。
        if execute('inspect', '--db', database, '--view', 'daily'):
            raise RuntimeError('失败后发布表出现了半成品日快照')
        execute('resume', '--db', database, '--run-id', run_id)
        show(execute('inspect', '--db', database, '--view', 'daily'), '日末快照（无交易日生成持有状态，盘点前为 unknown）')
        show(execute('inspect', '--db', database, '--view', 'monthly'), '月层指标（月末余额/日均/入出库分别表达）')

        # 跨月补录上月流水后重建，影响止于下一有效盘点。
        execute('import', '--db', database, '--input', backfill, '--batch', 'backfill')
        execute('rebuild', '--db', database)
        monthly = execute('inspect', '--db', database, '--view', 'monthly')
        show(monthly, '补录后的月层（11 月开盘承接 10 月末，盘点吸收调整 -2）')

        # 守恒关系：opening + inbound - outbound + count_adjustment = eod。
        for row in monthly:
            basis = json.loads(row['basis'])
            if basis['conservation'] is None:
                continue
            identity = (
                Decimal(row['opening_quantity'])
                + Decimal(row['inbound'])
                - Decimal(row['outbound'])
                + Decimal(row['count_adjustment'])
            )
            if identity != Decimal(row['eod_quantity']):
                raise RuntimeError(f"月层守恒不成立: {row['month']}")
        print('\n守恒关系校验通过', flush=True)

        # 日末量与月指标交叉核对：月入出库必须等于日入出库之和。
        daily = execute('inspect', '--db', database, '--view', 'daily')
        for row in monthly:
            month_days = [item for item in daily if item['day'].startswith(row['month'])]
            inbound = sum((Decimal(item['inbound']) for item in month_days), Decimal('0'))
            outbound = sum((Decimal(item['outbound']) for item in month_days), Decimal('0'))
            if inbound != Decimal(row['inbound']) or outbound != Decimal(row['outbound']):
                raise RuntimeError(f"日末量与月指标不一致: {row['month']}")
        print('日末量与月指标交叉核对通过', flush=True)

        exported = execute('export', '--db', database, '--objects', objects, '--view', 'daily')
        restored = ObjectStore(objects).read(exported['object'])
        if restored != daily:
            raise RuntimeError('导出与查询结果不符')
        print('对象内容寻址导出与往返校验通过', flush=True)


if __name__ == '__main__':
    main()
