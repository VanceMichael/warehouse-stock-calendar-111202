"""查询域服务。

``overview`` 仅回答“流水净变动”，它不是期末余额——盘点调整、未知区间
都不在其语义内。真正的周期快照见 ``warehouse.snapshots``。
"""

from collections import defaultdict
from decimal import Decimal

from .ledger import parse_quantity
from .snapshots import query_daily, query_monthly, list_runs


def overview(connection):
    rows = [
        dict(record)
        for record in connection.execute(
            'SELECT warehouse, sku, lot, quantity FROM movements ORDER BY movement_id'
        )
    ]
    totals = defaultdict(lambda: Decimal('0'))
    for record in rows:
        key = tuple(record[field] for field in ['warehouse', 'sku', 'lot'])
        value = parse_quantity(record['quantity'], allow_zero=False)
        totals[key] += value
    return [
        dict(zip(['warehouse', 'sku', 'lot'], key), **{'quantity': str(value)})
        for key, value in sorted(totals.items())
    ]


VIEWS = {
    'overview': overview,
    'daily': query_daily,
    'monthly': query_monthly,
    'runs': list_runs,
}


def view(connection, name='overview', **arguments):
    if name not in VIEWS:
        raise ValueError(f'未知视图: {name}')
    return VIEWS[name](connection, **arguments) if name in ('daily', 'monthly') else VIEWS[name](connection)
