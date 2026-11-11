from collections import defaultdict
from datetime import date, datetime, timezone
from decimal import Decimal


def overview(connection):
    rows = [dict(record) for record in connection.execute('SELECT warehouse, sku, lot, quantity FROM movements ORDER BY movement_id')]
    totals = defaultdict(lambda: Decimal("0"))
    for record in rows:
        key = tuple(record[field] for field in ['warehouse', 'sku', 'lot'])
        value = Decimal(record['quantity'])
        if not value.is_finite():
            raise ValueError("数量必须为有限十进制")
        totals[key] += value
    return [dict(zip(['warehouse', 'sku', 'lot'], key), **{'quantity': str(value)}) for key, value in sorted(totals.items())]
