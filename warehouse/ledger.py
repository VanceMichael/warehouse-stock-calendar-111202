"""事实装载的语义校验。

结构（字段集合）校验在 ``catalog.load_bundle`` 完成；本模块只处理
跨记录的业务规则：

* 流水时刻必须带偏移，业务日必须等于时刻在业务时区下的归属日；
* 数量为有限十进制，流水数量不可为 0，正负分别代表入/出库；
* 冲销必须指向已存在的原流水、符号相反，累计冲销量不得超过原量，
  冲销记录本身不能再被冲销（只允许一层）；
* 盘点版本同一（仓库, 货品, 批次, 截止业务日）内唯一，更正盘点以
  更大版本登记，旧版本保留用于来源解释。
"""

from decimal import Decimal, InvalidOperation

from .clock import business_day, parse_day, parse_instant


def parse_quantity(text, *, allow_zero):
    if not isinstance(text, str) or not text:
        raise ValueError('数量必须是十进制文本')
    try:
        value = Decimal(text)
    except InvalidOperation as exc:
        raise ValueError(f'无法解析数量: {text}') from exc
    if not value.is_finite():
        raise ValueError('数量必须为有限十进制')
    if not allow_zero and value == 0:
        raise ValueError('流水数量不能为 0')
    return value


def precheck_bundle(connection, bundle, tz):
    movements = bundle.get('movements', [])
    counts = bundle.get('counts', [])

    in_bundle = {row['movement_id']: row for row in movements}
    reversal_totals = {}

    for row in movements:
        quantity = parse_quantity(row['quantity'], allow_zero=False)
        instant = parse_instant(row['event_time'])
        if row['day'] != business_day(instant, tz).isoformat():
            raise ValueError(
                f"流水 {row['movement_id']} 的业务日 {row['day']} 与时刻 "
                f"{row['event_time']} 在时区 {tz.key} 下的归属日不一致"
            )
        parse_day(row['day'])
        reversal_of = row['reversal_of']
        if reversal_of is None:
            continue
        if reversal_of == row['movement_id']:
            raise ValueError(f'流水 {row["movement_id"]} 不能冲销自身')
        original = in_bundle.get(reversal_of)
        if original is None:
            record = connection.execute(
                'SELECT movement_id, quantity, reversal_of FROM movements WHERE movement_id=?',
                (reversal_of,),
            ).fetchone()
            if record is None:
                raise ValueError(f'流水 {row["movement_id"]} 冲销的原流水 {reversal_of} 不存在')
            original = dict(record)
        if original.get('reversal_of') is not None:
            raise ValueError(f'冲销记录 {reversal_of} 不能再被冲销')
        original_quantity = parse_quantity(original['quantity'], allow_zero=False)
        if original_quantity * quantity >= 0:
            raise ValueError(
                f'流水 {row["movement_id"]} 与原流水 {reversal_of} 符号相反才能冲销'
            )
        reversal_totals[reversal_of] = reversal_totals.get(reversal_of, Decimal('0')) + abs(quantity)

    for movement_id, amount in reversal_totals.items():
        if movement_id in in_bundle:
            original_quantity = parse_quantity(in_bundle[movement_id]['quantity'], allow_zero=False)
        else:
            original_quantity = parse_quantity(
                connection.execute('SELECT quantity FROM movements WHERE movement_id=?', (movement_id,)).fetchone()['quantity'],
                allow_zero=False,
            )
        # 已落库的历史冲销也要计入累计，跨批次超量同样拒绝。
        persisted = sum(
            (abs(parse_quantity(record['quantity'], allow_zero=False))
             for record in connection.execute(
                 'SELECT quantity FROM movements WHERE reversal_of=?', (movement_id,)
             )),
            Decimal('0'),
        )
        if amount + persisted > abs(original_quantity):
            raise ValueError(f'流水 {movement_id} 的累计冲销量超过原量')

    seen_count_versions = set()
    for row in counts:
        parse_quantity(row['quantity'], allow_zero=True)
        instant = parse_instant(row['cutoff_time'])
        if row['cutoff_day'] != business_day(instant, tz).isoformat():
            raise ValueError(
                f"盘点 {row['count_id']} 的截止业务日 {row['cutoff_day']} 与截止时刻 "
                f"{row['cutoff_time']} 在时区 {tz.key} 下的归属日不一致"
            )
        parse_day(row['cutoff_day'])
        signature = (row['warehouse'], row['sku'], row['lot'], row['cutoff_day'], row['version'])
        if signature in seen_count_versions:
            raise ValueError(f'同业务日的盘点版本重复: {signature}')
        seen_count_versions.add(signature)
