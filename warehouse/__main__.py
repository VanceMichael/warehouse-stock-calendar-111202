import argparse
import json
from pathlib import Path

from .catalog import connect, load_file
from .clock import parse_day
from .domain import VIEWS
from .snapshots import rebuild, resume_and_publish
from .store import ObjectStore


def main():
    parser = argparse.ArgumentParser(description='库存事实装载、周期快照重建与查询')
    parser.add_argument('command', choices=['import', 'rebuild', 'resume', 'inspect', 'export'])
    parser.add_argument('--db', required=True)
    parser.add_argument('--input')
    parser.add_argument('--batch')
    parser.add_argument('--objects')
    parser.add_argument('--timezone', default='Asia/Shanghai')
    parser.add_argument('--view', choices=sorted(VIEWS), default='overview')
    parser.add_argument('--month')
    parser.add_argument('--run-id')
    parser.add_argument('--failure-point')
    parser.add_argument('--as-of', help='核算截至日 YYYY-MM-DD，默认取事实水印日')
    args = parser.parse_args()
    if args.command == 'import' and (not args.input or not args.batch):
        parser.error('导入需要 --input 和 --batch')
    if args.command in ('export',) and not args.objects:
        parser.error('导出需要 --objects')
    if args.command == 'resume' and not args.run_id:
        parser.error('恢复重建需要 --run-id')
    Path(args.db).parent.mkdir(parents=True, exist_ok=True)
    connection = connect(args.db, args.timezone)
    try:
        if args.command == 'import':
            result = {'changed': load_file(connection, args.input, args.batch)}
        elif args.command == 'rebuild':
            as_of = parse_day(args.as_of) if args.as_of else None
            run_id, plan = rebuild(connection, failure_point=args.failure_point, as_of=as_of)
            result = {
                'run_id': run_id,
                'intervals': [
                    {'key': list(item['key']), 'start': item['start'].isoformat(), 'end': item['end'].isoformat()}
                    for item in plan
                ],
            }
        elif args.command == 'resume':
            result = {'run_id': resume_and_publish(connection, args.run_id)}
        else:
            kwargs = {'month': args.month} if args.month and args.view in ('daily', 'monthly') else {}
            result = VIEWS[args.view](connection, **kwargs)
            if args.command == 'export':
                records = result if isinstance(result, list) else [result]
                result = {'object': ObjectStore(args.objects).put(records), 'rows': len(records)}
        print(json.dumps(result, ensure_ascii=False, sort_keys=True, allow_nan=False))
    finally:
        connection.close()


if __name__ == '__main__':
    main()
