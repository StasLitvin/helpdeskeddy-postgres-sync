"""
Менеджер синхронизации с CLI интерфейсом
Запуск: python sync_manager.py --help
"""

import argparse
from datetime import datetime, timedelta
from helpdesk import HelpDeskSync, logger

API_KEY = ""
DOMAIN = "https://mospoly.helpdeskeddy.com/"
DATABASE_URL = "postgresql+psycopg2://postgres:PASSWORD@localhost:5432/helpdesk?client_encoding=utf8"

def sync_year_command(args):
    """Синхронизация за год"""
    sync = HelpDeskSync(API_KEY, DOMAIN, DATABASE_URL)
    try:
        sync.sync_dictionaries()
        sync.sync_year(args.year, chunk_months=args.chunk)
        sync.get_stats()
    finally:
        sync.close()

def sync_period_command(args):
    """Синхронизация за произвольный период"""
    sync = HelpDeskSync(API_KEY, DOMAIN, DATABASE_URL)
    try:
        sync.sync_dictionaries()
        sync.sync_tickets_by_date_range(
            f"{args.from_date} 00:00:00",
            f"{args.to_date} 23:59:59"
        )
        sync.get_stats()
    finally:
        sync.close()

def sync_recent_command(args):
    """Синхронизация за последние N дней"""
    sync = HelpDeskSync(API_KEY, DOMAIN, DATABASE_URL)
    try:
        to_date = datetime.now()
        from_date = to_date - timedelta(days=args.days)

        sync.sync_dictionaries()
        sync.sync_tickets_by_date_range(
            from_date.strftime('%Y-%m-%d 00:00:00'),
            to_date.strftime('%Y-%m-%d 23:59:59')
        )
        sync.get_stats()
    finally:
        sync.close()

def stats_command(args):
    """Показать статистику"""
    sync = HelpDeskSync(API_KEY, DOMAIN, DATABASE_URL)
    try:
        sync.get_stats()
    finally:
        sync.close()

def main():
    parser = argparse.ArgumentParser(
        description='HelpDeskEddy Sync Manager',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Примеры использования:
  python sync_manager.py year 2025              # Синхронизация за 2025 год
  python sync_manager.py year 2025 --chunk 2    # По 2 месяца за раз
  python sync_manager.py period 2025-01-01 2025-06-30  # За период
  python sync_manager.py recent 7               # За последние 7 дней
  python sync_manager.py stats                  # Показать статистику
        """
    )

    subparsers = parser.add_subparsers(dest='command', help='Команды')

    year_parser = subparsers.add_parser('year', help='Синхронизация за год')
    year_parser.add_argument('year', type=int, help='Год (например, 2025)')
    year_parser.add_argument('--chunk', type=int, default=1,
                             help='Количество месяцев в одном чанке (по умолчанию: 1)')
    year_parser.set_defaults(func=sync_year_command)

    period_parser = subparsers.add_parser('period', help='Синхронизация за период')
    period_parser.add_argument('from_date', help='Дата начала (YYYY-MM-DD)')
    period_parser.add_argument('to_date', help='Дата окончания (YYYY-MM-DD)')
    period_parser.set_defaults(func=sync_period_command)

    recent_parser = subparsers.add_parser('recent', help='Синхронизация за последние N дней')
    recent_parser.add_argument('days', type=int, help='Количество дней')
    recent_parser.set_defaults(func=sync_recent_command)

    stats_parser = subparsers.add_parser('stats', help='Показать статистику')
    stats_parser.set_defaults(func=stats_command)

    args = parser.parse_args()

    if args.command is None:
        parser.print_help()
        return

    args.func(args)

if __name__ == "__main__":
    main()
