import requests
import json
import time
from datetime import datetime, timedelta
from typing import Optional, Dict, List, Any
import logging
from sqlalchemy import create_engine, Column, Integer, String, Text, DateTime, Boolean, JSON, Index
from sqlalchemy.orm import declarative_base, sessionmaker
from sqlalchemy.dialects.postgresql import insert

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('helpdesk_sync.log', encoding='utf-8'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

Base = declarative_base()

class Ticket(Base):
    """Модель заявки"""
    __tablename__ = 'tickets'

    id = Column(Integer, primary_key=True)
    pid = Column(Integer, default=0)
    unique_id = Column(String(50), index=True)
    date_created = Column(DateTime, index=True)
    date_updated = Column(DateTime, index=True)
    title = Column(Text)
    source = Column(String(50))
    status_id = Column(String(50), index=True)
    priority_id = Column(Integer)
    type_id = Column(Integer)
    department_id = Column(Integer, index=True)
    department_name = Column(String(255))
    owner_id = Column(Integer, index=True)
    owner_name = Column(String(255))
    owner_lastname = Column(String(255))
    owner_email = Column(String(255))
    user_id = Column(Integer, index=True)
    user_name = Column(String(255))
    user_lastname = Column(String(255))
    user_email = Column(String(255))
    cc = Column(JSON, default=[])
    bcc = Column(JSON, default=[])
    followers = Column(JSON, default=[])
    ticket_lock = Column(Integer, default=0)
    sla_date = Column(String(50))
    sla_flag = Column(Integer, default=0)
    freeze_date = Column(String(50))
    freeze = Column(Integer, default=0)
    viewed_by_staff = Column(Integer, default=0)
    viewed_by_client = Column(Integer, default=0)
    rate = Column(String(50))
    rate_comment = Column(Text)
    rate_date = Column(String(50))
    deleted = Column(Integer, default=0)
    custom_fields = Column(JSON, default=[])
    tags = Column(JSON, default=[])
    jira_issues = Column(JSON, default=[])
    synced_at = Column(DateTime, default=datetime.utcnow)

class SyncProgress(Base):
    """Модель для отслеживания прогресса синхронизации"""
    __tablename__ = 'sync_progress'

    id = Column(Integer, primary_key=True, autoincrement=True)
    sync_type = Column(String(50))
    from_date = Column(String(30))
    to_date = Column(String(30))
    last_page = Column(Integer, default=0)
    total_pages = Column(Integer, default=0)
    total_tickets = Column(Integer, default=0)
    completed = Column(Boolean, default=False)
    started_at = Column(DateTime, default=datetime.utcnow)
    completed_at = Column(DateTime)

class Status(Base):
    """Справочник статусов"""
    __tablename__ = 'statuses'

    id = Column(String(50), primary_key=True)
    name_ru = Column(String(255))
    name_en = Column(String(255))
    name_ua = Column(String(255))

class Department(Base):
    """Справочник департаментов"""
    __tablename__ = 'departments'

    id = Column(Integer, primary_key=True)
    name_ru = Column(String(255))
    name_en = Column(String(255))
    name_ua = Column(String(255))

class Priority(Base):
    """Справочник приоритетов"""
    __tablename__ = 'priorities'

    id = Column(Integer, primary_key=True)
    name_ru = Column(String(255))
    name_en = Column(String(255))
    name_ua = Column(String(255))

class TicketType(Base):
    """Справочник типов заявок"""
    __tablename__ = 'ticket_types'

    id = Column(Integer, primary_key=True)
    name_ru = Column(String(255))
    name_en = Column(String(255))
    name_ua = Column(String(255))

class RateLimiter:
    """
    Rate Limiter для соблюдения ограничения 300 запросов в минуту.
    Использует консервативный подход - не более 250 запросов в минуту (с запасом).
    """

    def __init__(self, max_requests_per_minute: int = 250):
        self.max_requests = max_requests_per_minute
        self.requests_timestamps: List[float] = []
        self.remaining_from_api: Optional[int] = None
        self.min_interval = 60.0 / max_requests_per_minute

    def update_from_headers(self, headers: Dict):
        """Обновляем информацию о лимитах из заголовков ответа API"""
        if 'X-Rate-Limit-Remaining' in headers:
            self.remaining_from_api = int(headers['X-Rate-Limit-Remaining'])
            logger.debug(f"API Rate Limit Remaining: {self.remaining_from_api}")

    def wait_if_needed(self):
        """Ждём, если нужно, чтобы не превысить лимит"""
        now = time.time()

        self.requests_timestamps = [
            ts for ts in self.requests_timestamps
            if now - ts < 60
        ]

        if len(self.requests_timestamps) >= self.max_requests:
            oldest = min(self.requests_timestamps)
            sleep_time = 60 - (now - oldest) + 1
            if sleep_time > 0:
                logger.warning(
                    f"Rate limit reached ({len(self.requests_timestamps)} requests). Sleeping for {sleep_time:.1f} seconds...")
                time.sleep(sleep_time)
                self.requests_timestamps = []

        if self.remaining_from_api is not None and self.remaining_from_api < 20:
            logger.warning(f"API reports only {self.remaining_from_api} requests remaining. Sleeping 30 seconds...")
            time.sleep(30)
            self.remaining_from_api = None

        if self.requests_timestamps:
            time_since_last = now - self.requests_timestamps[-1]
            if time_since_last < self.min_interval:
                time.sleep(self.min_interval - time_since_last)

    def record_request(self):
        """Записываем факт запроса"""
        self.requests_timestamps.append(time.time())

def extract_name(name_data, default: str = '') -> Dict[str, str]:
    """
    Извлекает имена из данных API.
    API может вернуть строку или словарь {'ru': '...', 'en': '...', 'ua': '...'}
    """
    if name_data is None:
        return {'ru': default, 'en': '', 'ua': ''}

    if isinstance(name_data, str):
        return {'ru': name_data, 'en': '', 'ua': ''}

    if isinstance(name_data, dict):
        return {
            'ru': name_data.get('ru', default),
            'en': name_data.get('en', ''),
            'ua': name_data.get('ua', '')
        }

    return {'ru': str(name_data), 'en': '', 'ua': ''}

class DatabaseManager:
    """Менеджер базы данных PostgreSQL"""

    def __init__(self, database_url: str):
        self.engine = create_engine(database_url, echo=False, pool_pre_ping=True)
        Session = sessionmaker(bind=self.engine)
        self.session = Session()
        self._create_tables()

    def _create_tables(self):
        """Создаём таблицы если не существуют"""
        Base.metadata.create_all(self.engine)
        logger.info("Таблицы БД созданы/проверены")

    def upsert_ticket(self, ticket_data: Dict):
        """Вставить или обновить заявку (upsert)"""
        stmt = insert(Ticket).values(
            id=ticket_data.get('id'),
            pid=ticket_data.get('pid', 0),
            unique_id=ticket_data.get('unique_id'),
            date_created=self._parse_datetime(ticket_data.get('date_created')),
            date_updated=self._parse_datetime(ticket_data.get('date_updated')),
            title=ticket_data.get('title'),
            source=ticket_data.get('source'),
            status_id=ticket_data.get('status_id'),
            priority_id=ticket_data.get('priority_id'),
            type_id=ticket_data.get('type_id'),
            department_id=ticket_data.get('department_id'),
            department_name=ticket_data.get('department_name'),
            owner_id=ticket_data.get('owner_id'),
            owner_name=ticket_data.get('owner_name'),
            owner_lastname=ticket_data.get('owner_lastname'),
            owner_email=ticket_data.get('owner_email'),
            user_id=ticket_data.get('user_id'),
            user_name=ticket_data.get('user_name'),
            user_lastname=ticket_data.get('user_lastname'),
            user_email=ticket_data.get('user_email'),
            cc=ticket_data.get('cc', []),
            bcc=ticket_data.get('bcc', []),
            followers=ticket_data.get('followers', []),
            ticket_lock=ticket_data.get('ticket_lock', 0),
            sla_date=ticket_data.get('sla_date'),
            sla_flag=ticket_data.get('sla_flag', 0),
            freeze_date=ticket_data.get('freeze_date'),
            freeze=ticket_data.get('freeze', 0),
            viewed_by_staff=ticket_data.get('viewed_by_staff', 0),
            viewed_by_client=ticket_data.get('viewed_by_client', 0),
            rate=str(ticket_data.get('rate', '')),
            rate_comment=ticket_data.get('rate_comment'),
            rate_date=ticket_data.get('rate_date'),
            deleted=ticket_data.get('deleted', 0),
            custom_fields=ticket_data.get('custom_fields', []),
            tags=ticket_data.get('tags', []),
            jira_issues=ticket_data.get('jira_issues', []),
            synced_at=datetime.utcnow()
        )

        stmt = stmt.on_conflict_do_update(
            index_elements=['id'],
            set_={
                'pid': stmt.excluded.pid,
                'unique_id': stmt.excluded.unique_id,
                'date_created': stmt.excluded.date_created,
                'date_updated': stmt.excluded.date_updated,
                'title': stmt.excluded.title,
                'source': stmt.excluded.source,
                'status_id': stmt.excluded.status_id,
                'priority_id': stmt.excluded.priority_id,
                'type_id': stmt.excluded.type_id,
                'department_id': stmt.excluded.department_id,
                'department_name': stmt.excluded.department_name,
                'owner_id': stmt.excluded.owner_id,
                'owner_name': stmt.excluded.owner_name,
                'owner_lastname': stmt.excluded.owner_lastname,
                'owner_email': stmt.excluded.owner_email,
                'user_id': stmt.excluded.user_id,
                'user_name': stmt.excluded.user_name,
                'user_lastname': stmt.excluded.user_lastname,
                'user_email': stmt.excluded.user_email,
                'cc': stmt.excluded.cc,
                'bcc': stmt.excluded.bcc,
                'followers': stmt.excluded.followers,
                'ticket_lock': stmt.excluded.ticket_lock,
                'sla_date': stmt.excluded.sla_date,
                'sla_flag': stmt.excluded.sla_flag,
                'freeze_date': stmt.excluded.freeze_date,
                'freeze': stmt.excluded.freeze,
                'viewed_by_staff': stmt.excluded.viewed_by_staff,
                'viewed_by_client': stmt.excluded.viewed_by_client,
                'rate': stmt.excluded.rate,
                'rate_comment': stmt.excluded.rate_comment,
                'rate_date': stmt.excluded.rate_date,
                'deleted': stmt.excluded.deleted,
                'custom_fields': stmt.excluded.custom_fields,
                'tags': stmt.excluded.tags,
                'jira_issues': stmt.excluded.jira_issues,
                'synced_at': stmt.excluded.synced_at
            }
        )

        self.session.execute(stmt)

    def upsert_tickets_batch(self, tickets: List[Dict]):
        """Пакетная вставка заявок"""
        for ticket in tickets:
            self.upsert_ticket(ticket)
        self.session.commit()
        logger.debug(f"Сохранено {len(tickets)} заявок")

    def _parse_datetime(self, dt_str: Optional[str]) -> Optional[datetime]:
        """Парсинг строки даты"""
        if not dt_str:
            return None
        try:
            return datetime.strptime(dt_str, '%Y-%m-%d %H:%M:%S')
        except:
            return None

    def get_sync_progress(self, sync_type: str, from_date: str, to_date: str) -> Optional[Dict]:
        """Получить прогресс синхронизации для возобновления"""
        progress = self.session.query(SyncProgress).filter(
            SyncProgress.sync_type == sync_type,
            SyncProgress.from_date == from_date,
            SyncProgress.to_date == to_date,
            SyncProgress.completed == False
        ).order_by(SyncProgress.started_at.desc()).first()

        if progress:
            return {
                'last_page': progress.last_page,
                'total_pages': progress.total_pages,
                'total_tickets': progress.total_tickets
            }
            return None

    def save_sync_progress(self, sync_type: str, from_date: str, to_date: str,
                           last_page: int, total_pages: int, total_tickets: int,
                           completed: bool = False):
        """Сохранить прогресс синхронизации"""

        self.session.query(SyncProgress).filter(
            SyncProgress.sync_type == sync_type,
            SyncProgress.from_date == from_date,
            SyncProgress.to_date == to_date,
            SyncProgress.completed == False
        ).delete()

        progress = SyncProgress(
            sync_type=sync_type,
            from_date=from_date,
            to_date=to_date,
            last_page=last_page,
            total_pages=total_pages,
            total_tickets=total_tickets,
            completed=completed,
            started_at=datetime.utcnow(),
            completed_at=datetime.utcnow() if completed else None
        )
        self.session.add(progress)
        self.session.commit()

    def save_statuses(self, statuses: List[Dict]):
        """Сохранить справочник статусов"""
        for status in statuses:
            names = extract_name(status.get('name'))
            stmt = insert(Status).values(
                id=str(status.get('id')),
                name_ru=names['ru'],
                name_en=names['en'],
                name_ua=names['ua']
            ).on_conflict_do_update(
                index_elements=['id'],
                set_={
                    'name_ru': names['ru'],
                    'name_en': names['en'],
                    'name_ua': names['ua']
                }
            )
            self.session.execute(stmt)
        self.session.commit()
        logger.info(f"Сохранено {len(statuses)} статусов")

    def save_departments(self, departments: List[Dict]):
        """Сохранить справочник департаментов"""
        for dept in departments:
            names = extract_name(dept.get('name'))
            stmt = insert(Department).values(
                id=dept.get('id'),
                name_ru=names['ru'],
                name_en=names['en'],
                name_ua=names['ua']
            ).on_conflict_do_update(
                index_elements=['id'],
                set_={
                    'name_ru': names['ru'],
                    'name_en': names['en'],
                    'name_ua': names['ua']
                }
            )
            self.session.execute(stmt)
        self.session.commit()
        logger.info(f"Сохранено {len(departments)} департаментов")

    def save_priorities(self, priorities: List[Dict]):
        """Сохранить справочник приоритетов"""
        for priority in priorities:
            names = extract_name(priority.get('name'))
            stmt = insert(Priority).values(
                id=priority.get('id'),
                name_ru=names['ru'],
                name_en=names['en'],
                name_ua=names['ua']
            ).on_conflict_do_update(
                index_elements=['id'],
                set_={
                    'name_ru': names['ru'],
                    'name_en': names['en'],
                    'name_ua': names['ua']
                }
            )
            self.session.execute(stmt)
        self.session.commit()
        logger.info(f"Сохранено {len(priorities)} приоритетов")

    def save_types(self, types: List[Dict]):
        """Сохранить справочник типов заявок"""
        for ticket_type in types:
            names = extract_name(ticket_type.get('name'))
            stmt = insert(TicketType).values(
                id=ticket_type.get('id'),
                name_ru=names['ru'],
                name_en=names['en'],
                name_ua=names['ua']
            ).on_conflict_do_update(
                index_elements=['id'],
                set_={
                    'name_ru': names['ru'],
                    'name_en': names['en'],
                    'name_ua': names['ua']
                }
            )
            self.session.execute(stmt)
        self.session.commit()
        logger.info(f"Сохранено {len(types)} типов заявок")

    def get_tickets_count(self) -> int:
        """Получить общее количество заявок в БД"""
        return self.session.query(Ticket).count()

    def get_tickets_count_by_period(self, from_date: str, to_date: str) -> int:
        """Получить количество заявок за период"""
        from_dt = datetime.strptime(from_date, '%Y-%m-%d %H:%M:%S')
        to_dt = datetime.strptime(to_date, '%Y-%m-%d %H:%M:%S')
        return self.session.query(Ticket).filter(
            Ticket.date_created >= from_dt,
            Ticket.date_created < to_dt
        ).count()

    def close(self):
        """Закрыть соединение"""
        self.session.close()

class HelpDeskEddyAPI:
    """Клиент API HelpDeskEddy с поддержкой rate limiting"""

    def __init__(self, api_key: str, domain: str):
        self.api_key = api_key
        self.domain = domain.rstrip('/')
        self.base_url = f"{self.domain}/api/v2"

        self.headers = {
            'Authorization': f'Basic {api_key}',
            'Content-Type': 'application/json',
            'Cache-Control': 'no-cache'
        }

        self.rate_limiter = RateLimiter(max_requests_per_minute=250)
        self.request_count = 0

    def _make_request(self, method: str, endpoint: str, params: Dict = None) -> Optional[Dict]:
        """Выполнить запрос к API с учётом rate limiting"""

        self.rate_limiter.wait_if_needed()

        url = f"{self.base_url}/{endpoint.lstrip('/')}"

        try:
            self.rate_limiter.record_request()
            self.request_count += 1

            if method.upper() == 'GET':
                response = requests.get(url, headers=self.headers, params=params, timeout=30)
            else:
                response = requests.post(url, headers=self.headers, json=params, timeout=30)

            self.rate_limiter.update_from_headers(response.headers)

            response.raise_for_status()
            return response.json()

        except requests.exceptions.HTTPError as e:
            if e.response.status_code == 429:

                logger.error(f"Rate limit exceeded! Waiting 20 minutes...")
                time.sleep(20 * 60)
                return self._make_request(method, endpoint, params)
            logger.error(f"HTTP Error: {e}")
            return None
        except requests.exceptions.RequestException as e:
            logger.error(f"Request Error: {e}")
            return None

    def get_tickets(self, page: int = 1, filters: Dict = None) -> Optional[Dict]:
        """Получить список заявок"""
        params = {'page': page}
        if filters:
            params.update(filters)
        return self._make_request('GET', '/tickets/', params)

    def get_statuses(self) -> Optional[List[Dict]]:
        """Получить список статусов"""
        result = self._make_request('GET', '/statuses/')
        if result and 'data' in result:
            return result['data']
        return None

    def get_departments(self) -> Optional[List[Dict]]:
        """Получить список департаментов"""
        result = self._make_request('GET', '/departments/')
        if result and 'data' in result:

            data = result['data']
            if isinstance(data, dict):
                return list(data.values())
            return data
        return None

    def get_priorities(self) -> Optional[List[Dict]]:
        """Получить список приоритетов"""
        result = self._make_request('GET', '/priorities/')
        if result and 'data' in result:
            data = result['data']
            if isinstance(data, dict):
                return list(data.values())
            return data
        return None

    def get_types(self) -> Optional[List[Dict]]:
        """Получить список типов заявок"""
        result = self._make_request('GET', '/types/')
        if result and 'data' in result:
            data = result['data']
            if isinstance(data, dict):
                return list(data.values())
            return data
        return None

class HelpDeskSync:
    """Основной класс для синхронизации данных"""

    def __init__(self, api_key: str, domain: str, database_url: str):
        self.api = HelpDeskEddyAPI(api_key, domain)
        self.db = DatabaseManager(database_url)

    def sync_last_minutes(self, minutes: int = 30, overlap_minutes: int = 2):
        """
        Синхронизация заявок за последние N минут.
        overlap_minutes — небольшое перекрытие, чтобы не потерять заявки на границе интервалов.
        """
        now = datetime.now()
        from_dt = now - timedelta(minutes=minutes + overlap_minutes)

        from_date = from_dt.strftime('%Y-%m-%d %H:%M:%S')
        to_date = now.strftime('%Y-%m-%d %H:%M:%S')

        logger.info(f"Синхронизация за последние {minutes} минут (перекрытие {overlap_minutes} мин): {from_date} — {to_date}")

        return self.sync_tickets_by_date_range(from_date, to_date, resume=False)
    def sync_dictionaries(self):
        """Синхронизировать справочники"""
        logger.info("Синхронизация справочников...")

        statuses = self.api.get_statuses()
        if statuses:
            self.db.save_statuses(statuses)

        departments = self.api.get_departments()
        if departments:
            self.db.save_departments(departments)

        priorities = self.api.get_priorities()
        if priorities:
            self.db.save_priorities(priorities)

        types = self.api.get_types()
        if types:
            self.db.save_types(types)

        logger.info("Справочники синхронизированы")

    def sync_tickets_by_date_range(self, from_date: str, to_date: str, resume: bool = True):
        """
        Синхронизировать заявки за период

        :param from_date: Дата начала в формате 'YYYY-MM-DD HH:MM:SS'
        :param to_date: Дата окончания в формате 'YYYY-MM-DD HH:MM:SS'
        :param resume: Возобновить с последней страницы если прервано
        """
        logger.info(f"Синхронизация заявок с {from_date} по {to_date}")

        start_page = 1
        if resume:
            progress = self.db.get_sync_progress('tickets', from_date, to_date)
            if progress and progress['last_page'] > 0:
                start_page = progress['last_page'] + 1
                logger.info(f"Возобновление с страницы {start_page}")

        filters = {
            'from_date_created': from_date,
            'to_date_created': to_date,
            'order_by': 'date_created{asc}'
        }

        page = start_page
        total_synced = 0
        total_pages = None

        while True:
            logger.info(f"Загрузка страницы {page}" + (f"/{total_pages}" if total_pages else ""))

            result = self.api.get_tickets(page=page, filters=filters)

            if not result or 'data' not in result:
                logger.error(f"Ошибка получения данных на странице {page}")
                break

            tickets_data = result['data']
            pagination = result.get('pagination', {})
            total_pages = pagination.get('total_pages', 1)
            total_tickets = pagination.get('total', 0)

            if isinstance(tickets_data, dict):
                tickets_list = list(tickets_data.values())
            else:
                tickets_list = tickets_data

            if not tickets_list:
                logger.info("Больше нет заявок")
                break

            self.db.upsert_tickets_batch(tickets_list)
            total_synced += len(tickets_list)

            self.db.save_sync_progress(
                sync_type='tickets',
                from_date=from_date,
                to_date=to_date,
                last_page=page,
                total_pages=total_pages,
                total_tickets=total_tickets,
                completed=False
            )

            logger.info(
                f"Страница {page}/{total_pages}: сохранено {len(tickets_list)} заявок (всего: {total_synced})")

            if page >= total_pages:
                break

            page += 1

        self.db.save_sync_progress(
            sync_type='tickets',
            from_date=from_date,
            to_date=to_date,
            last_page=page,
            total_pages=total_pages or page,
            total_tickets=total_synced,
            completed=True
        )

        logger.info(f"Синхронизация завершена! Всего заявок: {total_synced}")
        return total_synced

    def sync_year(self, year: int, chunk_months: int = 1):
        """
        Синхронизировать заявки за год, разбивая по месяцам

        :param year: Год для синхронизации
        :param chunk_months: Количество месяцев в одном чанке (1 = помесячно)
        """
        logger.info(f"Синхронизация заявок за {year} год")

        total_synced = 0

        for month in range(1, 13, chunk_months):

            from_date = f"{year}-{month:02d}-01 00:00:00"

            end_month = month + chunk_months
            if end_month > 12:
                to_date = f"{year + 1}-01-01 00:00:00"
            else:
                to_date = f"{year}-{end_month:02d}-01 00:00:00"

            logger.info(f"Период: {from_date} - {to_date}")
        synced = self.sync_tickets_by_date_range(from_date, to_date)
        total_synced += synced

        logger.info(f"Год {year} синхронизирован! Всего заявок: {total_synced}")
        return total_synced

    def sync_recent(self, days: int = 7):
        """
        Синхронизировать заявки за последние N дней

        :param days: Количество дней
        """
        to_date = datetime.now()
        from_date = to_date - timedelta(days=days)

        logger.info(f"Синхронизация заявок за последние {days} дней")

        return self.sync_tickets_by_date_range(
            from_date.strftime('%Y-%m-%d 00:00:00'),
            to_date.strftime('%Y-%m-%d 23:59:59'),
            resume=False
        )

    def get_stats(self):
        """Получить статистику по БД"""
        total = self.db.get_tickets_count()
        logger.info(f"Всего заявок в БД: {total}")

        count_2025 = self.db.get_tickets_count_by_period('2025-01-01 00:00:00', '2026-01-01 00:00:00')
        logger.info(f"Заявок за 2025 год: {count_2025}")

        return {
            'total': total,
            '2025': count_2025
        }

    def close(self):
        """Закрыть соединения"""
        self.db.close()

def run_periodic(interval_minutes: int, window_minutes: int, overlap_minutes: int = 2):
    """
    Бесконечный цикл периодической синхронизации.
    interval_minutes — как часто запускать (например, 10)
    window_minutes — какое окно данных подтягивать (например, 15)
    overlap_minutes — перекрытие окна (например, 2)
    """
    API_KEY = ""
    DOMAIN = "https://mospoly.helpdeskeddy.com/"
    DATABASE_URL = "postgresql+psycopg2://postgres:PASSWORD@localhost:5432/helpdesk?client_encoding=utf8"

    logger.info("=" * 60)
    logger.info(f"РЕЖИМ АВТОСИНХРОНИЗАЦИИ: каждые {interval_minutes} мин, окно {window_minutes} мин")
    logger.info("=" * 60)

    next_run = time.time()

    while True:

        next_run += interval_minutes * 60

        try:
            sync = HelpDeskSync(API_KEY, DOMAIN, DATABASE_URL)

            sync.sync_dictionaries()

            synced = sync.sync_last_minutes(window_minutes, overlap_minutes=overlap_minutes)
            logger.info(f"Периодический цикл завершён. Синхронизировано заявок: {synced}")

            logger.info(f"Всего API запросов (за запуск): {sync.api.request_count}")

        except KeyboardInterrupt:
            logger.warning("Остановлено пользователем (Ctrl+C). Выходим из автоцикла.")
            break
        except Exception as e:
            logger.error(f"Ошибка в периодическом цикле: {e}", exc_info=True)

            time.sleep(10)
        finally:
            try:
                sync.close()
            except Exception:
                pass

        sleep_for = max(0, next_run - time.time())
        logger.info(f"Сон до следующего запуска: {sleep_for:.1f} сек")
        time.sleep(sleep_for)

def main():
    """Основная функция"""

    API_KEY = ""
    DOMAIN = "https://mospoly.helpdeskeddy.com/"
    DATABASE_URL = "postgresql+psycopg2://postgres:PASSWORD@localhost:5432/helpdesk?client_encoding=utf8"

    logger.info("=" * 60)
    logger.info("ЗАПУСК СИНХРОНИЗАЦИИ HELPDESKEDDY")
    logger.info("=" * 60)

    sync = HelpDeskSync(API_KEY, DOMAIN, DATABASE_URL)

    try:

        sync.sync_dictionaries()

        logger.info("=" * 60)
        logger.info("НАЧАЛО СИНХРОНИЗАЦИИ ЗАЯВОК ЗА 2025 ГОД")
        logger.info("=" * 60)

        sync.sync_year(2025, chunk_months=1)

        logger.info("=" * 60)
        logger.info("ИТОГОВАЯ СТАТИСТИКА")
        logger.info("=" * 60)
        sync.get_stats()
        logger.info(f"Всего API запросов: {sync.api.request_count}")

    except KeyboardInterrupt:
        logger.warning("Синхронизация прервана пользователем (Ctrl+C)")
        logger.info("При следующем запуске синхронизация продолжится с последней страницы")
    except Exception as e:
        logger.error(f"Ошибка: {e}", exc_info=True)
    finally:
        sync.close()
        logger.info("Соединения закрыты")

def cli():
    """Интерфейс командной строки"""
    import argparse

    API_KEY = ""
    DOMAIN = "https://mospoly.helpdeskeddy.com/"
    DATABASE_URL = "postgresql+psycopg2://postgres:PASSWORD@localhost:5432/helpdesk?client_encoding=utf8"

    parser = argparse.ArgumentParser(
        description='HelpDeskEddy Sync - Синхронизация заявок в PostgreSQL',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Примеры использования:
  python helpdesk_sync.py                        # Полная синхронизация за 2025 год
  python helpdesk_sync.py --year 2024            # Синхронизация за 2024 год
  python helpdesk_sync.py --recent 7             # За последние 7 дней
  python helpdesk_sync.py --from 2025-01-01 --to 2025-03-31  # За период
  python helpdesk_sync.py --stats                # Только статистика
  python helpdesk_sync.py --dictionaries         # Только справочники

 Ограничение API: 300 запросов в минуту (используется 250 для безопасности)
При прерывании (Ctrl+C) синхронизация продолжится с последней страницы
        """
    )

    parser.add_argument('--year', type=int, help='Синхронизировать за указанный год')
    parser.add_argument('--recent', type=int, metavar='DAYS', help='Синхронизировать за последние N дней')
    parser.add_argument('--from', dest='from_date', metavar='YYYY-MM-DD', help='Дата начала периода')
    parser.add_argument('--to', dest='to_date', metavar='YYYY-MM-DD', help='Дата окончания периода')
    parser.add_argument('--stats', action='store_true', help='Показать только статистику')
    parser.add_argument('--dictionaries', action='store_true', help='Синхронизировать только справочники')
    parser.add_argument('--chunk', type=int, default=1, help='Месяцев в одном чанке (по умолчанию: 1)')

    args = parser.parse_args()

    sync = HelpDeskSync(API_KEY, DOMAIN, DATABASE_URL)

    try:
        if args.stats:

            sync.get_stats()

        elif args.dictionaries:

            sync.sync_dictionaries()

        elif args.recent:

            sync.sync_dictionaries()
            sync.sync_recent(args.recent)
            sync.get_stats()

        elif args.from_date and args.to_date:

            sync.sync_dictionaries()
            sync.sync_tickets_by_date_range(
                f"{args.from_date} 00:00:00",
                f"{args.to_date} 23:59:59"
            )
            sync.get_stats()

        elif args.year:

            sync.sync_dictionaries()
            sync.sync_year(args.year, args.chunk)
            sync.get_stats()

        else:

            sync.sync_dictionaries()
            sync.sync_year(2025, args.chunk)
            sync.get_stats()

        logger.info(f"Всего API запросов: {sync.api.request_count}")

    except KeyboardInterrupt:
        logger.warning("\nСинхронизация прервана пользователем")
        logger.info("При следующем запуске продолжится с последней страницы")
    except Exception as e:
        logger.error(f"Ошибка: {e}", exc_info=True)
    finally:
        sync.close()

if __name__ == "__main__":
    import sys

    if len(sys.argv) > 1:
        cli()
    else:

        run_periodic(interval_minutes=10, window_minutes=100800, overlap_minutes=2)
