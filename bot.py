import time
import json
from google.oauth2.service_account import Credentials as SACreds
import os, io, asyncio, logging, re
import re
URL_RE = re.compile('(?i)\\b((?:https?|ftp)://[^\\s<>]+|www\\.[^\\s<>]+|[a-z0-9.-]+\\.[a-z]{2,}[^\\s<>]*)')

def extract_urls(text: str):
    """Виділяє всі посилання з тексту (http, https, ftp, www, доменні)."""
    return URL_RE.findall(text or '')
from dataclasses import dataclass, field
from datetime import datetime, date, timedelta
from zoneinfo import ZoneInfo
from typing import Dict, List, Optional, Tuple
from urllib.parse import quote
from dotenv import load_dotenv
from google.cloud import storage
import re
DATE_RE = re.compile('^(\\d{1,2})\\.(\\d{1,2})(?:\\.(\\d{2,4}))?$')
NP_POSTOMAT_REF = 'f9316480-5f2d-425d-bc2c-ac7cd29decf0'
from aiogram import Bot, Dispatcher, F
from aiogram.filters import CommandStart
from aiogram.types import Message, ReplyKeyboardMarkup, KeyboardButton, InlineKeyboardMarkup, InlineKeyboardButton, CallbackQuery, ContentType, ReplyKeyboardRemove

async def _show_np_menu(msg: Message) -> None:
    """Show NP menu with proper clearing and state."""
    await _clear_inline_markup(msg)
    profiles = np_profiles_list(msg.chat.id) if 'np_profiles_list' in globals() else []
    await msg.answer('Доставити замовлення Новою Поштою:', reply_markup=np_menu_kb(has_saved=bool(profiles)))
    st = state_by_chat.get(msg.chat.id)
    if st:
        st.prev_step = 'np_menu'
        st.current_step = 'np_menu'

async def _clear_inline_markup(msg: Message) -> None:
    """Best-effort remove lingering inline keyboards from the last inline message we sent."""
    st = state_by_chat.get(msg.chat.id)
    if st and getattr(st, 'last_inline_msg_id', None):
        mid = st.last_inline_msg_id
        try:
            await msg.bot.edit_message_reply_markup(chat_id=msg.chat.id, message_id=mid, reply_markup=None)
        except Exception:
            try:
                await msg.bot.delete_message(chat_id=msg.chat.id, message_id=mid)
            except Exception:
                pass
        st.last_inline_msg_id = None
    try:
        await msg.bot.edit_message_reply_markup(chat_id=msg.chat.id, message_id=msg.message_id, reply_markup=None)
    except Exception:
        pass
from aiogram.types import Message, ReplyKeyboardMarkup, KeyboardButton, InlineKeyboardMarkup, InlineKeyboardButton, CallbackQuery, ContentType, ReplyKeyboardRemove
import aiohttp
import gspread
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseUpload
from google.auth.transport.requests import Request
from google_auth_oauthlib.flow import InstalledAppFlow
from google.oauth2.credentials import Credentials as UserCreds
logging.basicConfig(level=logging.INFO, format='%(levelname)s:%(name)s:%(message)s')
logger = logging.getLogger('ambalab')
import html
def _nonempty_text(s: str, min_len: int = 1) -> bool:
    return bool((s or '').strip()) and len((s or '').strip()) >= min_len

def _text_only(s: str, min_len: int = 1) -> bool:
    """
    Дозволяємо лише літери (укр/лат), пробіли, апостроф/дефіс. Міндовжина — параметром.
    """
    t = (s or '').strip()
    if len(t) < min_len:
        return False
    return re.fullmatch(r"[A-Za-zА-Яа-яЁёІіЇїЄє'’\- ]+", t) is not None

async def _safe_append_row(values: Dict[str, str], msg) -> Optional[int]:
    try:
        return append_row(values)
    except Exception:
        logger.exception('Sheets append_row failed')
        await msg.answer("Тимчасові складності зі зв'язком. Спробуйте пізніше.")
        return None

async def _safe_set_cell(row: int, col_name: str, value, msg) -> bool:
    try:
        set_cell(row, col_name, value)
        return True
    except Exception:
        logger.exception('Sheets set_cell(%s) failed', col_name)
        await msg.answer("Тимчасові складності зі зв'язком. Спробуйте пізніше.")
        return False
load_dotenv()
BOOT_TS = int(time.time())
ORDER_PREFIX = 'VZ'
BOT_TOKEN = os.getenv('BOT_TOKEN')
SHEETS_SPREADSHEET_ID = os.getenv('SHEETS_SPREADSHEET_ID')
SHEETS_WORKSHEET_NAME = os.getenv('SHEETS_WORKSHEET_NAME', 'Лист1')
GOOGLE_AUTH_MODE = os.getenv('GOOGLE_AUTH_MODE', 'desktop')
OAUTH_CLIENT_SECRETS_JSON = os.getenv('OAUTH_CLIENT_SECRETS_JSON', 'oauth_client.json')
OAUTH_TOKEN_PATH = os.getenv('OAUTH_TOKEN_PATH', 'token_user.json')
FILES_CHANNEL_ID = int(os.getenv('FILES_CHANNEL_ID', '0'))
DRIVE_PARENT_FOLDER_ID = os.getenv('DRIVE_PARENT_FOLDER_ID', '').strip() or None
LAB_EMAIL = os.getenv('LAB_EMAIL', '').strip() or 'orders@example.com'
ADMIN_CHAT_ID = int(os.getenv('ADMIN_CHAT_ID', '0'))
DRIVE_SHARE_ANYONE = os.getenv('DRIVE_SHARE_ANYONE', '0') == '1'
NOVAPOSHTA_API_KEY = os.getenv('NOVAPOSHTA_API_KEY', '').strip()
assert BOT_TOKEN, 'BOT_TOKEN is empty'
assert SHEETS_SPREADSHEET_ID, 'SHEETS_SPREADSHEET_ID is empty'
assert OAUTH_CLIENT_SECRETS_JSON and os.path.exists(OAUTH_CLIENT_SECRETS_JSON), 'Set OAUTH_CLIENT_SECRETS_JSON and put oauth_client.json next to bot.py'
assert NOVAPOSHTA_API_KEY, 'NOVAPOSHTA_API_KEY is empty'
SCOPES = ['https://www.googleapis.com/auth/spreadsheets', 'https://www.googleapis.com/auth/drive.file', 'https://www.googleapis.com/auth/drive']
PRICE_URL = 'https://drive.google.com/file/d/1kjTVfhkm384f35SkaogaRtDXwPqjFkJc/view?usp=drive_link'

def get_creds():
    """
    Повертає креденшли для Google API.
    Режими:
      - GOOGLE_AUTH_MODE=desktop  -> Desktop OAuth (InstalledAppFlow) + token_user.json
      - GOOGLE_AUTH_MODE=service  -> Service Account JSON
    За замовчуванням desktop — щоб нічого не міняти у твоїй роботі зараз.
    """
    mode = (os.getenv('GOOGLE_AUTH_MODE', 'desktop') or 'desktop').lower()
    cfg = OAUTH_CLIENT_SECRETS_JSON
    tok = OAUTH_TOKEN_PATH
    if mode == 'service':
        if os.path.exists(cfg):
            return SACreds.from_service_account_file(cfg, scopes=SCOPES)
        raise FileNotFoundError(f'Service Account JSON не знайдено: {cfg}')
    creds = None
    if os.path.exists(tok):
        try:
            creds = UserCreds.from_authorized_user_file(tok, SCOPES)
        except Exception:
            creds = None
    if not creds or not creds.valid:
        if creds and creds.expired and creds.refresh_token:
            creds.refresh(Request())
        else:
            flow = InstalledAppFlow.from_client_secrets_file(cfg, SCOPES)
            try:
                creds = flow.run_local_server(port=0)
            except Exception:
                creds = flow.run_console()
        with open(tok, 'w', encoding='utf-8') as f:
            f.write(creds.to_json())
    return creds

def get_google_clients():
    creds = get_creds()
    gc = gspread.authorize(creds)
    sh = gc.open_by_key(SHEETS_SPREADSHEET_ID)
    ws = sh.worksheet(SHEETS_WORKSHEET_NAME)
    drive = build('drive', 'v3', credentials=creds, cache_discovery=False)
    return (gc, sh, ws, drive)
gc, sh, ws, drive = get_google_clients()

def append_files_method(row: int, method_key: str):
    """Append unique method keys to the existing 'files_method' column, comma-separated."""
    col = 'files_method'
    prev = (get_cell(row, col) or '').strip()
    parts = [p.strip() for p in prev.split(',') if p.strip()] if prev else []
    if method_key not in parts:
        parts.append(method_key)
        set_cell(row, col, ', '.join(parts))

def headers_map(ws_) -> Dict[str, int]:
    return {name.strip(): idx + 1 for idx, name in enumerate(ws_.row_values(1))}

def set_cell(row: int, col_name: str, value):
    col = headers_map(ws).get(col_name)
    if not col:
        logger.warning('Sheets: column %r not found', col_name)
        return
    ws.update_cell(row, col, str(value))

def get_cell(row: int, col_name: str) -> str:
    col = headers_map(ws).get(col_name)
    return ws.cell(row, col).value if col else ''

def append_row(values: Dict[str, str]) -> int:
    head = headers_map(ws)
    row_dict = {**{h: '' for h in head.keys()}, **values}
    ws.append_row([row_dict.get(h, '') for h in head.keys()], value_input_option='USER_ENTERED')
    order_id = values.get('order_id')
    cell = ws.find(order_id) if order_id else None
    return cell.row if cell else ws.row_count

def update_joined(row: int, col_name: str, items: List[str]) -> str:
    prev = (get_cell(row, col_name) or '').strip()
    merged = (prev + ' ' if prev else '') + ' '.join(items)
    set_cell(row, col_name, merged)
    return merged

def np_profiles_ws():
    return sh.worksheet('Лист2')

def np_head(ws_) -> Dict[str, int]:
    return {h.strip(): i + 1 for i, h in enumerate(ws_.row_values(1))}

def np_profile_get(chat_id: int) -> dict:
    ws2 = np_profiles_ws()
    head = np_head(ws2)
    try:
        row = ws2.find(str(chat_id)).row

        def v(k):
            c = head.get(k)
            return ws2.cell(row, c).value if c else ''
        return {'phone': v('phone'), 'recipient_name': v('recipient_name'), 'recipient_phone': v('recipient_phone'), 'np_city_name': v('np_city_name'), 'np_warehouse_desc': v('np_warehouse_desc')}
    except Exception:
        return {}

def np_profile_upsert(chat_id: int, profile: dict):
    """
    MULTI-ROW LOGIC:
    - Для одного chat_id може бути багато адрес (рядків).
    - Якщо знайдено РЯДОК із ТАКОЮ Ж адресою (np_city_ref + np_warehouse_ref + recipient_phone) — оновлюємо його.
    - Інакше додаємо НОВИЙ рядок (append).
    """
    ws2 = np_profiles_ws()
    head = np_head(ws2)
    chat_col = head.get('chat_id')
    if not chat_col:
        row_dict = {h: '' for h in head.keys()}
        row_dict['chat_id'] = str(chat_id)
        for k, v in profile.items():
            if k in head:
                row_dict[k] = str(v or '')
        if head.get('updated_at'):
            row_dict['updated_at'] = datetime.now().strftime('%Y-%m-%d %H:%M')
        ws2.append_row([row_dict.get(h, '') for h in head.keys()], value_input_option='USER_ENTERED')
        return
    try:
        col_vals = ws2.col_values(chat_col)
    except Exception:
        col_vals = []
    target = str(chat_id).strip()
    match_rows = [idx + 1 for idx, val in enumerate(col_vals) if str(val).strip() == target]
    keys = ['np_city_ref', 'np_warehouse_ref', 'recipient_phone']

    def row_value(row_vals, key):
        c = head.get(key)
        return row_vals[c - 1] if c and c - 1 < len(row_vals) else ''
    same_row = None
    for row in match_rows:
        try:
            row_vals = ws2.row_values(row)
        except Exception:
            continue
        is_same = True
        for k in keys:
            want = str(profile.get(k, '') or '').strip()
            have = str(row_value(row_vals, k)).strip()
            if want != have:
                is_same = False
                break
        if is_same:
            same_row = row
            break
    if same_row:
        for k, v in profile.items():
            c = head.get(k)
            if c:
                ws2.update_cell(same_row, c, str(v or ''))
        if head.get('updated_at'):
            try:
                ws2.update_cell(same_row, head['updated_at'], datetime.now().strftime('%Y-%m-%d %H:%M'))
            except Exception:
                pass
        return
    row_dict = {h: '' for h in head.keys()}
    row_dict['chat_id'] = str(chat_id)
    for k, v in profile.items():
        if k in head:
            row_dict[k] = str(v or '')
    if head.get('updated_at'):
        row_dict['updated_at'] = datetime.now().strftime('%Y-%m-%d %H:%M')
    ws2.append_row([row_dict.get(h, '') for h in head.keys()], value_input_option='USER_ENTERED')

def np_save_current_delivery(chat_id: int, st):
    """Зчитує поточні поля з Лист1 і зберігає/оновлює профіль у Лист2 (upsert)."""
    try:
        np_profile_upsert(chat_id, {'recipient_name': get_cell(st.sheet_row, 'recipient_name'), 'recipient_phone': normalize_ua_phone(get_cell(st.sheet_row, 'recipient_phone')) or get_cell(st.sheet_row, 'recipient_phone'), 'np_city_name': get_cell(st.sheet_row, 'np_city_name'), 'np_city_ref': get_cell(st.sheet_row, 'np_city_ref'), 'np_warehouse_desc': get_cell(st.sheet_row, 'np_warehouse_desc'), 'np_warehouse_ref': get_cell(st.sheet_row, 'np_warehouse_ref')})
    except Exception:
        pass

def doctor_phone_get(chat_id: int) -> str:
    """Return doctor's personal phone for chat_id if present in Лист2; else empty string."""
    ws2 = np_profiles_ws()
    head = np_head(ws2)
    chat_col = head.get('chat_id')
    phone_col = head.get('phone')
    if not (chat_col and phone_col):
        return ''
    try:
        col_vals = ws2.col_values(chat_col)
    except Exception:
        return ''
    target = str(chat_id).strip()
    match_rows = [idx + 1 for idx, val in enumerate(col_vals) if str(val).strip() == target]
    if not match_rows:
        return ''
    for row_idx in reversed(match_rows):
        try:
            ph = ws2.cell(row_idx, phone_col).value
            if str(ph or '').strip():
                return str(ph).strip()
        except Exception:
            continue
    return ''

def doctor_phone_create(chat_id: int, phone: str):
    """
    Create one profile row with chat_id + phone ONLY if no rows for chat_id exist.
    Does NOT update existing rows.
    """
    ws2 = np_profiles_ws()
    head = np_head(ws2)
    chat_col = head.get('chat_id')
    phone_col = head.get('phone')
    if not chat_col:
        row_dict = {h: '' for h in head.keys()}
        row_dict['chat_id'] = str(chat_id)
        if phone_col:
            row_dict['phone'] = str(phone or '')
        if head.get('updated_at'):
            row_dict['updated_at'] = datetime.now().strftime('%Y-%m-%d %H:%M')
        ws2.append_row([row_dict.get(h, '') for h in head.keys()], value_input_option='USER_ENTERED')
        return
    try:
        col_vals = ws2.col_values(chat_col)
    except Exception:
        col_vals = []
    target = str(chat_id).strip()
    match_rows = [idx + 1 for idx, val in enumerate(col_vals) if str(val).strip() == target]
    if match_rows:
        return
    row_dict = {h: '' for h in head.keys()}
    row_dict['chat_id'] = str(chat_id)
    if phone_col:
        row_dict['phone'] = str(phone or '')
    if head.get('updated_at'):
        row_dict['updated_at'] = datetime.now().strftime('%Y-%m-%d %H:%M')
    ws2.append_row([row_dict.get(h, '') for h in head.keys()], value_input_option='USER_ENTERED')

def np_profiles_list(chat_id: int) -> List[dict]:
    """1 read for whole chat_id column + 1 per matched row. No external helpers."""
    ws2 = np_profiles_ws()
    head = np_head(ws2)
    chat_col = head.get('chat_id')
    if not chat_col:
        return []
    col_vals = []
    for i in range(3):
        try:
            col_vals = ws2.col_values(chat_col)
            break
        except Exception as e:
            if 'Quota exceeded' in str(e) or '429' in str(e):
                time.sleep(1 * 2 ** i)
                continue
            raise
    target = str(chat_id).strip()
    match_rows = [idx + 1 for idx, val in enumerate(col_vals) if str(val).strip() == target]
    profiles: List[dict] = []
    for row in match_rows:
        row_vals = []
        for i in range(3):
            try:
                row_vals = ws2.row_values(row)
                break
            except Exception as e:
                if 'Quota exceeded' in str(e) or '429' in str(e):
                    time.sleep(1 * 2 ** i)
                    continue
                raise

        def v(k: str) -> str:
            c = head.get(k)
            return row_vals[c - 1] if c and c - 1 < len(row_vals) else ''
        profiles.append({'_row': row, 'recipient_name': v('recipient_name'), 'recipient_phone': v('recipient_phone'), 'np_city_name': v('np_city_name'), 'np_city_ref': v('np_city_ref'), 'np_warehouse_desc': v('np_warehouse_desc'), 'np_warehouse_ref': v('np_warehouse_ref')})
    return profiles

def np_profile_add(chat_id: int, profile: dict):
    ws2 = np_profiles_ws()
    head = np_head(ws2)
    row_dict = {h: '' for h in head.keys()}
    row_dict.update({'chat_id': str(chat_id), 'recipient_name': profile.get('recipient_name', ''), 'recipient_phone': profile.get('recipient_phone', ''), 'np_city_name': profile.get('np_city_name', ''), 'np_warehouse_desc': profile.get('np_warehouse_desc', ''), 'updated_at': datetime.now().strftime('%Y-%m-%d %H:%M')})
    ws2.append_row([row_dict.get(h, '') for h in head.keys()], value_input_option='USER_ENTERED')

@dataclass
class OrderState:
    order_id: str = ''
    sheet_row: int = 0
    step: str = ''
    patient_lastname: str = ''
    work_type: str = ''
    due_date_iso: str = ''
    drive_folder_id: Optional[str] = None
    drive_folder_link: Optional[str] = None
    drive_file_links: List[str] = field(default_factory=list)
    telegram_file_ids: List[str] = field(default_factory=list)
    links_external: List[str] = field(default_factory=list)
    email: str = ''
    delivery_step: str = ''
    np_city_ref: str = ''
    np_warehouse_ref: str = ''
    offtopic_tries: int = 0
    seen_boot_ts: int = 0
state_by_chat: Dict[int, OrderState] = {}

async def _warn_or_reset_to_menu(msg: Message, st: "OrderState") -> bool:
    """Мʼяко попереджає, на 3-й раз відправляє в Головне меню. Повертає True, якщо щось зроблено."""
    try:
        st.offtopic_tries += 1
    except Exception:
        await msg.answer("Будь ласка, притримуйтесь сценарію оформлення замовлення.", reply_markup=bottom_nav_kb())
        return True
    if st.offtopic_tries >= 3:
        if getattr(st, "sheet_row", None):
            try:
                set_cell(st.sheet_row, "status", "cancelled")
            except Exception:
                pass
        await msg.answer("Бачу, що ми відхиляємось від сценарію. Повертаю у Головне меню.", reply_markup=main_kb())
        state_by_chat[msg.chat.id] = OrderState()
        return True
    await msg.answer("Будь ласка, притримуйтесь сценарію оформлення замовлення.", reply_markup=bottom_nav_kb())
    return True


def main_kb() -> ReplyKeyboardMarkup:
    return ReplyKeyboardMarkup(keyboard=[[KeyboardButton(text='🧾 Зробити замовлення'), KeyboardButton(text='📷 Instagram')], [KeyboardButton(text="☎️ Зв'язатися з техніком"), KeyboardButton(text='📂 Завантажити прайс')]], resize_keyboard=True)

def files_method_kb() -> ReplyKeyboardMarkup:
    return ReplyKeyboardMarkup(keyboard=[[KeyboardButton(text='📁 Завантажити у бот (до 2Гб)')], [KeyboardButton(text='🔗 Надати посилання')], [KeyboardButton(text='✉️ Надіслати на e-mail')], [KeyboardButton(text='⬅️ Назад'), KeyboardButton(text='🏠 Головне меню')]], resize_keyboard=True, one_time_keyboard=True)

def done_kb() -> ReplyKeyboardMarkup:
    return ReplyKeyboardMarkup(keyboard=[[KeyboardButton(text='✅ Готово')], [KeyboardButton(text='⬅️ Назад'), KeyboardButton(text='🏠 Головне меню')]], resize_keyboard=True, one_time_keyboard=True)

def bottom_nav_kb() -> ReplyKeyboardMarkup:
    return ReplyKeyboardMarkup(keyboard=[[KeyboardButton(text='⬅️ Назад'), KeyboardButton(text='🏠 Головне меню')]], resize_keyboard=True, one_time_keyboard=True)

def files_aux_kb() -> ReplyKeyboardMarkup:
    return ReplyKeyboardMarkup(keyboard=[[KeyboardButton(text='⬅️ Обрати інший спосіб'), KeyboardButton(text='✅ Готово')], [KeyboardButton(text='⬅️ Назад'), KeyboardButton(text='🏠 Головне меню')]], resize_keyboard=True, one_time_keyboard=True)
NP_MENU_ADD = '✏️ Додати нову адресу'
NP_MENU_USE_SAVED = '📦 На збережену адресу'
NP_MENU_SKIP = '⏭️ Пропустити'

def np_menu_kb(has_saved: bool) -> ReplyKeyboardMarkup:
    rows = [[KeyboardButton(text=NP_MENU_ADD)]]
    if has_saved:
        rows.append([KeyboardButton(text=NP_MENU_USE_SAVED)])
    rows.append([KeyboardButton(text=NP_MENU_SKIP)])
    rows.append([KeyboardButton(text='⬅️ Назад'), KeyboardButton(text='🏠 Головне меню')])
    return ReplyKeyboardMarkup(keyboard=rows, resize_keyboard=True)

def notes_yesno_kb():
    return ReplyKeyboardMarkup(keyboard=[[KeyboardButton(text='Так'), KeyboardButton(text='Ні')], [KeyboardButton(text='⬅️ Назад'), KeyboardButton(text='🏠 Головне меню')]], resize_keyboard=True, one_time_keyboard=True)

def confirm_cancel_kb(one_time: bool=True) -> ReplyKeyboardMarkup:
    return ReplyKeyboardMarkup(keyboard=[[KeyboardButton(text='Так, скасувати і в меню'), KeyboardButton(text='Ні, продовжити')], [KeyboardButton(text='⬅️ Назад'), KeyboardButton(text='🏠 Головне меню')]], resize_keyboard=True, one_time_keyboard=one_time)

def _cancel_and_to_menu(msg: Message):
    st = state_by_chat.get(msg.chat.id)
    if st and getattr(st, 'sheet_row', None):
        try:
            set_cell(st.sheet_row, 'status', 'cancelled_by_user')
        except Exception:
            pass
    state_by_chat[msg.chat.id] = OrderState()

def _prev_step(st: OrderState) -> tuple[str, str]:
    sname = st.step or ''
    dname = st.delivery_step or ''
    if dname == 'saved_pick':
        return ('np_menu', 'Доставити замовлення Новою Поштою:')
    if sname == 'await_notes':
        return ('await_notes_choice', 'Хочете додати текстові пояснення або голосове повідомлення?')
    if sname == 'await_notes_choice':
        return ('choose_files_method', 'Оберіть спосіб передачі файлів:')
    if sname == 'choose_files_method':
        return ('np_menu', 'Доставити замовлення Новою Поштою:')
    if sname in ('await_tele_files', 'await_links', 'email_wait_done'):
        return ('choose_files_method', 'Оберіть спосіб передачі файлів:')
    if dname == 'warehouse_text' or sname == 'await_np_number':
        return ('city_text', 'Вкажіть місто (наприклад: Київ):')
    if dname == 'city_text':
        return ('recv_phone', 'Введіть телефон отримувача (380XXXXXXXXX):')
    if dname == 'recv_phone':
        return ('recv_name', 'Вкажіть ПІБ отримувача:')
    if dname == 'recv_name':
        return ('np_menu', 'Доставити замовлення Новою Поштою:')
    if dname == 'saved_pick':
        return ('np_menu', 'Доставити замовлення Новою Поштою:')
    if sname == 'np_menu' or dname == 'recv_name' or dname:
        return ('due_date', 'Вкажіть дату здачі у форматі ДД.ММ або ДД.ММ.РРРР (наприклад 05.10):')
    if sname == 'due_date':
        return ('work_type', 'Вкажіть, будь ласка, який апарат замовляєте (сплінт, елайнери тощо):')
    if sname == 'work_type':
        return ('patient_lastname', 'Вкажіть, будь ласка, прізвище пацієнта:')
    if sname == 'patient_lastname':
        return ('doctor_phone', 'Вкажіть, будь ласка, <b>Ваш номер телефону</b> для звʼязку:')
    return ('doctor_phone', 'Вкажіть, будь ласка, <b>Ваш номер телефону</b> для звʼязку:')

def gen_order_id() -> str:
    return f"{ORDER_PREFIX}-{datetime.now().strftime('%y%m%d-%H%M%S')}"

def extract_urls(text: str) -> List[str]:
    return URL_RE.findall(text or '')

def parse_date_uk(text: str) -> Optional[str]:
    m = DATE_RE.match(text or '')
    if not m:
        return None
    d, mth, y = m.groups()
    y = y or str(date.today().year)
    try:
        return date(int(y), int(mth), int(d)).strftime('%Y-%m-%d')
    except Exception:
        return None

def normalize_ua_phone(s: str):
    """
    Нормалізує український номер у формат 380XXXXXXXXX.
    Приймає +380..., 380..., 0XXXXXXXXX або будь-які 9+ цифр з «сміттям».
    Повертає None, якщо цифр < 9.
    """
    import re
    digits = ''.join(re.findall('\\d+', s or ''))
    if not digits:
        return None
    if digits.startswith('380') and len(digits) >= 12:
        return digits[:12]
    if digits.startswith('0') and len(digits) >= 10:
        return '380' + digits[1:10]
    if len(digits) >= 9:
        return '380' + digits[-9:]
    return None
WH_RE = re.compile('(поштомат|відділення|viddilennya|poshtomat|postomat|branch)?\\s*(?:№|#|номер|no|num|n)?\\s*(\\d{1,4})', re.IGNORECASE)

def normalize_wh_query(raw: str) -> str:
    s = (raw or '').strip()
    m = WH_RE.search(s)
    if not m:
        return s
    kind, num = m.groups()
    kind = (kind or '').lower()
    if 'поштомат' in kind or 'postomat' in kind or 'poshtomat' in kind:
        return f'Поштомат №{int(num)}'
    return f'Відділення №{int(num)}'
MAIN_BTNS = {'🧾 Зробити замовлення', '📂 Завантажити прайс', '📷 Instagram', "☎️ Зв'язатися з техніком"}

async def _silent_autostart_on_first_menu_click(msg: Message):
    st = state_by_chat.get(msg.chat.id) or OrderState()
    state_by_chat[msg.chat.id] = st
    if st.seen_boot_ts == BOOT_TS:
        return
    st.seen_boot_ts = BOOT_TS
    return

async def ensure_order_folder(st: OrderState):
    if st.drive_folder_id:
        return
    meta = {'name': st.order_id, 'mimeType': 'application/vnd.google-apps.folder', **({'parents': [DRIVE_PARENT_FOLDER_ID]} if DRIVE_PARENT_FOLDER_ID else {})}
    folder = drive.files().create(body=meta, fields='id,webViewLink').execute()
    st.drive_folder_id = folder['id']
    st.drive_folder_link = folder.get('webViewLink', '')
    set_cell(st.sheet_row, 'drive_folder_link', st.drive_folder_link)

def share_anyone(file_id: str):
    if not DRIVE_SHARE_ANYONE:
        return
    try:
        drive.permissions().create(fileId=file_id, body={'type': 'anyone', 'role': 'reader'}).execute()
    except Exception as e:
        logger.warning('Share-anyone failed: %s', e)

async def upload_to_drive(st: OrderState, file_name: str, data: bytes, mime: Optional[str]) -> Tuple[str, str]:
    """
    Якщо STORAGE_BACKEND=gcs — зберігаємо у Google Cloud Storage і повертаємо (object_name, url).
    Інакше працюємо по-старому через Google Drive і повертаємо (file_id, webViewLink).
    """
    backend = os.getenv("STORAGE_BACKEND", "drive").lower()

    if backend == "gcs":
        bucket_name = os.getenv("GCS_BUCKET")
        if not bucket_name:
            raise RuntimeError("GCS_BUCKET not configured")

        from google.cloud import storage
        import datetime

        # Клієнт GCS під сервісним акаунтом Cloud Run
        client = storage.Client()
        bucket = client.bucket(bucket_name)

        # «Папка» замовлення всередині бакету
        folder = f"{st.order_id}/"
        object_name = folder + file_name

        # Завантаження
        blob = bucket.blob(object_name)
        blob.upload_from_string(
            data,
            content_type=(mime or "application/octet-stream")
        )

        # Формуємо посилання
        if os.getenv("GCS_PUBLIC", "0") == "1":
            url = f"https://storage.googleapis.com/{bucket_name}/{object_name}"
        else:
            ttl = int(os.getenv("GCS_SIGNED_URL_TTL", "86400"))
            expires = datetime.timedelta(seconds=ttl)
            url = blob.generate_signed_url(version="v4", expiration=expires, method="GET")

        return object_name, url

    # ------------ СТАРИЙ ШЛЯХ: ЗБЕРЕЖЕННЯ У GOOGLE DRIVE ------------
    from googleapiclient.http import MediaIoBaseUpload

    await ensure_order_folder(st)
    media = MediaIoBaseUpload(
        io.BytesIO(data),
        mimetype=(mime or "application/octet-stream"),
        resumable=True
    )
    meta = {"name": file_name, "parents": [st.drive_folder_id]}

    f = drive.files().create(
        body=meta,
        media_body=media,
        fields="id,webViewLink"
    ).execute()

    share_anyone(f["id"])
    return f["id"], f.get("webViewLink", "")
NP_API_URL = 'https://api.novaposhta.ua/v2.0/json/'

async def np_api_call(model: str, method: str, props: dict) -> dict:
    payload = {'apiKey': NOVAPOSHTA_API_KEY, 'modelName': model, 'calledMethod': method, 'methodProperties': props or {}}
    timeout = aiohttp.ClientTimeout(total=20)
    async with aiohttp.ClientSession(timeout=timeout) as s:
        async with s.post(NP_API_URL, json=payload) as r:
            r.raise_for_status()
            return await r.json()

async def np_search_cities(query: str, limit: int=10) -> list:
    resp = await np_api_call('Address', 'getCities', {'FindByString': query, 'Page': '1', 'Limit': str(limit)})
    return resp.get('data', []) if resp.get('success') else []

async def np_search_warehouses(city_ref: str, query: str, limit: int=20) -> list:
    if not city_ref:
        return []
    props = {'CityRef': city_ref, 'Page': '1', 'Limit': str(limit)}
    if query:
        props['FindByString'] = query
    resp = await np_api_call('Address', 'getWarehouses', props)
    return resp.get('data', []) if resp.get('success') else []

def nz(v):
    """Return em dash instead of None/empty/"None" strings for user-facing text."""
    return v if v not in (None, '', 'None') else '—'

def build_summary_text(st: OrderState) -> str:
    v = lambda name: get_cell(st.sheet_row, name)
    files_map = {'telegram_upload': 'Завантаження у бот', 'link': 'Посилання', 'email': 'E-mail'}
    files_human = files_map.get(v('files_method') or '', v('files_method') or '')
    return f"Ваше замовлення № <b>{nz(st.order_id)}</b> прийнято.\n\nПацієнт: <b>{nz(v('patient_lastname'))}</b>\nВид робіт: <b>{nz(v('work_type'))}</b>\nДата здачі: <b>{nz(v('due_date'))}</b>\nСпосіб передачі файлів: <b>{files_human}</b>\n\nДоставка:\n— Місто: <b>{nz(v('np_city_name'))}</b>\n— Відділення/Поштомат: <b>{nz(v('np_warehouse_desc'))}</b>\n— Отримувач: <b>{nz(v('recipient_name'))}</b>\n\nДякуємо за співпрацю 🙂\nТехнік звʼяжеться з Вами найближчим часом."
bot = Bot(BOT_TOKEN)
dp = Dispatcher()

async def notify_admin_new_order(msg: Message, st: OrderState):
    text = f"<b>Нове замовлення</b>\nНомер: <b>{nz(st.order_id)}</b>\nЛікар: {(msg.from_user.full_name if msg.from_user else '')} {('@' + msg.from_user.username if msg.from_user and msg.from_user.username else '')}\nПацієнт: {st.patient_lastname}\nАпарат: {st.work_type}\nДата здачі: {st.due_date_iso}"
    try:
        if not ADMIN_CHAT_ID:
            raise RuntimeError('ADMIN_CHAT_ID is empty or 0')
        await bot.send_message(ADMIN_CHAT_ID, text, parse_mode='HTML')
    except Exception as e:
        logger.warning('Admin notify failed: %s', e)

@dp.message(CommandStart())
async def start(msg: Message):
    await _clear_inline_markup(msg)
    state_by_chat[msg.chat.id] = OrderState()
    await msg.answer('Вітаємо! Це бот AmbaLab. Натисніть «🧾 Зробити замовлення», щоб розпочати.', reply_markup=main_kb())

@dp.message(F.text == '/menu')
async def menu_cmd(msg: Message):
    await _clear_inline_markup(msg)
    state_by_chat[msg.chat.id] = state_by_chat.get(msg.chat.id, OrderState())
    await msg.answer('Готові прийняти замовлення. Натисніть «🧾 Зробити замовлення».', reply_markup=main_kb())

@dp.message(F.text == '📷 Instagram')
async def instagram_btn(msg: Message):
    await _clear_inline_markup(msg)
    await _silent_autostart_on_first_menu_click(msg)
    ikb = InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text='Відкрити Instagram', url='https://www.instagram.com/ambalab.laboratory?igsh=eXN4NTVuenh1cHNs')]])
    resp = await msg.answer('Наш Instagram:', reply_markup=ikb)
    st = state_by_chat.get(msg.chat.id)
    if st:
        st.last_inline_msg_id = resp.message_id

@dp.message(F.text == '📂 Завантажити прайс')
async def price_btn(msg: Message):
    await _clear_inline_markup(msg)
    await _silent_autostart_on_first_menu_click(msg)
    ikb = InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text='Відкрити прайс (PDF)', url=PRICE_URL)]])
    resp = await msg.answer('Прайс (PDF):', reply_markup=ikb)
    st = state_by_chat.get(msg.chat.id)
    if st:
        st.last_inline_msg_id = resp.message_id

@dp.message(F.text == "☎️ Зв'язатися з техніком")
async def contact_tech(msg: Message):
    await _clear_inline_markup(msg)
    await _silent_autostart_on_first_menu_click(msg)
    try:
        if ADMIN_CHAT_ID:
            doctor = msg.from_user.full_name if msg.from_user else ''
            uname = f'@{msg.from_user.username}' if msg.from_user and msg.from_user.username else ''
            await bot.send_message(ADMIN_CHAT_ID, f"Лікар просить зв'язатися: {doctor} {uname} (chat_id: {msg.chat.id})")
    except Exception as e:
        logger.warning('Cannot ping admin: %s', e)
    await msg.answer("Передали повідомлення техніку. Він зв'яжеться з Вами найближчим часом.", reply_markup=main_kb())

@dp.message(F.text == '🧾 Зробити замовлення')
async def new_order(msg: Message):
    await msg.answer("⏳ Перевіряю профіль лікаря. Зачекайте, будь ласка.")
    await _clear_inline_markup(msg)
    await _silent_autostart_on_first_menu_click(msg)
    st = OrderState()
    st.order_id = gen_order_id()
    st.email = LAB_EMAIL
    state_by_chat[msg.chat.id] = st
    phone = doctor_phone_get(msg.chat.id)
    base_values = {'order_id': st.order_id, 'created_at': datetime.now().strftime('%Y-%m-%d %H:%M'), 'doctor_name': msg.from_user.full_name if msg.from_user else '', 'tg_username': f'@{msg.from_user.username}' if msg.from_user and msg.from_user.username else '', 'chat_id': str(msg.chat.id), 'phone': phone, 'status': 'new'}
    st.sheet_row = await _safe_append_row(base_values, msg)
    if not st.sheet_row:
        return
    if not phone:
        await msg.answer('Вкажіть, будь ласка, Ваш номер телефону для звʼязку:', reply_markup=bottom_nav_kb())
        st.step = 'doctor_phone'
    else:
        await msg.answer('Вкажіть, будь ласка, прізвище пацієнта:', reply_markup=bottom_nav_kb())
        st.step = 'patient_lastname'

async def ask_notes(msg: Message, st: OrderState):
    await msg.answer('Хочете додати текстові пояснення або голосове повідомлення?', reply_markup=notes_yesno_kb())
    st.step = 'await_notes_choice'

async def finalize_order(msg: Message, st: OrderState):
    if getattr(st, 'finalized', False):
        return
    st.finalized = True
    try:
        if getattr(st, 'sheet_row', 0):
            set_cell(st.sheet_row, 'status', 'order_submitted')
    except Exception:
        logger.exception('Failed to set final status')

    await msg.answer(build_summary_text(st), parse_mode='HTML', reply_markup=main_kb())
    state_by_chat[msg.chat.id] = OrderState()

@dp.message()
async def flow(msg: Message):
    # Додатковий захист: URL/посилання поза сценарієм — також у Головне меню
    if msg.content_type == 'text':
        txt = (msg.text or '').strip()
        # Просте визначення URL або 'www.'/'t.me'/домен.зона
        url_like = bool(re.search(r"(https?://|www\.|t\.me/|\w+\.(?:ua|com|net|org|io|app|gov|edu)(/|\b))", txt, re.IGNORECASE))
        if url_like:
            st = state_by_chat.get(msg.chat.id)
            # Посилання очікуємо лише на кроках await_links або email_wait_done
            if not (st and st.step in ('await_links', 'email_wait_done')):
                if st and getattr(st, 'sheet_row', None):
                    try:
                        set_cell(st.sheet_row, 'status', 'cancelled')
                    except Exception:
                        pass
                await msg.answer('Повертаємось у Головне меню. Спробуйте ще раз.', reply_markup=main_kb())
                state_by_chat[msg.chat.id] = OrderState()
                return
    # Жорсткий захист: будь-який неочікуваний НЕ-текст → Головне меню
    st = state_by_chat.get(msg.chat.id)
    if msg.content_type != 'text':
        expecting_file = st and st.step == 'await_tele_files' and msg.content_type in (ContentType.DOCUMENT, ContentType.PHOTO)
        expecting_voice = st and st.step == 'await_notes' and msg.content_type == ContentType.VOICE
        if not (expecting_file or expecting_voice):
            if st and getattr(st, 'sheet_row', None):
                try:
                    set_cell(st.sheet_row, 'status', 'cancelled')
                except Exception:
                    pass
            await msg.answer('Повертаємось у Головне меню. Спробуйте ще раз.', reply_markup=main_kb())
            state_by_chat[msg.chat.id] = OrderState()
            return
    st = state_by_chat.get(msg.chat.id)
    st = state_by_chat.get(msg.chat.id)
    await _clear_inline_markup(msg)
    if (msg.text or '').strip() in MAIN_BTNS:
        return
    st = state_by_chat.get(msg.chat.id)
    text = (msg.text or '').strip()
    if text == '⬅️ Назад':
        st.confirm_exit = False
        if st and st.step in ('await_tele_files', 'await_links', 'email_wait_done'):
            st.step = 'choose_files_method'
            await msg.answer('Повернулись до вибору способу передачі файлів:', reply_markup=files_method_kb())
            return
        if not st:
            await msg.answer('Готові прийняти замовлення. Натисніть «🧾 Зробити замовлення».', reply_markup=main_kb())
            return
        if st and st.delivery_step == 'saved_pick':
            has_saved = bool(np_profiles_list(msg.chat.id))
            st.step = 'np_menu'
            st.delivery_step = ''
            await _clear_inline_markup(msg)
            await msg.answer('Доставити замовлення Новою Поштою:', reply_markup=np_menu_kb(has_saved))
            return
        prev_step, hint = _prev_step(st)
        if prev_step in ('recv_name', 'recv_phone', 'city_text', 'await_np_number'):
            st.delivery_step = prev_step
            st.step = ''
            await msg.answer(hint, parse_mode='HTML', reply_markup=bottom_nav_kb())
        else:
            st.step = prev_step
            st.delivery_step = ''
            if prev_step == 'await_notes_choice':
                await msg.answer(hint, reply_markup=notes_yesno_kb())
            elif prev_step == 'np_menu':
                has_saved = bool(np_profiles_list(msg.chat.id))
                await _clear_inline_markup(msg)
                await msg.answer(hint, reply_markup=np_menu_kb(has_saved))
            elif prev_step == 'choose_files_method':
                await _clear_inline_markup(msg)
                await msg.answer(hint, reply_markup=files_method_kb())
            else:
                await msg.answer(hint, parse_mode='HTML', reply_markup=bottom_nav_kb())
        return
    if text == '🏠 Головне меню':
        if st:
            st.confirm_exit = True
        await msg.answer('Повернення у «Головне меню» скасує поточне замовлення. Справді вийти?', reply_markup=confirm_cancel_kb())
        return
    if st and getattr(st, 'confirm_exit', False):
        if text == 'Так, скасувати і в меню':
            if getattr(st, 'sheet_row', None):
                try:
                    set_cell(st.sheet_row, 'status', 'cancelled_by_user')
                except Exception:
                    pass
            state_by_chat[msg.chat.id] = OrderState()
            await msg.answer('Готові прийняти замовлення. Натисніть «🧾 Зробити замовлення».', reply_markup=main_kb())
            return
        if text == 'Ні, продовжити':
            st.confirm_exit = False
            if st.delivery_step in ('recv_name', 'recv_phone', 'city_text', 'await_np_number'):
                hints = {'recv_name': 'Вкажіть ПІБ отримувача:', 'recv_phone': 'Введіть телефон отримувача (380XXXXXXXXX):', 'city_text': 'Вкажіть місто (наприклад: Київ):', 'await_np_number': 'Введіть номер відділення або поштомату (наприклад: 15 або 2345).'}
                await msg.answer(hints[st.delivery_step], reply_markup=bottom_nav_kb())
            elif st.step in ('await_np_number','await_np_pick'):
                await _prompt_np_number(msg)
                return
            elif st.delivery_step == 'saved_pick':
                await _show_np_saved_list(msg)
                return
            elif st.step == 'email_wait_done':
                lastname = getattr(st, 'patient_lastname', '') or ''
                subject = f'AmbaLab order {nz(st.order_id)} - {lastname}' if lastname else f'AmbaLab order {nz(st.order_id)}'
                text = f'Скопіюйте електронну адресу і тему листа\n\n📧 <code>{LAB_EMAIL}</code>\n🧾 <code>{subject}</code>\n\nПісля відправлення листа натисніть «✅ Готово».'
                await msg.answer(text, parse_mode='HTML', reply_markup=files_aux_kb())
                return

            else:
                reprompt_map = {'doctor_phone': ('Вкажіть, будь ласка, <b>Ваш номер телефону</b> для звʼязку:', bottom_nav_kb()), 'patient_lastname': ('Вкажіть, будь ласка, прізвище пацієнта:', bottom_nav_kb()), 'work_type': ('Вкажіть, будь ласка, який апарат замовляєте (сплінт, елайнери тощо):', bottom_nav_kb()), 'due_date': ('Вкажіть дату здачі у форматі ДД.ММ або ДД.ММ.РРРР (наприклад 05.10):', bottom_nav_kb()), 'np_menu': ('Доставити замовлення Новою Поштою:', np_menu_kb(has_saved=bool(np_profiles_list(msg.chat.id)))), 'choose_files_method': ('Оберіть спосіб передачі файлів:', files_method_kb()), 'await_tele_files': ('📎 <b>Надішліть файли</b> (можна кілька)\n\nПісля <i>кожного</i> файла я відповім:\n«✅ Файл збережено».\n\nКоли надішлете <b>ВСІ</b> файли —\nнатисніть «✅ Готово».', files_aux_kb()), 'await_links': ('🔗 <b>Надішліть посилання</b> (можна кілька)\n\nНадсилайте по одному в повідомленні — я відповім:\n«✅ Посилання збережено».\n\nКоли відправите <b>ВСІ</b> посилання —\nнатисніть «✅ Готово».', files_aux_kb()), 'email_wait_done': ('Перевірте e-mail і тему повідомлення (скопіюйте й надішліть). Коли завершите — натисніть «✅ Готово».', done_kb()), 'await_notes_choice': ('Хочете додати текстові пояснення або голосове повідомлення?', notes_yesno_kb()), 'await_notes': ('💬 <b>Надішліть текстові або голосові повідомлення</b>\n\nПісля <i>кожного</i> я підтверджу:\n«✅ Повідомлення збережено».\n\nКоли надішлете <b>ВСІ</b> потрібні повідомлення —\nнатисніть «✅ Готово».', done_kb())}
                hint, kb = reprompt_map.get(st.step, ('Готові продовжити замовлення.', bottom_nav_kb()))
                resp = await msg.answer(hint, reply_markup=kb, parse_mode='HTML')
            st = state_by_chat.get(msg.chat.id)
            if st:
                st.last_inline_msg_id = resp.message_id
            return
    if text == '⬅️ Обрати інший спосіб':
        if st and st.step in ('await_tele_files', 'await_links', 'email_wait_done'):
            st.step = 'choose_files_method'
            await _clear_inline_markup(msg)
            await msg.answer('Оберіть спосіб передачі файлів:', reply_markup=files_method_kb())
            return
    allowed_done_states = {'await_tele_files', 'await_links', 'email_wait_done', 'await_notes'}
    if text == '✅ Готово' and (not st or st.step not in allowed_done_states):
        await _clear_inline_markup(msg)
        await msg.answer('Оберіть спосіб передачі файлів:', reply_markup=files_method_kb())
        if st:
            st.step = 'choose_files_method'
        return
    if not st:
        state_by_chat[msg.chat.id] = OrderState()
        return await msg.answer('Готові прийняти замовлення. Натисніть «🧾 Зробити замовлення».', reply_markup=main_kb())
    if st and st.step == 'await_notes_choice':
        choice = (msg.text or '').strip()
        if choice == 'Так':
            await msg.answer('💬 <b>Надішліть текстові або голосові повідомлення</b>\n\nПісля <i>кожного</i> я підтверджу:\n«✅ Повідомлення збережено».\n\nКоли надішлете <b>ВСІ</b> потрібні повідомлення —\nнатисніть «✅ Готово».', reply_markup=done_kb(), parse_mode='HTML')
            st.step = 'await_notes'
            return
        if choice == 'Ні':
            await finalize_order(msg, st)
            return
        await msg.answer('Будь ласка, виберіть «Так» або «Ні».', reply_markup=notes_yesno_kb())
        return
    if st.step in ('await_np_number', 'await_np_pick'):
        if msg.content_type != 'text' or not msg.text:
            await msg.answer('Вкажіть номер відділення/поштомату (тільки цифри).')
            return
        num, want_postomat = np_detect_kind(msg.text)
        if not num:
            await msg.answer('Вкажіть номер відділення/поштомату (тільки цифри).')
            return
        if not getattr(st, 'np_city_ref', ''):
            await msg.answer('Спочатку оберіть місто доставки.')
            return
        await msg.answer('🔎 Шукаю відділення/поштомат за номером…')
        import asyncio
        try:
            whs = await asyncio.wait_for(np_search_warehouses(st.np_city_ref, str(num)), timeout=10)
        except asyncio.TimeoutError:
            await msg.answer('⏳ Нова Пошта довго відповідає. Спробуйте ввести номер ще раз або пізніше.')
            return
        except Exception as e:
            logger.warning('NP getWarehouses error: %s', e)
            whs = []
        if want_postomat is not None:
            filtered = [w for w in whs if _np_is_postomat(w) == want_postomat]
            whs = filtered or whs
        exact = [w for w in whs if str(w.get('Number')) == str(num)]
        whs = exact or whs
        whs = [w for w in whs if w.get('CityRef') == st.np_city_ref] or whs
        if not whs:
            await msg.answer('Тимчасові складності зі звʼязком. Спробуйте пізніше.')
            return
        if len(whs) > 1:
            st.last_np_items = whs[:10]
            rows = []
            for w in st.last_np_items:
                kind = 'Поштомат' if _np_is_postomat(w) else 'Відділення'
                label = f"{kind} №{w.get('Number')}: {(w.get('ShortAddress') or w.get('Description'))[:64]}"
                rows.append([InlineKeyboardButton(text=label, callback_data=f"np_wh_pick:{w.get('Ref', '')}")])
            rows.append([InlineKeyboardButton(text='↩️ Ввести інший номер', callback_data='np_wh_back')])
            resp = resp = await msg.answer('Знайшлось кілька варіантів. Оберіть потрібний:', reply_markup=InlineKeyboardMarkup(inline_keyboard=rows))
            st = state_by_chat.get(msg.chat.id)
            if st:
                st.last_inline_msg_id = resp.message_id
            st = state_by_chat.get(msg.chat.id)
            None
            if st:
                st.last_inline_msg_id = resp.message_id
            st = state_by_chat.get(msg.chat.id)
            None
            if st:
                st.last_inline_msg_id = resp.message_id
            st.step = 'await_np_pick'
            return
        w = whs[0]
        desc = w.get('Description', f'№{num}')
        if not await _safe_set_cell(st.sheet_row, 'np_warehouse_desc', desc, msg): return
        if not await _safe_set_cell(st.sheet_row, 'np_warehouse_ref', w.get('Ref', ''), msg): return
        np_save_current_delivery(msg.chat.id, st)
        await msg.answer(f'✅ Адресу доставки збережено: <b>{desc}</b>', parse_mode='HTML')
        await _clear_inline_markup(msg)
        await msg.answer('Оберіть спосіб передачі файлів:', reply_markup=files_method_kb())
        st.step = 'choose_files_method'
        return
    if st.delivery_step:
        txt = (msg.text or '').strip()
        if st.delivery_step == 'recv_name':
            if not _text_only(txt, min_len=3):
                await msg.answer('Поле не може бути порожнім. Вкажіть ПІБ отримувача текстом.')
                return
            if not await _safe_set_cell(st.sheet_row, 'recipient_name', txt, msg): return
            await msg.answer('Введіть телефон отримувача (380XXXXXXXXX):', reply_markup=bottom_nav_kb())
            st.delivery_step = 'recv_phone'
            return
        if st.delivery_step == 'recv_phone':
            ph_digits = re.sub('\\D+', '', txt)
            if not re.fullmatch('380\\d{9}', ph_digits):
                await msg.answer('Будь ласка, введіть правильний номер телефону у форматі 380XXXXXXXXX (12 цифр).')
                return
            if not await _safe_set_cell(st.sheet_row, 'recipient_phone', ph_digits, msg): return
            await msg.answer('Вкажіть місто (наприклад: Київ):', reply_markup=bottom_nav_kb())
            st.delivery_step = 'city_text'
            return
        if st.delivery_step == 'city_text':
            if not await _safe_set_cell(st.sheet_row, 'np_city_name', txt, msg): return
            import asyncio
            try:
                matches = await asyncio.wait_for(np_search_cities(txt), timeout=10)
            except asyncio.TimeoutError:
                await msg.answer('⏳ Нова Пошта довго відповідає. Зачекайте, будь ласка.')
                return
            except Exception as e:
                logger.warning('NP getCities error: %s', e)
                matches = []
            if not matches:
                await msg.answer('Тимчасові складності зі звʼязком. Спробуйте пізніше.')
                return
            if len(matches) == 1:
                city = matches[0]
                st.np_city_ref = city.get('Ref', '')
                if not await _safe_set_cell(st.sheet_row, 'np_city_name', city.get('Description', ''), msg): return
                if not await _safe_set_cell(st.sheet_row, 'np_city_ref', st.np_city_ref, msg): return
                st.delivery_step = ''
                st.step = 'await_np_number'
                await msg.answer('Введіть номер відділення або поштомату (наприклад: 15 або 2345).', reply_markup=bottom_nav_kb())
                return
            st.last_np_cities = matches[:20]
            rows = []
            for c in matches[:20]:
                label = c.get('Description', '')
                area = c.get('AreaDescription', '')
                text_btn = f'{label} ({area})' if area else label
                rows.append([InlineKeyboardButton(text=text_btn[:64], callback_data=f"np_city_pick:{c.get('Ref')}")])
            resp = await msg.answer('Знайдено кілька міст. Оберіть потрібне:', reply_markup=InlineKeyboardMarkup(inline_keyboard=rows))
            st = state_by_chat.get(msg.chat.id)
            if st:
                st.last_inline_msg_id = resp.message_id
            st = state_by_chat.get(msg.chat.id)
            if st:
                st.current_step = 'np_city_pick'
            st = state_by_chat.get(msg.chat.id)
            None
            if st:
                st.last_inline_msg_id = resp.message_id
            st = state_by_chat.get(msg.chat.id)
            None
            if st:
                st.last_inline_msg_id = resp.message_id
            return
        return
    if st.step == 'doctor_phone':
        raw = (msg.text or '').strip()
        txt_norm = re.sub(r'[ \-\(\)]', '', raw)
        if not re.fullmatch(r'^\+?[1-9]\d{11,14}$', txt_norm):
            await msg.answer('Вкажіть номер телефону у міжнародному форматі (наприклад, +380XXXXXXXXX).')
            return
        if not await _safe_set_cell(st.sheet_row, 'phone', txt_norm, msg): return
        try:
            doctor_phone_create(msg.chat.id, txt_norm)
        except Exception:
            pass
        await msg.answer('Вкажіть, будь ласка, прізвище пацієнта:', reply_markup=bottom_nav_kb())
        st.step = 'patient_lastname'
        return
    if st.step == 'patient_lastname':
        val = (msg.text or '').strip()
        if not _text_only(val, min_len=2):
            await msg.answer('Поле не може бути порожнім. Вкажіть прізвище пацієнта текстом.')
            return
        st.patient_lastname = val
        if not await _safe_set_cell(st.sheet_row, 'patient_lastname', st.patient_lastname, msg): return
        await msg.answer('Який апарат замовляєте (сплінт, елайнери тощо):', reply_markup=bottom_nav_kb())
        st.step = 'work_type'
        return
    if st.step == 'work_type':
        val = (msg.text or '').strip()
        if not _text_only(val, min_len=2):
            await msg.answer('Поле не може бути порожнім. Вкажіть апарат текстом.')
            return
        st.work_type = val
        if not await _safe_set_cell(st.sheet_row, 'work_type', st.work_type, msg): return
        await msg.answer('Вкажіть дату здачі у форматі ДД.ММ або ДД.ММ.РРРР (наприклад 05.10):', reply_markup=bottom_nav_kb())
        st.step = 'due_date'
        return
    if st.step == 'due_date':
        iso = parse_date_uk(msg.text or '')
        if not iso:
            await msg.answer('Не можу розпізнати дату. Приклад: 05.10 або 10.10.2025.')
            return
        st.due_date_iso = iso
        if not await _safe_set_cell(st.sheet_row, 'due_date', st.due_date_iso, msg): return
        if not getattr(st, 'admin_notified', False):
            await notify_admin_new_order(msg, st)
            st.admin_notified = True
        st.step = ''
        profiles = np_profiles_list(msg.chat.id)
        await _clear_inline_markup(msg)
        await msg.answer('Доставити замовлення Новою Поштою:', reply_markup=np_menu_kb(has_saved=bool(profiles)))
        st.step = 'np_menu'
        return
    if st and st.step == 'np_menu':
        t = (msg.text or '').strip()
        if t == NP_MENU_ADD:
            st.delivery_step = 'recv_name'
            await msg.answer('Вкажіть ПІБ отримувача:', reply_markup=bottom_nav_kb())
            return
        if t == NP_MENU_USE_SAVED:
            profiles = np_profiles_list(msg.chat.id)
            if not profiles:
                await msg.answer('Збережені адреси відсутні. Заповніть доставку.', reply_markup=bottom_nav_kb())
                st.delivery_step = 'recv_name'
                await msg.answer('Вкажіть ПІБ отримувача:', reply_markup=bottom_nav_kb())
                return
            if len(profiles) == 1:
                p = profiles[0]
                phone_val = normalize_ua_phone(p.get('recipient_phone', '')) or p.get('recipient_phone', '')
                set_cell(st.sheet_row, 'recipient_name', p.get('recipient_name', ''))
                set_cell(st.sheet_row, 'recipient_phone', phone_val)
                set_cell(st.sheet_row, 'np_city_name', p.get('np_city_name', ''))
                set_cell(st.sheet_row, 'np_warehouse_desc', p.get('np_warehouse_desc', ''))
                if p.get('np_city_ref'):
                    set_cell(st.sheet_row, 'np_city_ref', p['np_city_ref'])
                    st.np_city_ref = p['np_city_ref']
                if p.get('np_warehouse_ref'):
                    set_cell(st.sheet_row, 'np_warehouse_ref', p['np_warehouse_ref'])
                try:
                    np_profile_upsert(msg.chat.id, {'recipient_name': get_cell(st.sheet_row, 'recipient_name'), 'recipient_phone': normalize_ua_phone(get_cell(st.sheet_row, 'recipient_phone')) or get_cell(st.sheet_row, 'recipient_phone'), 'np_city_name': get_cell(st.sheet_row, 'np_city_name'), 'np_city_ref': get_cell(st.sheet_row, 'np_city_ref'), 'np_warehouse_desc': get_cell(st.sheet_row, 'np_warehouse_desc'), 'np_warehouse_ref': get_cell(st.sheet_row, 'np_warehouse_ref')})
                except Exception:
                    pass
                await msg.answer('Дані доставки підставлено.')
                await _clear_inline_markup(msg)
                await _clear_inline_markup(msg)
                await msg.answer('Оберіть спосіб передачі файлів:', reply_markup=files_method_kb())
                st.delivery_step = ''
                st.step = 'choose_files_method'
                return
            rows = []
            for pr in profiles[:20]:
                full_name = (pr.get('recipient_name') or '').strip()
                parts = full_name.split()
                if len(parts) == 3:
                    short_name = f'{parts[0]} {parts[1][0]}.{parts[2][0]}.'
                elif len(parts) == 2:
                    short_name = f'{parts[0]} {parts[1][0]}.'
                elif len(parts) == 1:
                    short_name = parts[0]
                else:
                    short_name = full_name
                text_btn = f"{short_name} • {pr.get('np_city_name', '')} • {pr.get('np_warehouse_desc', '')}"[:64]
                rows.append([InlineKeyboardButton(text=text_btn, callback_data=f"np_pick:{pr.get('_row', '0')}")])
            resp = resp = await msg.answer('Оберіть збережену адресу:', reply_markup=InlineKeyboardMarkup(inline_keyboard=rows))
            st = state_by_chat.get(msg.chat.id)
            if st:
                st.last_inline_msg_id = resp.message_id
            st.delivery_step = 'saved_pick'
            st.step = ''
            st = state_by_chat.get(msg.chat.id)
            if st:
                st.prev_step = 'np_menu'
                st.current_step = 'np_saved_list'
            return
        if t == NP_MENU_SKIP:
            await _clear_inline_markup(msg)
            np_cols = ('recipient_name', 'recipient_phone', 'np_city_name', 'np_warehouse_desc', 'np_city_ref', 'np_warehouse_ref')
            if any(((get_cell(st.sheet_row, c) or '').strip() for c in np_cols)):
                for c in np_cols:
                    set_cell(st.sheet_row, c, '')
                st.np_city_ref = ''
                st.np_warehouse_ref = ''
            st.delivery_step = ''
            await _clear_inline_markup(msg)
            await _clear_inline_markup(msg)
            await msg.answer('Оберіть спосіб передачі файлів:', reply_markup=files_method_kb())
            st.step = 'choose_files_method'
            return
    if st.step == 'choose_files_method':
        t = msg.text or ''
        if 'Завантажити у бот' in t:
            append_files_method(st.sheet_row, 'telegram_upload')
            await msg.answer('📎 <b>Надішліть файли</b> (можна кілька)\n\nПісля <i>кожного</i> файла я відповім:\n«✅ Файл збережено».\n\nКоли надішлете <b>ВСІ</b> файли —\nнатисніть «✅ Готово».', reply_markup=files_aux_kb(), parse_mode='HTML')
            st.step = 'await_tele_files'
            return
        if 'Надати посилання' in t:
            append_files_method(st.sheet_row, 'link')
            await msg.answer('🔗 <b>Надішліть посилання</b> (можна кілька)\n\nНадсилайте по одному в повідомленні — я відповім:\n«✅ Посилання збережено».\n\nКоли відправите <b>ВСІ</b> посилання —\nнатисніть «✅ Готово».', reply_markup=files_aux_kb(), parse_mode='HTML')
            st.step = 'await_links'
            return
        if 'e-mail' in t.lower() or 'email' in t.lower():
            append_files_method(st.sheet_row, 'email')
            if LAB_EMAIL:
                set_cell(st.sheet_row, 'email', LAB_EMAIL)
            lastname = getattr(st, 'patient_lastname', '') or ''
            subject = f'AmbaLab order {nz(st.order_id)} - {lastname}' if lastname else f'AmbaLab order {nz(st.order_id)}'
            text = f'Скопіюйте електронну адресу і тему листа\n\n📧 <code>{LAB_EMAIL}</code>\n\n🧾 <code>{subject}</code>\n\nПісля відправлення листа натисніть «✅ Готово».'
            await msg.answer(text, parse_mode='HTML', reply_markup=files_aux_kb())
            st.step = 'email_wait_done'
            return
        await _clear_inline_markup(msg)
        await _clear_inline_markup(msg)
        await msg.answer('Оберіть спосіб передачі файлів:', reply_markup=files_method_kb())
        return
        return
    if st.step == 'await_links':
        if (msg.text or '').strip() == '✅ Готово':
            if not (get_cell(st.sheet_row, 'links_external') or '').strip():
                await msg.answer('Поки що посилань не додано. Надішліть хоча б одне або оберіть інший спосіб.', reply_markup=files_aux_kb())
                return
            set_cell(st.sheet_row, 'status', 'files_expected')
            await ask_notes(msg, st)
            return
        urls = extract_urls(msg.text or '')
        if not urls:
            await msg.answer('Не бачу посилань. Надішліть URL, потім натисніть ✅ Готово.', reply_markup=files_aux_kb())
            return
        update_joined(st.sheet_row, 'links_external', urls)
        set_cell(st.sheet_row, 'status', 'files_received')
        await msg.answer('✅ Посилання збережено.', reply_markup=files_aux_kb())
        return
    if st.step == 'await_tele_files':
        if (msg.text or '').strip() == '✅ Готово':
            if not (get_cell(st.sheet_row, 'files_telegram_id') or '').strip():
                await msg.answer('Поки що файлів не додано. Надішліть хоча б один або оберіть інший спосіб.', reply_markup=files_aux_kb())
                return
            set_cell(st.sheet_row, 'status', 'files_expected')
            await ask_notes(msg, st)
            return
    if st.step == 'email_wait_done':
        if (msg.text or '').strip() == '✅ Готово':
            set_cell(st.sheet_row, 'status', 'files_expected')
            set_cell(st.sheet_row, 'email_sent', 'Yes')
            set_cell(st.sheet_row, 'status', 'files_expected')
            await ask_notes(msg, st)
            return
    if st and st.step == 'await_tele_files' and msg.content_type in (ContentType.DOCUMENT, ContentType.PHOTO):
        if not FILES_CHANNEL_ID:
            await msg.answer('⚠️ FILES_CHANNEL_ID не налаштований у .env')
            return
        caption = f"ID замовлення: {nz(st.order_id)}\nПацієнт: {st.patient_lastname or ''}"
        try:
            if msg.content_type == ContentType.DOCUMENT:
                if msg.document.file_size and msg.document.file_size > 2 * 1024 * 1024 * 1024:
                    await msg.answer('❌ Файл більший за 2 ГБ. Оберіть інший спосіб.')
                    return
                file_id = msg.document.file_id
                await bot.send_document(FILES_CHANNEL_ID, file_id, caption=caption)
            else:
                file_id = msg.photo[-1].file_id
                await bot.send_photo(FILES_CHANNEL_ID, file_id, caption=caption)
            prev = get_cell(st.sheet_row, 'files_telegram_id')
            set_cell(st.sheet_row, 'files_telegram_id', (prev + ' ' if prev else '') + file_id)
            set_cell(st.sheet_row, 'status', 'files_received')
            await msg.answer('✅ Ваші файли отримані і збережені', reply_markup=files_aux_kb())
        except Exception as e:
            logger.exception('Send to channel failed')
            await msg.answer("Не можу зберегти файл. Тимчасові складності зі зв'язком. Спробуйте пізніше.", reply_markup=files_aux_kb())
        return
    if st.step == 'await_notes':
        if (msg.text or '').strip() == '✅ Готово':
            return await finalize_order(msg, st)
        if st and st.step == 'await_notes' and msg.content_type == ContentType.VOICE:
            file_id_tg = msg.voice.file_id
            prev = get_cell(st.sheet_row, 'voice_id')
            set_cell(st.sheet_row, 'voice_id', (prev + ' ' if prev else '') + file_id_tg)
            try:
                file = await bot.get_file(file_id_tg)
                buf = await bot.download_file(file.file_path)
                
                # універсальне добування байтів: працює і з потоками, і з bytes
                if hasattr(buf, 'read'):
                    data = buf.read()
                elif isinstance(buf, (bytes, bytearray)):
                    data = bytes(buf)
                else:
                    import io
                    bio = io.BytesIO()
                    await bot.download_file(file.file_path, destination=bio)
                    bio.seek(0)
                    data = bio.getvalue()
                
                fname = f"voice_{nz(st.order_id)}_{datetime.now().strftime('%H%M%S')}.ogg"
                _, vlink = await upload_to_drive(st, fname, data, 'audio/ogg')
                prev_link = get_cell(st.sheet_row, 'voice_link')
                set_cell(st.sheet_row, 'voice_link', (prev_link + ' ' if prev_link else '') + vlink)
                await msg.answer('✅ Голосове повідомлення збережено.', reply_markup=done_kb())
            except Exception as e:
                logger.exception('Voice upload error')
                await msg.answer("Не можу зберегти файл. Тимчасові складності зі зв'язком. Спробуйте пізніше.", reply_markup=done_kb())
            return
        if msg.text:
            prev = get_cell(st.sheet_row, 'notes')
            set_cell(st.sheet_row, 'notes', (prev + '\n' if prev else '') + (msg.text or ''))
            await msg.answer('✅ Текстове повідомлення збережено.', reply_markup=done_kb())
            return
        await msg.answer('Надішліть текст або голосове, або натисніть «✅ Готово».', reply_markup=done_kb())
        return

    # Якщо жодна з умов не спрацювала — користувач відійшов від сценарію
    st = state_by_chat.get(msg.chat.id)
    if st is not None:
        handled = await _warn_or_reset_to_menu(msg, st)
        if handled:
            return
    return
def _np_is_postomat(w: dict) -> bool:
    return w.get('TypeOfWarehouseRef') == NP_POSTOMAT_REF

def np_detect_kind(s: str):
    """
    Правило: беремо лише цифри з введення.
    - Якщо цифр немає -> (None, None) і просимо ввести номер.
    - Якщо довжина цифр ≤ 3 -> це відділення (want_postomat=False).
    - Якщо довжина цифр > 3 -> це поштомат (want_postomat=True).
    """
    import re
    s = (s or '').strip()
    digits = ''.join(re.findall('\\d+', s))
    if not digits:
        return (None, None)
    want_postomat = len(digits) > 3
    return (digits, want_postomat)
    return

@dp.callback_query(F.data == 'notes_yes')
async def notes_yes_cb(q: CallbackQuery):
    st = state_by_chat.get(q.message.chat.id)
    await msg.answer('💬 <b>Надішліть текстові або голосові повідомлення</b>\n\nПісля <i>кожного</i> я підтверджу:\n«✅ Повідомлення збережено».\n\nКоли надішлете <b>ВСІ</b> потрібні повідомлення —\nнатисніть «✅ Готово».', reply_markup=done_kb(), parse_mode='HTML')
    st.step = 'await_notes'
    await q.answer()

@dp.callback_query(F.data == 'notes_no')
async def notes_no_cb(q: CallbackQuery):
    st = state_by_chat.get(q.message.chat.id)
    await finalize_order(q.message, st)
    await q.answer()

async def np_start_cb(q: CallbackQuery):
    """Старт майстра нової адреси: ПІБ → телефон → місто → номер відділення/поштомату."""
    st = state_by_chat.get(q.message.chat.id)
    if not st:
        await q.answer()
        return
    st.delivery_step = 'recv_name'
    await q.message.answer('Вкажіть ПІБ отримувача:')
    await q.answer()

async def np_skip_cb(q: CallbackQuery):
    st = state_by_chat.get(q.message.chat.id)
    if st:
        st.delivery_step = ''
    await q.message.answer('Оберіть спосіб передачі файлів:', reply_markup=files_method_kb())
    st.step = 'choose_files_method'
    await q.answer()

@dp.callback_query(F.data == 'np_use_saved')
async def np_use_saved_cb(q: CallbackQuery):
    st = state_by_chat.get(q.message.chat.id)
    profiles = np_profiles_list(q.message.chat.id)
    if not profiles:
        await q.message.answer('Збережені адреси відсутні. Заповніть доставку.')
        st.delivery_step = 'recv_name'
        await q.message.answer('Вкажіть ПІБ отримувача:')
        return await q.answer()
    if len(profiles) == 1:
        p = profiles[0]
        phone_val = normalize_ua_phone(p.get('recipient_phone', '')) or p.get('recipient_phone', '')
        set_cell(st.sheet_row, 'recipient_name', p.get('recipient_name', ''))
        set_cell(st.sheet_row, 'recipient_phone', phone_val)
        set_cell(st.sheet_row, 'np_city_name', p.get('np_city_name', ''))
        set_cell(st.sheet_row, 'np_warehouse_desc', p.get('np_warehouse_desc', ''))
        if p.get('np_city_ref'):
            set_cell(st.sheet_row, 'np_city_ref', p['np_city_ref'])
            st.np_city_ref = p['np_city_ref']
        if p.get('np_warehouse_ref'):
            set_cell(st.sheet_row, 'np_warehouse_ref', p['np_warehouse_ref'])
        try:
            np_profile_upsert(q.message.chat.id, {'recipient_name': get_cell(st.sheet_row, 'recipient_name'), 'recipient_phone': normalize_ua_phone(get_cell(st.sheet_row, 'recipient_phone')) or get_cell(st.sheet_row, 'recipient_phone'), 'np_city_name': get_cell(st.sheet_row, 'np_city_name'), 'np_city_ref': get_cell(st.sheet_row, 'np_city_ref'), 'np_warehouse_desc': get_cell(st.sheet_row, 'np_warehouse_desc'), 'np_warehouse_ref': get_cell(st.sheet_row, 'np_warehouse_ref')})
        except Exception:
            pass
        await q.message.answer('Дані доставки підставлено.')
        await q.message.answer('Оберіть спосіб передачі файлів:', reply_markup=files_method_kb())
        st.delivery_step = ''
        st.step = 'choose_files_method'
        return await q.answer()
    rows = []
    for pr in profiles[:20]:
        full_name = (pr.get('recipient_name') or '').strip()
        parts = full_name.split()
        short_name = ''
        if len(parts) == 3:
            short_name = f'{parts[0]} {parts[1][0]}.{parts[2][0]}.'
        elif len(parts) == 2:
            short_name = f'{parts[0]} {parts[1][0]}.'
        elif len(parts) == 1:
            short_name = parts[0]
        else:
            short_name = full_name
        text = f"{short_name} • {pr.get('np_city_name', '')} • {pr.get('np_warehouse_desc', '')}"[:64]
        rows.append([InlineKeyboardButton(text=text, callback_data=f"np_pick:{pr['_row']}")])
    rows += [[InlineKeyboardButton(text='✏️ Заповнити нову адресу', callback_data='np_start')], [InlineKeyboardButton(text='⏭️ Пропустити', callback_data='np_skip')]]
    resp = await q.message.answer('Оберіть збережену адресу:', reply_markup=InlineKeyboardMarkup(inline_keyboard=rows))
    if st:
        st.delivery_step = 'saved_pick'
        st.step = ''
        st.last_inline_msg_id = resp.message_id
    return await q.answer()

@dp.callback_query(F.data.startswith('np_pick:'))
async def np_pick_cb(q: CallbackQuery):
    st = state_by_chat.get(q.message.chat.id)
    try:
        row = int(q.data.split(':', 1)[1])
    except Exception:
        return await q.answer()
    ws2 = np_profiles_ws()
    head = np_head(ws2)
    row_vals = []
    for i in range(3):
        try:
            row_vals = ws2.row_values(row)
            break
        except Exception as e:
            if 'Quota exceeded' in str(e) or '429' in str(e):
                time.sleep(1 * 2 ** i)
                continue
            raise

    def v(k: str) -> str:
        c = head.get(k)
        return row_vals[c - 1] if c and c - 1 < len(row_vals) else ''
    phone_val = normalize_ua_phone(v('recipient_phone')) or v('recipient_phone')
    set_cell(st.sheet_row, 'recipient_name', v('recipient_name'))
    set_cell(st.sheet_row, 'recipient_phone', phone_val)
    set_cell(st.sheet_row, 'np_city_name', v('np_city_name'))
    set_cell(st.sheet_row, 'np_warehouse_desc', v('np_warehouse_desc'))
    if head.get('np_city_ref'):
        set_cell(st.sheet_row, 'np_city_ref', v('np_city_ref'))
        st.np_city_ref = v('np_city_ref')
    if head.get('np_warehouse_ref'):
        set_cell(st.sheet_row, 'np_warehouse_ref', v('np_warehouse_ref'))
    await q.message.answer('Дані доставки підставлено.')
    await q.message.answer('Оберіть спосіб передачі файлів:', reply_markup=files_method_kb())
    st.delivery_step = ''
    st.step = 'choose_files_method'
    await q.answer()

@dp.callback_query(F.data.startswith('np_city_pick:'))
async def np_city_pick_cb(q: CallbackQuery):
    st = state_by_chat.get(q.message.chat.id)
    try:
        ref = q.data.split(':', 1)[1]
    except Exception:
        await q.answer()
        return
    st.np_city_ref = ref
    set_cell(st.sheet_row, 'np_city_ref', ref)
    city_name = ''
    for c in getattr(st, 'last_np_cities', []) or []:
        if c.get('Ref') == ref:
            city_name = c.get('Description', '') or c.get('DescriptionRu', '')
            break
    if city_name:
        set_cell(st.sheet_row, 'np_city_name', city_name)
    st.delivery_step = ''
    st.step = 'await_np_number'
    await q.message.answer('Введіть номер відділення або поштомату (наприклад: 15 або 2345).')
    await q.answer()

@dp.callback_query(F.data.startswith('np_wh_pick:'))
async def np_wh_pick_cb(q: CallbackQuery):
    st = state_by_chat.get(q.message.chat.id)
    ref = q.data.split(':', 1)[1]
    picked = None
    for w in getattr(st, 'last_np_items', []) or []:
        if w.get('Ref') == ref:
            picked = w
            break
    set_cell(st.sheet_row, 'np_warehouse_ref', ref)
    np_save_current_delivery(q.message.chat.id, st)
    if picked:
        set_cell(st.sheet_row, 'np_warehouse_desc', picked.get('Description', ''))
        if picked.get('CityDescription'):
            set_cell(st.sheet_row, 'np_city_name', picked.get('CityDescription'))
    st.delivery_step = ''
    st.step = 'choose_files_method'
    await q.message.answer('Адресу доставки збережено.')
    await q.message.answer('Оберіть спосіб передачі файлів:', reply_markup=files_method_kb())
    await q.answer()

async def np_wh_back_cb(q: CallbackQuery):
    st = state_by_chat.get(q.message.chat.id)
    st.step = 'await_np_number'
    await q.message.answer('Введіть інший номер відділення або поштомату:')
    await q.answer()

@dp.callback_query(F.data == 'files_methods_back')
async def files_methods_back_cb(q: CallbackQuery):
    st = state_by_chat.get(q.message.chat.id)
    if not st:
        try:
            await q.answer(cache_time=1)
        except Exception:
            pass
        return
    await q.message.answer('Оберіть спосіб передачі файлів:', reply_markup=files_method_kb())
    st.step = 'choose_files_method'
    try:
        await q.answer(cache_time=1)
    except Exception:
        pass

@dp.callback_query(F.data == 'email_copy')
async def email_copy_cb(q: CallbackQuery):
    st = state_by_chat.get(q.message.chat.id)
    lastname = getattr(st, 'patient_lastname', '') or ''
    subj = f'AmbaLab order {nz(st.order_id)} - {lastname}' if st and lastname else f'AmbaLab order {nz(st.order_id)}' if st else 'AmbaLab order'
    await q.message.answer(f'Скопіюйте адресу та тему:\nEmail: <code>{LAB_EMAIL}</code>\nТема: <code>{subj}</code>', parse_mode='HTML')
    await q.answer('Скопіюйте з повідомлення нижче')

@dp.callback_query(F.data == 'email_done')
async def email_done_cb(q: CallbackQuery):
    st = state_by_chat.get(q.message.chat.id)
    set_cell(st.sheet_row, 'status', 'files_expected')
    set_cell(st.sheet_row, 'email_sent', 'Yes')
    await ask_notes(q.message, st)
    st.step = 'await_notes'
    await q.answer()

# ==== Helpers added: _prompt_np_number and _show_np_saved_list ====
async def _prompt_np_number(msg):
    """Повторити підказку для введення номера відділення/поштомату."""
    await msg.answer(
        'Введіть номер відділення або поштомату (наприклад: 15 або 2345).',
        reply_markup=bottom_nav_kb()
    )


async def _show_np_saved_list(msg):
    """Показати список збережених адрес НП з inline-кнопками."""
    st = state_by_chat.get(msg.chat.id)
    profiles = np_profiles_list(msg.chat.id) if 'np_profiles_list' in globals() else []
    if not profiles:
        await msg.answer('Збережених адрес немає.', reply_markup=np_menu_kb(False))
        if st:
            st.delivery_step = ''
        return

    rows = []
    for pr in profiles[:30]:
        text_btn = pr.get('np_warehouse_desc') or pr.get('np_city_name') or 'Адреса'
        rows.append([
            InlineKeyboardButton(
                text=str(text_btn)[:64],
                callback_data=f"np_pick:{pr.get('_row', '0')}"
            )
        ])
    markup = InlineKeyboardMarkup(inline_keyboard=rows)
    sent = await msg.answer('Оберіть збережену адресу:', reply_markup=markup)

    if st:
        st.last_inline_msg_id = getattr(sent, "message_id", None)
        st.delivery_step = 'saved_pick'
        st.prev_step = 'np_menu'
        st.current_step = 'np_saved_list'

# ====== Auto-cancel unfinished orders at midnight (Europe/Kyiv) ======
FINAL_STATUSES = {'order_submitted', 'cancelled', 'cancelled_by_user', 'auto_cancelled'}

def _is_unfinished(status: str) -> bool:
    return (status or '').strip() not in FINAL_STATUSES

def _parse_created_at(s: str):
    """
    Очікуємо формат 'YYYY-MM-DD HH:MM' у локальному часі.
    Повертаємо aware-дату в Europe/Kyiv або None.
    """
    try:
        dt = datetime.strptime((s or '').strip(), '%Y-%m-%d %H:%M')
        return dt.replace(tzinfo=ZoneInfo('Europe/Kyiv'))
    except Exception:
        return None

async def _cancel_yesterdays_unfinished_orders():
    """
    На північ перевіряємо всі замовлення за попередню добу і
    тільки незавершеним ставимо status='auto_cancelled',
    сповіщаємо лікаря і повертаємо Головне меню.
    """
    tz = ZoneInfo('Europe/Kyiv')
    now = datetime.now(tz)
    yesterday = (now.date() - timedelta(days=1))

    # читаємо всю таблицю разом
    all_vals = ws.get_all_values()
    if not all_vals:
        return

    head = headers_map(ws)
    idx_created = head.get('created_at')
    idx_status  = head.get('status')
    idx_chat    = head.get('chat_id')
    idx_order   = head.get('order_id')

    if not (idx_created and idx_status and idx_chat):
        logger.warning('Auto-cancel: missing required columns (need created_at, status, chat_id).')
        return

    for rownum in range(2, len(all_vals) + 1):
        try:
            created_raw = ws.cell(rownum, idx_created).value or ''
            status_raw  = ws.cell(rownum, idx_status).value or ''
            chat_raw    = ws.cell(rownum, idx_chat).value or ''
            order_id    = ws.cell(rownum, idx_order).value if idx_order else ''

            dt = _parse_created_at(created_raw)
            if not dt or dt.date() != yesterday:
                continue  # беремо тільки вчорашні

            if not _is_unfinished(status_raw):
                continue  # завершені/скасовані/auto_cancelled не чіпаємо

            # 1) проставити статус
            try:
                set_cell(rownum, 'status', 'auto_cancelled')
            except Exception:
                logger.exception('Auto-cancel: failed to write status for row %s', rownum)

            # 2) повідомити лікаря і вивести Головне меню
            chat_id = None
            try:
                chat_id = int(str(chat_raw).strip())
            except Exception:
                pass

            if chat_id:
                try:
                    await bot.send_message(
                        chat_id,
                        "Ваше незавершене замовлення скасовано. Щоб розпочати нове, натисніть «Зробити замовлення».",
                        reply_markup=main_kb()
                    )
                except Exception:
                    logger.exception('Auto-cancel: failed to notify chat_id=%s', chat_id)

                # 3) скинути локальний стан
                try:
                    state_by_chat[chat_id] = OrderState()
                except Exception:
                    pass

            logger.info('Auto-cancelled unfinished order %s (row %s)', order_id, rownum)

        except Exception:
            logger.exception('Auto-cancel: loop error at row %s', rownum)

async def _midnight_scheduler():
    """
    Безкінечний планувальник:
    засинає до найближчої 00:00 (Europe/Kyiv), потім запускає перевірку.
    """
    tz = ZoneInfo('Europe/Kyiv')
    while True:
        now = datetime.now(tz)
        nxt = (now + timedelta(days=1)).replace(hour=0, minute=0, second=0, microsecond=0)
        await asyncio.sleep((nxt - now).total_seconds())
        try:
            await _cancel_yesterdays_unfinished_orders()
        except Exception:
            logger.exception('Auto-cancel: midnight run failed')
        await asyncio.sleep(1)  # антидубль на випадок мікросекундних зсувів
# ====== /Auto-cancel ======
        
# ==== End helpers ====

async def main():
    logger.info('Starting AmbaLab Bot...')
    asyncio.create_task(_midnight_scheduler())
    await dp.start_polling(bot)

# --- Cloud Run entrypoint: HTTP server + aiogram polling ---
import os
import asyncio
from aiohttp import web

async def _health(request):
    return web.Response(text="ok")

async def _main():
    # 1) HTTP-сервер для healthcheck-ів Cloud Run
    app = web.Application()
    app.add_routes([web.get("/", _health), web.get("/healthz", _health)])
    runner = web.AppRunner(app)
    await runner.setup()
    site = web.TCPSite(runner, "0.0.0.0", int(os.environ.get("PORT", "8080")))
    await site.start()
    asyncio.create_task(_midnight_scheduler())

    # 2) Запускаємо бота в режимі polling
    await dp.start_polling(bot, allowed_updates=dp.resolve_used_update_types())

if __name__ == "__main__":
    asyncio.run(_main())
