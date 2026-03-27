"""
Стол заказов — Скрипт ежедневной сверки.

Запускается через GitHub Actions (cron) раз в день в 09:00 МСК.
1. Скачивает CSV-фид наличия
2. Читает из Google Sheets заявки со статусом «Ждёт деталь»
3. Сверяет category + part_name с фидом (available=true), исключая excluded_ids
4. Совпадения → статус «Найдено — связаться» + matched_at
5. Просроченные → статус «Просрочено»
6. Отправляет сводку в Telegram
"""

import os
import csv
import io
import json
import urllib.request
import urllib.parse
from datetime import datetime, date

# ─── Настройки из переменных окружения (GitHub Secrets) ───

FEED_URL = os.environ.get("FEED_URL", "https://baz-on.ru/export/c1326/2b944/razborangar-drom.csv")
TELEGRAM_BOT_TOKEN = os.environ["TELEGRAM_BOT_TOKEN"]
TELEGRAM_CHAT_ID = os.environ["TELEGRAM_CHAT_ID"]
GOOGLE_SHEETS_ID = os.environ["GOOGLE_SHEETS_ID"]
GOOGLE_SERVICE_ACCOUNT_JSON = os.environ["GOOGLE_SERVICE_ACCOUNT_JSON"]

# ─── Колонки таблицы (0-indexed) ───

COL = {
    "id": 0,
    "created_at": 1,
    "client_name": 2,
    "contact_type": 3,
    "contact_nick": 4,
    "contact_phone": 5,
    "category": 6,
    "year": 7,
    "generation": 8,
    "restyling": 9,
    "vin": 10,
    "part_name": 11,
    "position": 12,
    "quantity": 13,
    "catalog_number": 14,
    "comment": 15,
    "wait_until": 16,
    "status": 17,
    "matched_at": 18,
    "excluded_ids": 19,
}

DATA_START_ROW = 4  # 1=заголовки, 2=описания, 3=пример (1-indexed в Sheets API)


# ═══════════════════════════════════════════
# Google Sheets API (через сервисный аккаунт)
# ═══════════════════════════════════════════

import google.auth
from google.oauth2 import service_account
from googleapiclient.discovery import build

def get_sheets_service():
    """Создаёт клиент Google Sheets API из сервисного аккаунта."""
    creds_info = json.loads(GOOGLE_SERVICE_ACCOUNT_JSON)
    creds = service_account.Credentials.from_service_account_info(
        creds_info,
        scopes=["https://www.googleapis.com/auth/spreadsheets"]
    )
    return build("sheets", "v4", credentials=creds)


def read_orders(service):
    """Читает все строки из листа «Заявки» начиная с DATA_START_ROW."""
    result = service.spreadsheets().values().get(
        spreadsheetId=GOOGLE_SHEETS_ID,
        range=f"Заявки!A{DATA_START_ROW}:T",
    ).execute()
    return result.get("values", [])


def update_cell(service, row_idx, col_idx, value):
    """Обновляет одну ячейку. row_idx — 0-indexed от DATA_START_ROW."""
    # Конвертируем в A1-нотацию
    col_letter = chr(ord("A") + col_idx)
    cell = f"Заявки!{col_letter}{DATA_START_ROW + row_idx}"
    service.spreadsheets().values().update(
        spreadsheetId=GOOGLE_SHEETS_ID,
        range=cell,
        valueInputOption="RAW",
        body={"values": [[value]]},
    ).execute()


def batch_update_cells(service, updates):
    """Пакетное обновление ячеек. updates = [{"row": 0, "col": 0, "value": "..."}]"""
    data = []
    for u in updates:
        col_letter = chr(ord("A") + u["col"])
        cell = f"Заявки!{col_letter}{DATA_START_ROW + u['row']}"
        data.append({"range": cell, "values": [[u["value"]]]})

    if not data:
        return

    service.spreadsheets().values().batchUpdate(
        spreadsheetId=GOOGLE_SHEETS_ID,
        body={"valueInputOption": "RAW", "data": data},
    ).execute()


PRODUCT_URL_BASE = "https://razbor-angar.ru/p"

# ═══════════════════════════════════════════
# CSV-фид наличия (новый формат — drom)
# ═══════════════════════════════════════════

def download_feed():
    """Скачивает CSV-фид (drom-формат), возвращает список товаров в наличии."""
    print(f"Downloading feed from {FEED_URL}...")
    resp = urllib.request.urlopen(FEED_URL)
    raw = resp.read().decode("cp1251")
    reader = csv.DictReader(io.StringIO(raw), delimiter=";", quotechar='"')

    items = []
    for row in reader:
        if row.get("Статус", "").strip() != "В наличии":
            continue

        item_id = row.get("Артикул", "").strip()

        # Собираем позицию из структурированных полей
        pos_parts = []
        pz = row.get("Перед/Зад", "").strip()
        lr = row.get("Лев/Прав", "").strip()
        if pz:
            pos_parts.append("передний" if "перед" in pz.lower() else "задний")
        if lr:
            pos_parts.append("левый" if "лев" in lr.lower() else "правый")
        feed_position = " ".join(pos_parts)

        # Краткое описание для Telegram: кузов, год
        desc_parts = []
        if row.get("Кузов", "").strip():
            desc_parts.append(row["Кузов"].strip())
        if row.get("Год", "").strip():
            desc_parts.append(row["Год"].strip())
        short_desc = ", ".join(desc_parts)

        # category = "Марка Модель" для совместимости с заявками
        brand = row.get("Марка", "").strip()
        model = row.get("Модель", "").strip()
        category = f"{brand} {model}".strip()

        items.append({
            "id": item_id,
            "category": category,
            "name": row.get("Наименование", "").strip(),
            "price": row.get("Цена", "").strip(),
            "url": f"{PRODUCT_URL_BASE}{item_id}",
            "short_desc": short_desc,
            "position": feed_position,
            "vendor_codes": row.get("Номер", "").strip(),
            "brand": brand,
            "model": model,
            "year": row.get("Год", "").strip(),
        })

    print(f"Feed loaded: {len(items)} items available")
    return items


# ═══════════════════════════════════════════
# Матчинг
# ═══════════════════════════════════════════

def position_matches(order_position, feed_position):
    """
    Проверяет совместимость позиции заявки и товара из фида.
    Позиция из фида уже нормализована: 'передний правый', 'задний', '' и т.д.
    """
    if not order_position:
        return True
    if not feed_position:
        return True

    op = order_position.lower()
    fp = feed_position.lower()

    if op == fp:
        return True
    if op in ("передний", "задний"):
        return fp.startswith(op)
    if op in ("левый", "правый"):
        return fp.endswith(op)

    return False


def find_matches(order_category, order_part_name, order_position, order_catalog_number, excluded_ids_str, feed_items):
    """
    Ищет совпадения в фиде. Два режима:
    1. Если есть каталожный номер — ищет по нему среди всех vendor_codes товара
    2. Иначе — по category + name + position

    Исключает товары из excluded_ids.
    """
    excluded = set()
    if excluded_ids_str:
        for eid in excluded_ids_str.split(","):
            eid = eid.strip()
            if eid:
                excluded.add(eid)

    matches = []
    cat_num = order_catalog_number.strip().lower() if order_catalog_number else ""

    for item in feed_items:
        if item["id"] in excluded:
            continue

        # Режим 1: матч по каталожному номеру (если клиент указал)
        if cat_num:
            vendor_codes = [c.strip().lower() for c in item["vendor_codes"].split(",") if c.strip()]
            if cat_num in vendor_codes:
                if position_matches(order_position, item["position"]):
                    matches.append(item)
            continue

        # Режим 2: матч по category + name + position
        if (item["category"].lower() == order_category.lower() and
                item["name"].lower() == order_part_name.lower()):
            if position_matches(order_position, item["position"]):
                matches.append(item)

    return matches


# ═══════════════════════════════════════════
# Telegram
# ═══════════════════════════════════════════

def send_telegram(text):
    """Отправляет сообщение в Telegram."""
    url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"
    payload = json.dumps({
        "chat_id": TELEGRAM_CHAT_ID,
        "text": text,
        "parse_mode": "HTML",
        "disable_web_page_preview": True,
    }).encode("utf-8")

    req = urllib.request.Request(url, data=payload, headers={"Content-Type": "application/json"})
    try:
        urllib.request.urlopen(req)
    except Exception as e:
        print(f"Telegram error: {e}")


def split_and_send_telegram(text, max_len=4000):
    """Telegram ограничивает сообщения ~4096 символами. Разбиваем если нужно."""
    if len(text) <= max_len:
        send_telegram(text)
        return

    lines = text.split("\n")
    chunk = ""
    for line in lines:
        if len(chunk) + len(line) + 1 > max_len:
            send_telegram(chunk)
            chunk = ""
        chunk += line + "\n"
    if chunk.strip():
        send_telegram(chunk)


# ═══════════════════════════════════════════
# Основной процесс
# ═══════════════════════════════════════════

def get_cell(row, col_idx, default=""):
    """Безопасное получение значения ячейки."""
    if col_idx < len(row):
        return str(row[col_idx]).strip()
    return default


def main():
    today = date.today().isoformat()
    print(f"=== Сверка заявок: {today} ===")

    # 1. Скачиваем фид
    feed_items = download_feed()

    # 2. Читаем заявки
    service = get_sheets_service()
    rows = read_orders(service)
    print(f"Orders loaded: {len(rows)} rows")

    # 3. Обрабатываем
    updates = []

    # Группировка по клиенту: ключ = (client_name, contact_type, contact_nick, contact_phone, category, generation, restyling)
    # Значение = список найденных деталей
    clients = {}

    for i, row in enumerate(rows):
        status = get_cell(row, COL["status"])

        if status != "Ждёт деталь":
            continue

        order_id = get_cell(row, COL["id"])
        client_name = get_cell(row, COL["client_name"])
        contact_type = get_cell(row, COL["contact_type"])
        contact_nick = get_cell(row, COL["contact_nick"])
        contact_phone = get_cell(row, COL["contact_phone"])
        category = get_cell(row, COL["category"])
        year = get_cell(row, COL["year"])
        generation = get_cell(row, COL["generation"])
        restyling = get_cell(row, COL["restyling"])
        part_name = get_cell(row, COL["part_name"])
        position = get_cell(row, COL["position"])
        wait_until = get_cell(row, COL["wait_until"])
        excluded_ids = get_cell(row, COL["excluded_ids"])

        catalog_number = get_cell(row, COL["catalog_number"])

        # Проверяем просрочку
        if wait_until:
            try:
                if date.fromisoformat(wait_until) < date.today():
                    updates.append({"row": i, "col": COL["status"], "value": "Просрочено"})
                    continue
            except ValueError:
                pass

        # Ищем совпадения в фиде
        matches = find_matches(category, part_name, position, catalog_number, excluded_ids, feed_items)

        if matches:
            updates.append({"row": i, "col": COL["status"], "value": "Найдено — связаться"})
            updates.append({"row": i, "col": COL["matched_at"], "value": today})

            # Ключ группировки — клиент + авто
            client_key = (client_name, contact_type, contact_nick, contact_phone, category, year, generation, restyling)

            if client_key not in clients:
                clients[client_key] = []

            part_display = part_name
            if position:
                part_display += f" ({position})"

            part_block = f"🔔 {part_display}\n"
            for m in matches:
                price_str = f"{m['price']} ₽" if m["price"] else "цена не указана"
                short_desc = m.get("short_desc", "")
                if short_desc:
                    part_block += f"• {m['name']} ({short_desc}) — {price_str}\n  {m['url']}\n"
                else:
                    part_block += f"• {m['name']} — {price_str}\n  {m['url']}\n"

            clients[client_key].append(part_block)

    # 4. Применяем обновления пакетно
    if updates:
        print(f"Applying {len(updates)} updates...")
        batch_update_cells(service, updates)

    # 5. Формируем сводку в Telegram
    print(f"Results: {len(clients)} clients with matches")

    if not clients:
        split_and_send_telegram("Сегодня новых совпадений нет. Хорошего дня! ✌️")
        print("Done! No matches.")
        return

    msg_parts = []
    msg_parts.append(f"Всем привет! Для <b>{len(clients)}</b> клиентов появились детали, они их ждут. Свяжитесь с ними сегодня! 🚗\n")

    for client_key, parts in clients.items():
        client_name, contact_type, contact_nick, contact_phone, category, year, generation, restyling = client_key

        msg_parts.append("━━━━━━━━━━━━━━━━━━━━━\n")

        # Клиент
        msg_parts.append(f"👤 <b>{client_name}</b>")

        # Контакты — всегда показываем оба если есть
        contact_parts = []
        if contact_nick:
            contact_parts.append(f"{contact_type}: {contact_nick}")
        if contact_phone:
            contact_parts.append(f"Тел: {contact_phone}")
        if contact_parts:
            msg_parts.append(" | ".join(contact_parts))
        else:
            msg_parts.append("Контакт не указан")

        # Авто — одна строка
        car = category
        if year:
            car += f" {year}"
        if generation:
            car += f", {generation} поколение"
        if restyling:
            car += f", {restyling}"
        msg_parts.append(car + "\n")

        # Детали
        for part_block in parts:
            msg_parts.append(part_block)

    msg_parts.append("━━━━━━━━━━━━━━━━━━━━━\n")
    msg_parts.append("Хорошего дня! ✌️")

    full_message = "\n".join(msg_parts)
    split_and_send_telegram(full_message)
    print("Done!")


if __name__ == "__main__":
    main()
