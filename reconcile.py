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

FEED_URL = os.environ.get("FEED_URL", "https://baz-on.ru/export/c1326/5edf4/razborangar-socposter.csv")
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


# ═══════════════════════════════════════════
# CSV-фид наличия
# ═══════════════════════════════════════════

def download_feed():
    """Скачивает CSV-фид, возвращает список товаров в наличии."""
    print(f"Downloading feed from {FEED_URL}...")
    resp = urllib.request.urlopen(FEED_URL)
    raw = resp.read().decode("cp1251")
    reader = csv.DictReader(io.StringIO(raw), delimiter=";", quotechar='"')

    items = []
    for row in reader:
        if row.get("available", "").strip().lower() == "true":
            items.append({
                "id": row.get("id", "").strip(),
                "category": row.get("category", "").strip(),
                "name": row.get("name", "").strip(),
                "price": row.get("price", "").strip(),
                "url": row.get("url", "").strip(),
                "description": row.get("description", "").strip().split("\n")[0],  # первая строка
                "vendorCode": row.get("vendorCode", "").strip(),
            })

    print(f"Feed loaded: {len(items)} items available")
    return items


# ═══════════════════════════════════════════
# Матчинг
# ═══════════════════════════════════════════

def find_matches(order_category, order_part_name, excluded_ids_str, feed_items):
    """
    Ищет совпадения по category + name в фиде.
    Исключает товары из excluded_ids.
    Возвращает список совпавших товаров.
    """
    # Парсим excluded_ids
    excluded = set()
    if excluded_ids_str:
        for eid in excluded_ids_str.split(","):
            eid = eid.strip()
            if eid:
                excluded.add(eid)

    matches = []
    for item in feed_items:
        if item["id"] in excluded:
            continue
        if (item["category"].lower() == order_category.lower() and
                item["name"].lower() == order_part_name.lower()):
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
    updates = []        # Пакетные обновления ячеек
    matched_orders = []  # Для Telegram-отчёта
    expired_orders = []  # Просроченные
    waiting_count = 0    # Счётчик ожидающих

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
        part_name = get_cell(row, COL["part_name"])
        position = get_cell(row, COL["position"])
        wait_until = get_cell(row, COL["wait_until"])
        excluded_ids = get_cell(row, COL["excluded_ids"])

        contact = contact_nick or contact_phone or "—"

        # Проверяем просрочку
        if wait_until:
            try:
                if date.fromisoformat(wait_until) < date.today():
                    updates.append({"row": i, "col": COL["status"], "value": "Просрочено"})
                    expired_orders.append(f"  {order_id}: {client_name} — {part_name} ({category})")
                    continue
            except ValueError:
                pass  # Некорректная дата — пропускаем проверку

        # Ищем совпадения в фиде
        matches = find_matches(category, part_name, excluded_ids, feed_items)

        if matches:
            # Обновляем статус и дату
            updates.append({"row": i, "col": COL["status"], "value": "Найдено — связаться"})
            updates.append({"row": i, "col": COL["matched_at"], "value": today})

            # Формируем блок для Telegram
            part_display = part_name
            if position:
                part_display += f" ({position})"

            block = f"🔔 <b>{client_name}</b> — {part_display}\n"
            block += f"   {contact_type}: {contact}\n"
            block += f"   Авто: {category}\n"

            for m in matches:
                price_str = f"{m['price']} ₽" if m["price"] else "цена не указана"
                block += f"   • <a href=\"{m['url']}\">{m['description'][:80]}</a> — {price_str}\n"

            matched_orders.append(block)
        else:
            waiting_count += 1

    # 4. Применяем обновления пакетно
    if updates:
        print(f"Applying {len(updates)} updates...")
        batch_update_cells(service, updates)

    # 5. Формируем и отправляем сводку в Telegram
    print(f"Results: {len(matched_orders)} matched, {len(expired_orders)} expired, {waiting_count} waiting")

    msg_parts = []
    msg_parts.append(f"📋 <b>Сверка заявок — {today}</b>\n")

    if matched_orders:
        msg_parts.append(f"✅ <b>Найдено совпадений: {len(matched_orders)}</b>\n")
        msg_parts.extend(matched_orders)
    else:
        msg_parts.append("Новых совпадений нет.\n")

    if expired_orders:
        msg_parts.append(f"\n⏰ <b>Просрочено: {len(expired_orders)}</b>")
        msg_parts.extend(expired_orders)

    msg_parts.append(f"\n📊 Ожидают деталь: {waiting_count}")

    full_message = "\n".join(msg_parts)
    split_and_send_telegram(full_message)
    print("Done!")


if __name__ == "__main__":
    main()
