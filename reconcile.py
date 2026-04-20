"""
Стол заказов — Скрипт ежедневной сверки v3.

Запускается через GitHub Actions (cron) раз в день в 09:00 МСК.
1. Скачивает CSV-фид наличия
2. Читает из Google Sheets заявки со статусом «Ждёт деталь»
3. Сверяет category + part_name (+position, +catalog_number) с фидом, исключая excluded_ids
4. Совпадения → статус «Найдено — связаться» + matched_at + дополняет matched_ids
5. Просроченные → статус «Просрочено»
6. Отправляет сводку в Telegram с новым шаблоном
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
SHEETS_URL = os.environ.get("SHEETS_URL", "")

# Теги продавцов через запятую, например: "@ivanov,@petrov,@sidorov"
SELLER_TAGS = os.environ.get("SELLER_TAGS", "")

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
    "matched_ids": 20,  # Колонка U — скрипт дополняет, не перезаписывает
}

DATA_START_ROW = 4  # строки 1-3: заголовки, описания, пример

PRODUCT_URL_BASE = "https://razbor-angar.ru/p"

# Динамическое заверение по дням недели
CLOSING_BY_WEEKDAY = {
    0: "Хорошего понедельника! ✌️",
    1: "Продуктивного вторника! ✌️",
    2: "Хорошей среды! ✌️",
    3: "Продуктивного четверга! ✌️",
    4: "Хорошей пятницы! ✌️",
    5: "Хорошей субботы! ✌️",
    6: "Хорошего воскресенья! ✌️",
}


# ═══════════════════════════════════════════
# Google Sheets API
# ═══════════════════════════════════════════

from google.oauth2 import service_account
from googleapiclient.discovery import build


def get_sheets_service():
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
        range=f"Заявки!A{DATA_START_ROW}:U",  # A–U включая matched_ids
    ).execute()
    return result.get("values", [])


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
    print(f"Downloading feed from {FEED_URL}...")
    resp = urllib.request.urlopen(FEED_URL)
    raw = resp.read().decode("cp1251")
    reader = csv.DictReader(io.StringIO(raw), delimiter=";", quotechar='"')

    items = []
    for row in reader:
        if row.get("Статус", "").strip() != "В наличии":
            continue

        item_id = row.get("Артикул", "").strip()

        pos_parts = []
        pz = row.get("Перед/Зад", "").strip()
        lr = row.get("Лев/Прав", "").strip()
        if pz:
            pos_parts.append("передний" if "перед" in pz.lower() else "задний")
        if lr:
            pos_parts.append("левый" if "лев" in lr.lower() else "правый")
        feed_position = " ".join(pos_parts)

        desc_parts = []
        if row.get("Кузов", "").strip():
            desc_parts.append(row["Кузов"].strip())
        if row.get("Год", "").strip():
            desc_parts.append(row["Год"].strip())
        short_desc = ", ".join(desc_parts)

        brand = row.get("Марка", "").strip()
        model = row.get("Модель", "").strip()
        category = f"{brand} {model}".strip()

        price_raw = row.get("Цена", "").strip()
        try:
            price_val = float(price_raw) if price_raw else None
        except ValueError:
            price_val = None

        items.append({
            "id": item_id,
            "category": category,
            "name": row.get("Наименование", "").strip(),
            "price": price_raw,
            "price_val": price_val,
            "url": f"{PRODUCT_URL_BASE}{item_id}",
            "short_desc": short_desc,
            "position": feed_position,
            "vendor_codes": row.get("Номер", "").strip(),
        })

    print(f"Feed loaded: {len(items)} items available")
    return items


# ═══════════════════════════════════════════
# Матчинг
# ═══════════════════════════════════════════

def position_matches(order_position, feed_position):
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

        if cat_num:
            vendor_codes = [c.strip().lower() for c in item["vendor_codes"].split(",") if c.strip()]
            if cat_num in vendor_codes:
                if position_matches(order_position, item["position"]):
                    matches.append(item)
            continue

        if (item["category"].lower() == order_category.lower() and
                item["name"].lower() == order_part_name.lower()):
            if position_matches(order_position, item["position"]):
                matches.append(item)

    return matches


# ═══════════════════════════════════════════
# Вспомогательные функции
# ═══════════════════════════════════════════

def get_cell(row, col_idx, default=""):
    if col_idx < len(row):
        return str(row[col_idx]).strip()
    return default


def price_range(matches):
    """Возвращает строку диапазона цен или одну цену."""
    prices = [m["price_val"] for m in matches if m["price_val"] is not None]
    if not prices:
        return None
    lo = min(prices)
    hi = max(prices)
    if lo == hi:
        return f"{int(lo):,}".replace(",", " ") + " ₽"
    return f"{int(lo):,}".replace(",", " ") + " — " + f"{int(hi):,}".replace(",", " ") + " ₽"


def average_price(matches):
    """Возвращает среднюю цену по позициям или None."""
    prices = [m["price_val"] for m in matches if m["price_val"] is not None]
    if not prices:
        return None
    return sum(prices) / len(prices)


def merge_matched_ids(existing_str, new_ids):
    """Дополняет matched_ids новыми ID, не затирая старые."""
    existing = set()
    if existing_str:
        for eid in existing_str.split(","):
            e = eid.strip()
            if e:
                existing.add(e)
    merged = existing | set(new_ids)
    return ",".join(sorted(merged))


# ═══════════════════════════════════════════
# Telegram
# ═══════════════════════════════════════════

def send_telegram(text):
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

def main():
    today = date.today().isoformat()
    weekday = date.today().weekday()
    closing = CLOSING_BY_WEEKDAY[weekday]

    print(f"=== Сверка заявок: {today} ===")

    feed_items = download_feed()

    service = get_sheets_service()
    rows = read_orders(service)
    print(f"Orders loaded: {len(rows)} rows")

    updates = []

    # Группировка новых совпадений по клиенту для сообщения
    clients = {}

    # Счётчики по всему файлу
    total_active = 0        # все не финальные статусы
    total_need_contact = 0  # статус «Найдено — связаться» по всему файлу

    FINAL_STATUSES = {"Продано", "Отказ", "Просрочено"}

    # Суммируем среднюю цену по новым совпадениям для выручки
    today_avg_sum = 0.0

    for i, row in enumerate(rows):
        status = get_cell(row, COL["status"])

        # Считаем счётчики по всему файлу
        if status not in FINAL_STATUSES and status:
            total_active += 1
        if status == "Найдено — связаться":
            total_need_contact += 1

        if status != "Ждёт деталь":
            continue

        order_id        = get_cell(row, COL["id"])
        client_name     = get_cell(row, COL["client_name"])
        contact_type    = get_cell(row, COL["contact_type"])
        contact_nick    = get_cell(row, COL["contact_nick"])
        contact_phone   = get_cell(row, COL["contact_phone"])
        category        = get_cell(row, COL["category"])
        year            = get_cell(row, COL["year"])
        generation      = get_cell(row, COL["generation"])
        restyling       = get_cell(row, COL["restyling"])
        part_name       = get_cell(row, COL["part_name"])
        position        = get_cell(row, COL["position"])
        wait_until      = get_cell(row, COL["wait_until"])
        excluded_ids    = get_cell(row, COL["excluded_ids"])
        catalog_number  = get_cell(row, COL["catalog_number"])
        existing_matched_ids = get_cell(row, COL["matched_ids"])

        # Проверяем просрочку
        if wait_until:
            try:
                if date.fromisoformat(wait_until) < date.today():
                    updates.append({"row": i, "col": COL["status"], "value": "Просрочено"})
                    continue
            except ValueError:
                pass

        matches = find_matches(category, part_name, position, catalog_number, excluded_ids, feed_items)

        if not matches:
            continue

        # Обновляем статус, matched_at, matched_ids
        updates.append({"row": i, "col": COL["status"], "value": "Найдено — связаться"})
        updates.append({"row": i, "col": COL["matched_at"], "value": today})

        new_ids = [m["id"] for m in matches]
        merged_ids = merge_matched_ids(existing_matched_ids, new_ids)
        updates.append({"row": i, "col": COL["matched_ids"], "value": merged_ids})

        # Считаем среднюю цену по заявке для выручки сегодня
        avg = average_price(matches)
        if avg:
            today_avg_sum += avg

        # Обновляем счётчик — эта заявка переходит в «Найдено — связаться»
        total_need_contact += 1

        # Группируем для сообщения
        client_key = (client_name, contact_type, contact_nick, contact_phone, category, year, generation, restyling)

        if client_key not in clients:
            clients[client_key] = []

        part_display = part_name
        if position:
            part_display += f" ({position})"

        p_range = price_range(matches)

        part_block = f"🔔 <b>{part_display}</b>\n"
        for m in matches:
            price_str = f"{m['price']} ₽" if m["price"] else "цена не указана"
            short_desc = m.get("short_desc", "")
            if short_desc:
                part_block += f"  · {price_str} — {m['url']}\n"
            else:
                part_block += f"  · {price_str} — {m['url']}\n"

        if p_range:
            part_block += f"  Примерная стоимость детали: {p_range}\n"

        clients[client_key].append(part_block)

    # Применяем обновления
    if updates:
        print(f"Applying {len(updates)} updates...")
        batch_update_cells(service, updates)

    print(f"Results: {len(clients)} new matches, {total_need_contact} need contact, {total_active} active")

    # ─── Формируем сообщение ───

    if not clients:
        msg = f"Сегодня новых совпадений нет.\n\n{closing}"
        split_and_send_telegram(msg)
        print("Done! No matches.")
        return

    lines = []

    # Приветствие
    lines.append(f"<b>Всем привет!</b> Для {len(clients)} клиентов появились детали, они их ждут. Свяжитесь с ними сегодня! 🚗")

    # Блоки по клиентам
    for client_key, parts in clients.items():
        client_name, contact_type, contact_nick, contact_phone, category, year, generation, restyling = client_key

        lines.append("")
        lines.append("─ ─ ─")
        lines.append("")

        lines.append(f"<b>{client_name}</b>")

        contact_parts = []
        if contact_nick:
            contact_parts.append(f"{contact_type}: {contact_nick}")
        if contact_phone:
            contact_parts.append(f"Тел: {contact_phone}")
        if contact_parts:
            lines.append(" | ".join(contact_parts))

        car = category
        if year:
            car += f" {year}"
        if generation:
            car += f", {generation} поколение"
        if restyling:
            car += f", {restyling}"
        lines.append(car)
        lines.append("")

        for part_block in parts:
            lines.append(part_block)

    # Выручка сегодня
    lines.append("")
    lines.append("─ ─ ─")
    lines.append("")
    if today_avg_sum > 0:
        revenue_str = f"{int(today_avg_sum):,}".replace(",", " ")
        lines.append(f"Потенциальная выручка при продаже сегодня: ~<b>{revenue_str} ₽</b>")

    # Итоговый блок
    lines.append("")
    lines.append("= = =")
    lines.append("")
    lines.append(f"Всего актуальных заявок: {total_active}")
    lines.append(f"Нужно связаться с: <b>{total_need_contact}</b>")

    if SHEETS_URL:
        lines.append(f"\n<a href=\"{SHEETS_URL}\">Перейти в стол заказов</a>")

    # Теги продавцов
    if SELLER_TAGS:
        tags = " ".join(t.strip() for t in SELLER_TAGS.split(",") if t.strip())
        lines.append(f"\n{tags}")

    # Заверение
    lines.append(f"\n{closing}")

    full_message = "\n".join(lines)
    split_and_send_telegram(full_message)
    print("Done!")


if __name__ == "__main__":
    main()
