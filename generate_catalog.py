"""
generate_catalog.py — Генерация JSON-каталога из CSV-фида наличия.

Скачивает CSV по URL (или читает локальный файл), парсит category и name,
формирует структуру: бренд → модели (category) → детали (name).

Используется:
  - GitHub Actions (еженедельный cron) для автообновления каталога
  - Локально для первичной генерации

Usage:
  python generate_catalog.py                          # скачать с URL по умолчанию
  python generate_catalog.py --file feed.csv          # из локального файла
  python generate_catalog.py --url https://...        # из другого URL
  python generate_catalog.py --output docs/catalog.json  # в другой путь
"""

import csv
import json
import io
import sys
import argparse
from datetime import date
from urllib.request import urlopen

DEFAULT_FEED_URL = "https://baz-on.ru/export/c1326/2b944/razborangar-drom.csv"
DEFAULT_OUTPUT = "catalog.json"

# Нормализация брендов: ключ = uppercase, значение = красивое отображение
BRAND_DISPLAY = {
    "AUDI": "Audi",
    "BMW": "BMW",
    "CITROEN": "Citroën",
    "DODGE": "Dodge",
    "FORD": "Ford",
    "HONDA": "Honda",
    "HYUNDAI": "Hyundai",
    "KIA": "Kia",
    "MINI": "MINI",
    "MAZDA": "Mazda",
    "MERCEDES-BENZ": "Mercedes-Benz",
    "MITSUBISHI": "Mitsubishi",
    "NISSAN": "Nissan",
    "OPEL": "Opel",
    "PEUGEOT": "Peugeot",
    "RENAULT": "Renault",
    "SKODA": "Škoda",
    "SUBARU": "Subaru",
    "TOYOTA": "Toyota",
    "VOLKSWAGEN": "Volkswagen",
    "VOLVO": "Volvo",
}


def read_csv_from_url(url: str) -> list[dict]:
    """Скачать CSV (CP1251) по URL и вернуть список словарей."""
    print(f"Downloading feed from {url} ...")
    resp = urlopen(url)
    raw = resp.read().decode("cp1251")
    reader = csv.DictReader(io.StringIO(raw), delimiter=";", quotechar='"')
    return list(reader)


def read_csv_from_file(path: str) -> list[dict]:
    """Прочитать локальный CSV (CP1251)."""
    print(f"Reading feed from {path} ...")
    with open(path, "r", encoding="cp1251") as f:
        reader = csv.DictReader(f, delimiter=";", quotechar='"')
        return list(reader)


def normalize_brand(category: str) -> str:
    """Извлечь и нормализовать бренд из category."""
    parts = category.strip().split()
    if not parts:
        return category
    # Handle Mercedes-Benz (hyphenated)
    if len(parts) >= 1 and parts[0].upper().startswith("MERCEDES"):
        raw = "MERCEDES-BENZ"
    else:
        raw = parts[0].upper()
    return BRAND_DISPLAY.get(raw, parts[0].title())


def build_catalog(rows: list[dict]) -> dict:
    """Построить структуру каталога из строк CSV. Поддерживает оба формата фида."""
    brand_map: dict[str, set] = {}   # brand_display -> set of categories
    cat_parts: dict[str, set] = {}   # category -> set of part names

    for row in rows:
        # Поддержка нового формата (drom) и старого (socposter)
        if "Марка" in row:
            # Новый формат
            raw_brand = row.get("Марка", "").strip()
            model = row.get("Модель", "").strip()
            cat = f"{raw_brand} {model}".strip()
            name = row.get("Наименование", "").strip()
        else:
            # Старый формат
            cat = row.get("category", "").strip()
            name = row.get("name", "").strip()
            raw_brand = cat.split()[0] if cat else ""

        if not cat or not name:
            continue

        brand = normalize_brand(cat)

        if brand not in brand_map:
            brand_map[brand] = set()
        brand_map[brand].add(cat)

        if cat not in cat_parts:
            cat_parts[cat] = set()
        cat_parts[cat].add(name)

    # Build output
    catalog = {
        "generated": date.today().isoformat(),
        "feed_url": DEFAULT_FEED_URL,
        "stats": {
            "brands": len(brand_map),
            "models": sum(len(v) for v in brand_map.values()),
            "unique_parts": len(set().union(*cat_parts.values())) if cat_parts else 0,
        },
        "brands": [],
    }

    for brand in sorted(brand_map.keys()):
        brand_obj = {"name": brand, "models": []}
        for cat in sorted(brand_map[brand]):
            parts_list = sorted(cat_parts.get(cat, []))
            brand_obj["models"].append({"category": cat, "parts": parts_list})
        catalog["brands"].append(brand_obj)

    # Flat list of all unique parts (for potential global search)
    all_parts = set()
    for parts in cat_parts.values():
        all_parts.update(parts)
    catalog["all_parts"] = sorted(all_parts)

    return catalog


def main():
    parser = argparse.ArgumentParser(description="Generate catalog.json from CSV feed")
    parser.add_argument("--url", default=DEFAULT_FEED_URL, help="Feed URL")
    parser.add_argument("--file", default=None, help="Local CSV file (overrides --url)")
    parser.add_argument("--output", default=DEFAULT_OUTPUT, help="Output JSON path")
    args = parser.parse_args()

    if args.file:
        rows = read_csv_from_file(args.file)
    else:
        rows = read_csv_from_url(args.url)

    print(f"Parsed {len(rows)} rows from CSV")

    catalog = build_catalog(rows)
    stats = catalog["stats"]
    print(f"Catalog: {stats['brands']} brands, {stats['models']} models, {stats['unique_parts']} unique parts")

    with open(args.output, "w", encoding="utf-8") as f:
        json.dump(catalog, f, ensure_ascii=False, indent=2)

    print(f"Saved to {args.output} ({len(json.dumps(catalog, ensure_ascii=False)):,} chars)")


if __name__ == "__main__":
    main()
