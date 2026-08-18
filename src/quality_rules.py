import json
import re
from datetime import datetime


ARABIC_DIGITS = str.maketrans(
    "٠١٢٣٤٥٦٧٨٩٫٬",
    "0123456789.,"
)

KNOWN_PRICE_WORDS = {
    "ألف": 1000,
    "ألفان": 2000,
    "ثلاثة آلاف": 3000,
    "أربعة آلاف": 4000,
    "خمسة آلاف": 5000,
}

STATUS_ALIASES = {
    "مؤكد": "confirmed",
    "قيد الانتظار": "pending",
    "مرتجع": "returned",
    "مدفوع": "paid",
    "تم الدفع": "paid",
    "بانتظار الدفع": "pending",
}


def add_correction(corrections, field, original, corrected, rule_code):
    if original != corrected:
        corrections.append({
            "field": field,
            "original_value": original,
            "corrected_value": corrected,
            "rule_code": rule_code,
        })


def normalize_arabic_digits(value):
    if not isinstance(value, str):
        return value

    return value.translate(ARABIC_DIGITS)


def clean_number(value):
    if value is None:
        return None

    text = normalize_arabic_digits(str(value)).strip()

    text = text.replace(",", "")

    match = re.search(r"-?\d+(\.\d+)?", text)

    if not match:
        return None

    return float(match.group())


def normalize_currency(value):
    if value is None:
        return None

    text = str(value).strip()

    currency_map = {
        "YER": "YER",
        "ريال": "YER",
        "ريال يمني": "YER",
        "SAR": "SAR",
        "USD": "USD",
    }

    return currency_map.get(text, text)


def normalize_phone(value):
    if value is None:
        return None

    phone = re.sub(r"\D", "", str(value))

    if phone.startswith("967"):
        phone = phone[3:]

    if len(phone) == 9:
        return phone

    return None


def normalize_email(value):
    if value is None:
        return None

    email = str(value).strip().lower()

    while "@@" in email:
        email = email.replace("@@", "@")

    while ".." in email:
        email = email.replace("..", ".")

    if email.count("@") != 1:
        return None

    return email


def normalize_date(value):
    if not value:
        return None

    formats = [
        "%Y-%m-%dT%H:%M:%S",
        "%Y-%m-%d",
        "%d/%m/%Y",
        "%Y/%m/%d",
    ]

    for date_format in formats:
        try:
            parsed = datetime.strptime(str(value).strip(), date_format)
            return parsed.isoformat()
        except ValueError:
            continue

    return None


def normalize_status(value):
    if value is None:
        return None

    cleaned = str(value).strip()

    return STATUS_ALIASES.get(cleaned, cleaned)


def validate_items(items_json):
    if not items_json:
        return None, "ITEMS_EMPTY"

    try:
        items = json.loads(items_json)
    except (json.JSONDecodeError, TypeError):
        return None, "JSON_ITEMS_CORRUPTED"

    if not items:
        return None, "ITEMS_EMPTY"

    for item in items:
        qty = item.get("qty")
        price = item.get("unit_price")

        if qty is None or price is None:
            return None, "PRICE_UNKNOWN"

        if qty < 0 or price < 0:
            return None, "VALUE_NEGATIVE_AMBIGUOUS"

    return items, None