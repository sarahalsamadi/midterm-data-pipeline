import json
import re
from datetime import datetime


ARABIC_DIGITS = str.maketrans(
    "٠١٢٣٤٥٦٧٨٩٫٬",
    "0123456789.,",
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


def add_correction(
    corrections,
    field,
    original,
    corrected,
    rule_code,
):
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

    return value.translate(
        ARABIC_DIGITS
    )


def clean_number(value):
    if value is None:
        return None

    text = normalize_arabic_digits(
        str(value)
    ).strip()

    if text in KNOWN_PRICE_WORDS:
        return float(
            KNOWN_PRICE_WORDS[text]
        )

    text = text.replace(
        ",",
        "",
    )

    match = re.search(
        r"-?\d+(\.\d+)?",
        text,
    )

    if not match:
        return None

    return float(
        match.group()
    )


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

    return currency_map.get(
        text,
        text,
    )


def normalize_phone(value):
    if value is None:
        return None

    phone = re.sub(
        r"\D",
        "",
        str(value),
    )

    if phone.startswith("967"):
        phone = phone[3:]

    if len(phone) == 9:
        return phone

    return None


def normalize_email(value):
    if value is None:
        return None

    email = (
        str(value)
        .strip()
        .lower()
    )

    while "@@" in email:
        email = email.replace(
            "@@",
            "@",
        )

    while ".." in email:
        email = email.replace(
            "..",
            ".",
        )

    if email.count("@") != 1:
        return None

    return email


def normalize_date(value):
    if not value:
        return None

    formats = [
        "%Y-%m-%dT%H:%M:%S",
        "%Y-%m-%d %H:%M:%S",
        "%d-%m-%Y %H:%M:%S",
        "%d/%m/%Y %H:%M:%S",
        "%Y/%m/%d %H:%M:%S",
        "%Y-%m-%d",
        "%d/%m/%Y",
        "%Y/%m/%d",
    ]

    for date_format in formats:
        try:
            parsed = datetime.strptime(
                str(value).strip(),
                date_format,
            )

            return parsed.isoformat()

        except ValueError:
            continue

    return None


def normalize_status(value):
    if value is None:
        return None

    cleaned = str(value).strip()

    return STATUS_ALIASES.get(
        cleaned,
        cleaned,
    )


def normalize_item_number(value):
    if value is None:
        return None

    if isinstance(
        value,
        (int, float),
    ):
        return float(
            value
        )

    return clean_number(
        value
    )


def validate_items(items_json):
    if not items_json:
        return (
            None,
            "ITEMS_EMPTY",
        )

    try:
        items = json.loads(
            items_json
        )

    except (
        json.JSONDecodeError,
        TypeError,
    ):
        return (
            None,
            "JSON_ITEMS_CORRUPTED",
        )

    if not isinstance(
        items,
        list,
    ):
        return (
            None,
            "JSON_ITEMS_CORRUPTED",
        )

    if not items:
        return (
            None,
            "ITEMS_EMPTY",
        )

    normalized_items = []

    for item in items:
        if not isinstance(
            item,
            dict,
        ):
            return (
                None,
                "JSON_ITEMS_CORRUPTED",
            )

        qty = normalize_item_number(
            item.get(
                "qty"
            )
        )

        unit_price = normalize_item_number(
            item.get(
                "unit_price"
            )
        )

        item_total = normalize_item_number(
            item.get(
                "total"
            )
        )

        if (
            qty is None
            or unit_price is None
        ):
            return (
                None,
                "PRICE_UNKNOWN",
            )

        if (
            qty < 0
            or unit_price < 0
        ):
            return (
                None,
                "VALUE_NEGATIVE_AMBIGUOUS",
            )

        if (
            item_total is not None
            and item_total < 0
        ):
            return (
                None,
                "VALUE_NEGATIVE_AMBIGUOUS",
            )

        normalized_item = dict(
            item
        )

        normalized_item[
            "qty"
        ] = qty

        normalized_item[
            "unit_price"
        ] = unit_price

        if item_total is not None:
            normalized_item[
                "total"
            ] = item_total

        normalized_items.append(
            normalized_item
        )

    return (
        normalized_items,
        None,
    )


def recompute_total(
    items,
    delivery_cost,
):
    if (
        not items
        or delivery_cost is None
    ):
        return None

    items_total = 0.0

    for item in items:
        qty = normalize_item_number(
            item.get(
                "qty"
            )
        )

        unit_price = normalize_item_number(
            item.get(
                "unit_price"
            )
        )

        item_total = normalize_item_number(
            item.get(
                "total"
            )
        )

        if (
            qty is None
            or unit_price is None
        ):
            return None

        if (
            qty < 0
            or unit_price < 0
        ):
            return None

        if item_total is not None:
            if item_total < 0:
                return None

            items_total += (
                item_total
            )

        else:
            items_total += (
                qty
                * unit_price
            )

    return (
        items_total
        + float(
            delivery_cost
        )
    )