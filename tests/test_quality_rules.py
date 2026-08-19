import unittest

from src.quality_rules import (
    clean_number,
    normalize_currency,
    normalize_date,
    normalize_email,
    normalize_phone,
    normalize_status,
    recompute_total,
    validate_items,
)


class TestQualityRules(unittest.TestCase):

    def test_arabic_digits(self):
        self.assertEqual(
            clean_number(
                "١٢٣٤"
            ),
            1234.0,
        )

    def test_thousands_separator(self):
        self.assertEqual(
            clean_number(
                "1,500"
            ),
            1500.0,
        )

    def test_price_words(self):
        self.assertEqual(
            clean_number(
                "خمسة آلاف"
            ),
            5000.0,
        )

    def test_currency(self):
        self.assertEqual(
            normalize_currency(
                "ريال يمني"
            ),
            "YER",
        )

    def test_phone(self):
        self.assertEqual(
            normalize_phone(
                "+967777123456"
            ),
            "777123456",
        )

    def test_email(self):
        self.assertEqual(
            normalize_email(
                "TEST@@MAIL..COM"
            ),
            "test@mail.com",
        )

    def test_date(self):
        self.assertEqual(
            normalize_date(
                "19/08/2026"
            ),
            "2026-08-19T00:00:00",
        )

    def test_status_alias(self):
        self.assertEqual(
            normalize_status(
                "مؤكد"
            ),
            "confirmed",
        )

    def test_valid_items(self):
        items_json = (
            '[{"sku":"A1",'
            '"name":"Item",'
            '"qty":2,'
            '"unit_price":1000,'
            '"total":2000}]'
        )

        items, error = (
            validate_items(
                items_json
            )
        )

        self.assertIsNone(
            error
        )

        self.assertEqual(
            len(items),
            1,
        )

    def test_corrupted_items(self):
        items, error = (
            validate_items(
                "{bad json}"
            )
        )

        self.assertIsNone(
            items
        )

        self.assertEqual(
            error,
            "JSON_ITEMS_CORRUPTED",
        )

    def test_negative_item(self):
        items_json = (
            '[{"sku":"A1",'
            '"name":"Item",'
            '"qty":-1,'
            '"unit_price":1000,'
            '"total":-1000}]'
        )

        items, error = (
            validate_items(
                items_json
            )
        )

        self.assertIsNone(
            items
        )

        self.assertEqual(
            error,
            "VALUE_NEGATIVE_AMBIGUOUS",
        )

    def test_recompute_total(self):
        items = [
            {
                "qty": 2,
                "unit_price": 1000,
                "total": 2000,
            },
            {
                "qty": 1,
                "unit_price": 500,
                "total": 500,
            },
        ]

        result = recompute_total(
            items,
            500,
        )

        self.assertEqual(
            result,
            3000.0,
        )


if __name__ == "__main__":
    unittest.main()