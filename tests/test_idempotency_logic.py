import unittest

from src.spark_upsert import (
    business_documents_equal,
)


class TestIdempotencyLogic(unittest.TestCase):

    def test_same_business_record_is_unchanged(self):
        existing = {
            "order_id": "ORD-1",
            "order_date": "2026-08-19",
            "status": "confirmed",
            "customer_id": "C-1",
            "customer_name": "Sara",
            "customer_phone": "777123456",
            "customer_email": "sara@example.com",
            "city": "Sanaa",
            "district": "Test",
            "delivery_type": "standard",
            "delivery_cost": 500.0,
            "payment_method": "cash",
            "payment_status": "paid",
            "payment_amount": 3000.0,
            "currency": "YER",
            "total_amount": 3000.0,
            "items": [],
            "quality_status": "corrected",
            "corrections": [],
            "last_run_id": "old-run",
        }

        new = dict(
            existing
        )

        new[
            "last_run_id"
        ] = "new-run"

        self.assertTrue(
            business_documents_equal(
                existing,
                new,
            )
        )

    def test_changed_business_record_is_updated(self):
        existing = {
            "order_id": "ORD-1",
            "total_amount": 3000.0,
        }

        new = {
            "order_id": "ORD-1",
            "total_amount": 3500.0,
        }

        self.assertFalse(
            business_documents_equal(
                existing,
                new,
            )
        )


if __name__ == "__main__":
    unittest.main()