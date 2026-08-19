import os
import tempfile
import unittest
from unittest.mock import patch

from src.file_router import (
    choose_engine,
    get_file_size_mb,
)


class TestFileRouter(unittest.TestCase):

    def test_get_file_size_mb(self):
        with tempfile.NamedTemporaryFile(
            delete=False
        ) as temp_file:
            temp_file.write(
                b"x" * 1024 * 1024
            )

            temp_path = temp_file.name

        try:
            size_mb = get_file_size_mb(
                temp_path
            )

            self.assertAlmostEqual(
                size_mb,
                1.0,
                places=2,
            )

        finally:
            os.remove(
                temp_path
            )

    @patch(
        "src.file_router."
        "SMALL_FILE_THRESHOLD_MB",
        200,
    )
    @patch(
        "src.file_router."
        "get_file_size_mb",
        return_value=5.0,
    )
    def test_small_file_uses_python_batch(
        self,
        mock_size,
    ):
        engine = choose_engine(
            "small.csv"
        )

        self.assertEqual(
            engine,
            "python_batch",
        )

    @patch(
        "src.file_router."
        "SMALL_FILE_THRESHOLD_MB",
        200,
    )
    @patch(
        "src.file_router."
        "get_file_size_mb",
        return_value=200.0,
    )
    def test_threshold_file_uses_python_batch(
        self,
        mock_size,
    ):
        engine = choose_engine(
            "threshold.csv"
        )

        self.assertEqual(
            engine,
            "python_batch",
        )

    @patch(
        "src.file_router."
        "SMALL_FILE_THRESHOLD_MB",
        200,
    )
    @patch(
        "src.file_router."
        "get_file_size_mb",
        return_value=201.0,
    )
    def test_large_file_uses_pyspark(
        self,
        mock_size,
    ):
        engine = choose_engine(
            "large.csv"
        )

        self.assertEqual(
            engine,
            "pyspark",
        )

    @patch(
        "src.file_router."
        "SMALL_FILE_THRESHOLD_MB",
        200,
    )
    @patch(
        "src.file_router."
        "get_file_size_mb",
        return_value=5.0,
    )
    @patch(
        "builtins.print"
    )
    def test_router_prints_reason(
        self,
        mock_print,
        mock_size,
    ):
        choose_engine(
            "small.csv"
        )

        printed_lines = [
            call.args[0]
            for call
            in mock_print.call_args_list
        ]

        self.assertTrue(
            any(
                line.startswith(
                    "Reason:"
                )
                for line
                in printed_lines
            )
        )


if __name__ == "__main__":
    unittest.main()