import unittest


class TestConsistency(unittest.TestCase):

    def test_raw_equals_classified(self):
        raw = 10

        valid = 0
        corrected = 8
        quarantine = 2

        classified = (
            valid
            + corrected
            + quarantine
        )

        self.assertEqual(
            raw,
            classified,
        )

    def test_large_example_consistency(self):
        raw = 500000

        quarantine = 33359
        accepted = (
            raw
            - quarantine
        )

        valid = 1868
        corrected = (
            accepted
            - valid
        )

        self.assertEqual(
            raw,
            (
                valid
                + corrected
                + quarantine
            ),
        )


if __name__ == "__main__":
    unittest.main()