import unittest

from google.cloud import bigquery
from main import get_tables


class TestGetTablesReal(unittest.TestCase):
    def setUp(self):
        self.project_id = "any-development"  # ここに実際のプロジェクトIDを設定
        self.dataset_id = "TEST_DATA"  # ここに実際のデータセットIDを設定
        self.bq_client = bigquery.Client(project=self.project_id)

    def test_get_tables_real(self):
        try:
            tables = get_tables(self.bq_client, self.dataset_id)
            print(f"Tables in dataset {self.dataset_id}: {tables}")
            self.assertIsInstance(tables, list)
        except Exception as e:
            self.fail(f"get_tables raised an exception: {e}")


if __name__ == "__main__":
    unittest.main()
