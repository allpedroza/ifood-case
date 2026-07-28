import importlib.util
import json
from pathlib import Path
import unittest

import yaml


ROOT = Path(__file__).resolve().parents[1]


class DashboardContractTest(unittest.TestCase):
    def test_every_widget_references_an_existing_dataset(self):
        dashboard = json.loads(
            (ROOT / "src/databricks/dashboards/data_quality.lvdash.json").read_text()
        )
        dataset_names = {dataset["name"] for dataset in dashboard["datasets"]}
        serialized = json.dumps(dashboard["pages"])

        for dataset_name in dataset_names:
            if f'"datasetName": "{dataset_name}"' in serialized:
                continue

        referenced = set()

        def collect(value):
            if isinstance(value, dict):
                if "datasetName" in value:
                    referenced.add(value["datasetName"])
                for child in value.values():
                    collect(child)
            elif isinstance(value, list):
                for child in value:
                    collect(child)

        collect(dashboard["pages"])
        self.assertEqual(set(), referenced - dataset_names)
        self.assertNotIn("rules_analysis", dataset_names)


class ExtractionContractTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        path = ROOT / "src/01_extracao.py"
        spec = importlib.util.spec_from_file_location("extracao", path)
        cls.extracao = importlib.util.module_from_spec(spec)
        assert spec.loader is not None
        spec.loader.exec_module(cls.extracao)

    def test_month_window_crosses_year_boundary(self):
        self.assertEqual(
            ["2023-11", "2023-12", "2024-01", "2024-02"],
            list(self.extracao.meses_no_intervalo("2023-11", "2024-02")),
        )

    def test_previous_month_crosses_year_boundary(self):
        self.assertEqual(
            "2025-12",
            self.extracao.mes_anterior(self.extracao.date(2026, 1, 10)),
        )


class QualityContractTest(unittest.TestCase):
    def test_rule_ids_are_unique_and_required_fields_exist(self):
        contract = yaml.safe_load((ROOT / "src/quality/rules.yml").read_text())
        rules = contract["rules"]
        ids = [rule["id"] for rule in rules]

        self.assertEqual(len(ids), len(set(ids)))
        for rule in rules:
            with self.subTest(rule=rule["id"]):
                self.assertTrue(
                    {"id", "name", "dimension", "description", "expression", "layers"}
                    <= rule.keys()
                )
                self.assertTrue(rule["layers"])

    def test_case_rules_resolve_to_warn(self):
        contract = yaml.safe_load((ROOT / "src/quality/rules.yml").read_text())
        default_severity = contract["defaults"]["severity"]
        severities = {
            rule.get("severity", default_severity)
            for rule in contract["rules"]
        }

        self.assertEqual({"WARN"}, severities)


if __name__ == "__main__":
    unittest.main()
