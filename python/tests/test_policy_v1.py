"""Cross-language parity and validation tests for policy/v1."""

from __future__ import annotations

import json
import unittest
from pathlib import Path

from scheduler_rl.policy_v1 import ContractError, decode_action, encode, schema


FIXTURE = Path(__file__).resolve().parents[2] / "internal" / "policyv1" / "testdata" / "parity.json"


class PolicyV1Test(unittest.TestCase):
    def setUp(self) -> None:
        self.fixture = json.loads(FIXTURE.read_text(encoding="utf-8"))

    def test_encoder_matches_shared_go_fixture(self) -> None:
        self.assertEqual(schema(self.fixture["config"]), self.fixture["expected_schema"])
        self.assertEqual(encode(self.fixture["config"], self.fixture["state"]), self.fixture["expected"])

    def test_schema_and_action_mapping(self) -> None:
        contract = schema(self.fixture["config"])
        self.assertEqual(contract["version"], "policy/v1")
        self.assertEqual(contract["defer_action"], 9)
        observation = self.fixture["expected"]
        self.assertEqual(decode_action(self.fixture["config"], observation, 1), {"defer": False, "job_index": 0, "node_index": 1})
        self.assertEqual(decode_action(self.fixture["config"], observation, 9), {"defer": True, "job_index": -1, "node_index": -1})
        with self.assertRaisesRegex(ContractError, "masked"):
            decode_action(self.fixture["config"], observation, 0)

    def test_rejects_unknown_and_nonfinite_inputs(self) -> None:
        bad_config = dict(self.fixture["config"], mystery=True)
        with self.assertRaises(ContractError):
            schema(bad_config)
        bad_state = dict(self.fixture["state"], arrival_rate=float("nan"))
        with self.assertRaises(ContractError):
            encode(self.fixture["config"], bad_state)
        bad_node_state = dict(self.fixture["state"])
        bad_node_state["nodes"] = [dict(self.fixture["state"]["nodes"][0], mystery=True)]
        with self.assertRaises(ContractError):
            encode(self.fixture["config"], bad_node_state)


if __name__ == "__main__":
    unittest.main()
