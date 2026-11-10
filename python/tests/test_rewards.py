"""Reward accounting must penalize waiting and preserve invalid-action state."""
import unittest
from scheduler_rl.environment import TrainingEnvironment, EnvironmentError
from scheduler_rl.reward import RewardConfig
from test_environment import episode, job, policy_config


class RewardTest(unittest.TestCase):
    def test_queue_wait_integrates_across_defer(self):
        env = TrainingEnvironment(policy_config(), lambda _: episode([job("first", 0, 2), job("later", 5, 2)]))
        obs, _ = env.reset(seed=1)
        _, reward, _, _, info = env.step(len(obs["action_mask"]) - 1)
        self.assertEqual(info["reward_terms"]["queue"], -0.5)
        self.assertLess(reward, 0)

    def test_completion_and_deadline_terms(self):
        item = job("first", 0, 2)
        item["deadline_seconds"] = 1
        env = TrainingEnvironment(policy_config(), lambda _: episode([item]))
        env.reset(seed=1)
        _, _, terminated, _, info = env.step(0)
        self.assertTrue(terminated)
        self.assertEqual(info["reward_terms"]["throughput"], 1)
        self.assertEqual(info["reward_terms"]["sla"], -1)

    def test_invalid_action_does_not_allocate(self):
        env = TrainingEnvironment(policy_config(), lambda _: episode([job("first", 0, 2)]))
        env.reset(seed=1)
        with self.assertRaises(EnvironmentError):
            env.step(1)
        self.assertEqual(env.reward_totals["invalid"], -1)
        self.assertEqual(env.nodes[0]["allocated"]["cpu_millis"], 0)
        self.assertEqual(env.steps, 0)

    def test_rejects_nonfinite_weight(self):
        with self.assertRaises(ValueError):
            RewardConfig(queue=float("nan")).validate()
