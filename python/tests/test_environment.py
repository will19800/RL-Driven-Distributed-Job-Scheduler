"""Determinism and lifecycle tests for the training environment."""

from __future__ import annotations

import copy
import unittest

from scheduler_rl.environment import Episode, EnvironmentError, SyntheticEpisodeConfig, SyntheticEpisodeFactory, TrainingEnvironment


RESOURCES = ("cpu_millis", "memory_mib", "gpus", "ephemeral_disk_mib")


def resources(cpu: int) -> dict[str, int]:
    return {"cpu_millis": cpu, "memory_mib": 0, "gpus": 0, "ephemeral_disk_mib": 0}


def policy_config() -> dict[str, object]:
    return {"max_jobs": 4, "max_nodes": 2, "normalization": {"cpu_millis": 4000, "memory_mib": 8000, "gpus": 2, "ephemeral_disk_mib": 2000, "duration_seconds": 100, "priority": 10, "queue_length": 10, "arrival_rate": 10, "simulation_time_seconds": 100}}


def job(identifier: str, submit: float, duration: float, cpu: int = 1000) -> dict[str, object]:
    return {"id": identifier, "submit_time_seconds": submit, "request": resources(cpu), "duration_seconds": duration, "priority": 0, "deadline_seconds": None, "state": "queued"}


def episode(jobs: list[dict[str, object]], capacity: int = 1000) -> Episode:
    return Episode([{"id": "node-1", "capacity": resources(capacity), "allocated": resources(0), "state": "available"}], jobs)


class TrainingEnvironmentTest(unittest.TestCase):
    def test_reset_and_step_are_replay_deterministic(self) -> None:
        synthetic = SyntheticEpisodeFactory(SyntheticEpisodeConfig(2, resources(2000), 8, 2.0, 1.0, 3.0, resources(1000)))
        first = TrainingEnvironment(policy_config(), synthetic)
        second = TrainingEnvironment(policy_config(), synthetic)
        first_observation, first_info = first.reset(seed=77)
        second_observation, second_info = second.reset(seed=77)
        self.assertEqual((first_observation, first_info), (second_observation, second_info))
        self.assertNotEqual(first_observation, TrainingEnvironment(policy_config(), synthetic).reset(seed=78)[0])

        while True:
            action = next(index for index, allowed in enumerate(first_observation["action_mask"][:-1]) if allowed)
            first_result = first.step(action)
            second_result = second.step(action)
            self.assertEqual(first_result, second_result)
            first_observation = first_result[0]
            if first_result[2] or first_result[3]:
                self.assertTrue(first_result[2]); self.assertFalse(first_result[3]); break
        self.assertEqual(first_result[4]["completed_jobs"], 8)

    def test_same_time_events_and_capacity_advance_match_simulator_semantics(self) -> None:
        environment = TrainingEnvironment(policy_config(), lambda _: episode([job("job-b", 1, 2), job("job-a", 1, 3)]))
        observation, info = environment.reset(seed=1)
        self.assertEqual(info["simulation_time_seconds"], 1)
        self.assertEqual(observation["job_ids"][:2], ["job-a", "job-b"])

        observation, reward, terminated, truncated, info = environment.step(0)
        self.assertEqual(reward, 0.0)
        self.assertFalse(terminated or truncated)
        self.assertEqual(info["simulation_time_seconds"], 4)
        self.assertEqual(observation["job_ids"][0], "job-b")

        _, _, terminated, truncated, info = environment.step(0)
        self.assertTrue(terminated); self.assertFalse(truncated)
        self.assertEqual(info["completed_jobs"], 2)
        self.assertEqual(info["simulation_time_seconds"], 6)

    def test_defer_advances_and_unschedulable_queue_truncates(self) -> None:
        environment = TrainingEnvironment(policy_config(), lambda _: episode([job("one", 1, 2), job("two", 5, 2)]))
        observation, _ = environment.reset(seed=0)
        defer = len(observation["action_mask"]) - 1
        observation, _, terminated, truncated, info = environment.step(defer)
        self.assertFalse(terminated or truncated)
        self.assertEqual(info["simulation_time_seconds"], 5)
        self.assertEqual(observation["job_mask"].count(True), 2)

        blocked = TrainingEnvironment(policy_config(), lambda _: episode([job("large", 0, 1, 2000)], capacity=1000))
        observation, _ = blocked.reset(seed=0)
        _, _, terminated, truncated, info = blocked.step(len(observation["action_mask"]) - 1)
        self.assertFalse(terminated); self.assertTrue(truncated)
        self.assertEqual(info["truncation_reason"], "unschedulable_queue")

    def test_rejects_invalid_actions_and_episode_aliasing(self) -> None:
        source = episode([job("one", 0, 1)])
        environment = TrainingEnvironment(policy_config(), lambda _: source)
        observation, _ = environment.reset(seed=2)
        source.nodes[0]["capacity"]["cpu_millis"] = 0
        self.assertEqual(observation["node_features"][0][4], 0.25)
        with self.assertRaises(EnvironmentError):
            environment.step(1)  # padded node action

        duplicate = copy.deepcopy(source)
        duplicate.nodes.append(copy.deepcopy(duplicate.nodes[0]))
        with self.assertRaisesRegex(EnvironmentError, "duplicate node"):
            TrainingEnvironment(policy_config(), lambda _: duplicate).reset(seed=0)


if __name__ == "__main__":
    unittest.main()
