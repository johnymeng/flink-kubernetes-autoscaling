"""Exercise the public manifest renderer for the continuous Q8 profile."""
import os
from pathlib import Path
import subprocess
import tempfile
import unittest

HERE = Path(__file__).resolve().parent
REPO = HERE.parents[1]


class ContinuousQ8Test(unittest.TestCase):
    def test_default_images_use_main_sources_and_revision(self):
        revision = subprocess.check_output(["git", "rev-parse", "--short", "HEAD"],
                                           cwd=REPO, text=True).strip()
        command = (
            'source experiments/1729-checkpoint-aware-rescaling/run-env.sh >/dev/null && '
            'printf "%s\\n" "$FLINK_RUNTIME_SOURCE_DIR" "$OPERATOR_SOURCE_DIR" '
            '"$FLINK_RUNTIME_IMAGE_TAG" "$FLINK_BENCHMARK_IMAGE_TAG" "$OPERATOR_IMAGE_TAG"'
        )
        values = subprocess.check_output(
            ["bash", "-c", command], cwd=REPO, text=True,
            env={"PATH": os.environ["PATH"], "EXPERIMENT_PROFILE": "continuous",
                 "QUERY": "q8", "RUN_ID": "sources"},
        ).splitlines()
        self.assertEqual(values, ["sources/flink/flink-runtime-1-18-a4s",
                                  "sources/operators/flink-kubernetes-operator-a4s",
                                  *([f"q8-{revision}"] * 3)])

    def test_policies_use_one_continuous_workload(self):
        manifests = []
        for policy, justin, a4s in [
            ("ds2", "false", "false"),
            ("justin", "true", "false"),
            ("a4s", "true", "true"),
        ]:
            env = {
                "PATH": os.environ["PATH"],
                "EXPERIMENT_PROFILE": "continuous",
                "QUERY": "q8",
                "POLICY": policy,
                "RUN_ID": "comparison",
                "KAFKA_BOOTSTRAP": "q8-kafka:19092",
                "FLINK_BENCHMARK_IMAGE_TAG": "tested",
            }
            with tempfile.TemporaryDirectory() as directory:
                output = Path(directory) / "q8.yaml"
                subprocess.run(["bash", HERE / "render-job.sh", output], cwd=REPO,
                               env=env, capture_output=True, check=True)
                rendered = output.read_text()
            for key, expected in (
                ("job.autoscaler.justin.enabled", justin),
                ("job.autoscaler.a4s.enabled", a4s),
                ("state.backend.rocksdb.metrics.stack-distance-histogram", a4s),
            ):
                setting = f"    {key}: '{expected}'\n"
                self.assertEqual(rendered.count(setting), 1)
                rendered = rendered.replace(setting, "")
            self.assertIn("job.autoscaler.checkpoint-rescale.producer-pause.enabled: 'false'", rendered)
            self.assertIn("execution.checkpointing.interval: 10s", rendered)
            self.assertIn("flink-benchmark-runtime:tested\n", rendered)
            self.assertIn("    - --query\n    - q8_unique\n", rendered)
            self.assertIn("    - --bootstrap-servers\n    - q8-kafka:19092\n", rendered)
            job_name = f"q8_unique-continuous-{policy}-comparison"
            self.assertIn("    - --job-name\n    - " + job_name + "\n", rendered)
            manifests.append(rendered.replace(job_name, "comparison"))
        self.assertEqual(manifests[0], manifests[1])
        self.assertEqual(manifests[1], manifests[2])


if __name__ == "__main__":
    unittest.main()
