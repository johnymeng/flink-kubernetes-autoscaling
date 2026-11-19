"""The producer resets once on a real Q8 JobID replacement and ignores REST gaps."""
from concurrent.futures import ThreadPoolExecutor
from io import BytesIO
import json
from unittest import TestCase
from unittest.mock import patch

from kafka_control import change_reason, read_job


class RecoverConsumer(TestCase):
    def test_replacement_id_requires_a_uniquely_matching_live_job(self):
        initial = {"job_id": "old", "created": 1, "job_start": 1, "parallelism": {}, "tasks": {}, "state": "RUNNING"}
        old = {"jid": "old", "state": "CANCELED", "name": "q8-test"}
        replacement = {"jid": "new", "state": "INITIALIZING", "name": "q8-test", "vertices": []}
        other = {"jid": "unrelated", "state": "RUNNING", "name": "other-job"}
        def reply(value):
            return BytesIO(json.dumps(value).encode())
        with ThreadPoolExecutor(1) as pool:
            with patch("kafka_control.urlopen", return_value=reply({"jobs": [old, other]})):
                with self.assertRaises(ValueError):
                    read_job("http://localhost", "old", pool, "q8-test")
            with patch("kafka_control.urlopen", side_effect=[reply({"jobs": [old, other, replacement]}),
                                                            reply(replacement)]) as get:
                current = read_job("http://localhost", "old", pool, "q8-test")
            self.assertEqual(get.call_args.args[0], "http://localhost/jobs/new")
            self.assertEqual(change_reason(initial, current), "job_id")
            self.assertIsNone(change_reason(current, current))
