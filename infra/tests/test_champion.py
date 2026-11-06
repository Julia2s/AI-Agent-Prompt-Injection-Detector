from types import SimpleNamespace

import pytest
from mlflow.exceptions import MlflowException
from mlflow.protos.databricks_pb2 import RESOURCE_DOES_NOT_EXIST

from mlops.train import promote


class Client:
    def __init__(self, old, new, absent=False):
        self.old, self.new, self.absent = old, new, absent
        self.alias = None

    def get_run(self, run_id):
        return SimpleNamespace(data=self.new if run_id == "new" else self.old)

    def get_model_version_by_alias(self, name, alias):
        if self.absent:
            raise MlflowException("missing", error_code=RESOURCE_DOES_NOT_EXIST)
        return SimpleNamespace(run_id="old", version="1")

    def set_registered_model_alias(self, name, alias, version):
        self.alias = version


@pytest.mark.parametrize(
    "old_score,new_score,old_hash,new_hash,expected",
    [
        (0.8, 0.9, "a", "a", True),
        (0.9, 0.8, "a", "a", False),
        (0.8, 0.8, "a", "a", False),
        (None, 0.8, "a", "a", False),
        (0.7, 0.8, "a", "b", False),
    ],
)
def test_champion_policy(old_score, new_score, old_hash, new_hash, expected):
    def data(score, digest):
        return SimpleNamespace(
            metrics={} if score is None else {"f1_score": score},
            tags={"validation_sha256": digest, "evaluation_method": "v1"},
        )

    client = Client(data(old_score, old_hash), data(new_score, new_hash))
    assert promote(client, "2", "new") is expected
    assert (client.alias == "2") is expected


def test_first_champion():
    data = SimpleNamespace(metrics={}, tags={})
    client = Client(data, data, absent=True)
    assert promote(client, "2", "new")
