import json
from pathlib import Path

import yaml

ROOT = Path(__file__).resolve().parents[1]


def test_logging_services_are_optional_and_storage_is_bounded():
    compose = yaml.safe_load((ROOT / "docker-compose.yml").read_text(encoding="utf-8"))
    assert compose["include"][0]["path"] == "./observability/compose.yaml"
    logging_compose = yaml.safe_load(
        (ROOT / "observability/compose.yaml").read_text(encoding="utf-8")
    )
    services = logging_compose["services"]
    for name in ("loki", "alloy", "vector"):
        assert set(services[name]["profiles"]) == {"logs", "observability"}
        assert services[name]["mem_limit"]
        assert ":latest" not in services[name]["image"]
    for name in ("app", "worker", "producer"):
        assert not (
            {"loki", "alloy", "vector"}
            & set(compose["services"][name].get("depends_on", {}))
        )
    assert "load" in compose["services"]["producer"]["profiles"]
    assert services["loki"]["healthcheck"]
    assert (
        "/var/run/docker.sock:/var/run/docker.sock:ro" in services["alloy"]["volumes"]
    )
    assert (
        "/var/run/docker.sock:/var/run/docker.sock:ro" in services["vector"]["volumes"]
    )
    loki = yaml.safe_load(
        (ROOT / "observability/loki.yaml").read_text(encoding="utf-8")
    )
    assert loki["limits_config"]["retention_period"] == "48h"
    assert loki["limits_config"]["discover_service_name"] == []
    assert loki["limits_config"]["discover_log_levels"] is False
    assert loki["compactor"]["retention_enabled"] is True


def test_agent_restricts_project_services_and_label_set():
    config = (ROOT / "observability/alloy.alloy").read_text(encoding="utf-8")
    assert '"com.docker.compose.project=" + sys.env("COMPOSE_PROJECT_NAME")' in config
    assert 'regex = "app|worker"' in config
    assert 'values = ["service", "level", "env"]' in config
    vector = (ROOT / "observability/vector.yaml").read_text(encoding="utf-8")
    assert "com.docker.compose.project=${COMPOSE_PROJECT_NAME}" in vector
    assert '.label."com.docker.compose.service" != "producer"' in vector


def test_metrics_store_and_grafana_are_provisioned_and_bounded():
    compose = yaml.safe_load(
        (ROOT / "observability/compose.yaml").read_text(encoding="utf-8")
    )
    services = compose["services"]
    for name in ("victoriametrics", "grafana"):
        assert services[name]["mem_limit"], f"{name} has no mem_limit"
        assert ":latest" not in services[name]["image"], f"{name} uses :latest tag"
        assert services[name]["healthcheck"], f"{name} has no healthcheck"

    assert "metrics" in services["victoriametrics"]["profiles"]
    assert "observability" in services["grafana"]["profiles"]

    base = yaml.safe_load((ROOT / "docker-compose.yml").read_text(encoding="utf-8"))
    for name in ("app", "worker", "producer"):
        deps = set(base["services"][name].get("depends_on", {}).keys())
        assert not ({"victoriametrics", "grafana", "otel-collector"} & deps), (
            f"{name} depends on observability stack"
        )


def test_grafana_provisioning_files_exist_and_alerts_have_for():
    ds = yaml.safe_load(
        (
            ROOT / "observability/grafana/provisioning/datasources/datasources.yaml"
        ).read_text(encoding="utf-8")
    )
    uids = {d["uid"] for d in ds["datasources"]}
    assert {"vm-ds", "loki-ds", "tempo-ds"} <= uids, "Missing datasource UIDs"

    rules = yaml.safe_load(
        (ROOT / "observability/grafana/provisioning/alerting/rules.yaml").read_text(
            encoding="utf-8"
        )
    )
    for group in rules["groups"]:
        for rule in group["rules"]:
            assert rule.get("for"), f"Alert '{rule['title']}' has no 'for' duration"

    dash = json.loads(
        (ROOT / "observability/grafana/dashboards/pipeline.json").read_text(
            encoding="utf-8"
        )
    )
    titles = " ".join(p["title"] for p in dash["panels"])
    for needle in ("очереди", "p50", "Память", "503", "DLQ", "Loki"):
        assert needle in titles, f"Dashboard missing panel with '{needle}'"

    assert any(v["name"] == "worker" for v in dash["templating"]["list"]), (
        "Dashboard missing 'worker' variable"
    )
