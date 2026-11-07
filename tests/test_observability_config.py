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
