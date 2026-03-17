import json
from pathlib import Path
from unittest.mock import MagicMock

import pytest
from typer.testing import CliRunner

import cloud_ru_mls


class FakeResponse:
    def __init__(self, payload, status_code=200):
        self._payload = payload
        self.status_code = status_code

    def raise_for_status(self):
        if self.status_code >= 400:
            raise RuntimeError(f"HTTP {self.status_code}")

    def json(self):
        return self._payload


@pytest.fixture
def runner():
    return CliRunner()


def test_get_workspace_returns_first_when_workspace_not_specified():
    config = {
        "workspaces": {
            "ws-a": {"x-workspace-id": "1", "x-api-key": "k1"},
            "ws-b": {"x-workspace-id": "2", "x-api-key": "k2"},
        }
    }

    name, ws = cloud_ru_mls.get_workspace(config)

    assert name == "ws-a"
    assert ws["x-workspace-id"] == "1"


def test_get_workspace_returns_named_workspace():
    config = {
        "workspaces": {
            "ws-a": {"x-workspace-id": "1", "x-api-key": "k1"},
            "ws-b": {"x-workspace-id": "2", "x-api-key": "k2"},
        }
    }

    name, ws = cloud_ru_mls.get_workspace(config, "ws-b")

    assert name == "ws-b"
    assert ws["x-api-key"] == "k2"


def test_get_workspace_raises_when_not_found():
    config = {"workspaces": {"ws-a": {"x-workspace-id": "1", "x-api-key": "k1"}}}

    with pytest.raises(ValueError, match="Workspace 'ws-z' not found"):
        cloud_ru_mls.get_workspace(config, "ws-z")


def test_load_config_reads_file(tmp_path, monkeypatch):
    cfg_file = tmp_path / "credentials.json"
    cfg = {"auth": {"client_id": "cid", "client_secret": "sec"}, "workspaces": {}}
    cfg_file.write_text(json.dumps(cfg), encoding="utf-8")

    monkeypatch.setattr(cloud_ru_mls, "CONFIG_PATH", str(cfg_file))

    loaded = cloud_ru_mls.load_config()

    assert loaded == cfg


def test_load_config_raises_exit_when_missing(monkeypatch):
    monkeypatch.setattr(cloud_ru_mls.os.path, "exists", lambda _: False)
    printer = MagicMock()
    monkeypatch.setattr(cloud_ru_mls, "rprint", printer)

    with pytest.raises(cloud_ru_mls.typer.Exit):
        cloud_ru_mls.load_config()

    printer.assert_called_once()


def test_authenticate_returns_access_token(monkeypatch):
    mocked_post = MagicMock(return_value=FakeResponse({"token": {"access_token": "abc"}}))
    monkeypatch.setattr(cloud_ru_mls.requests, "post", mocked_post)

    token = cloud_ru_mls.authenticate(
        {"x-api-key": "key1"},
        {"client_id": "cid", "client_secret": "secret"},
    )

    assert token == "abc"
    call = mocked_post.call_args
    assert call.kwargs["headers"]["x-api-key"] == "key1"
    assert call.kwargs["json"]["client_id"] == "cid"


def test_authenticate_raises_when_token_missing(monkeypatch):
    monkeypatch.setattr(cloud_ru_mls.requests, "post", MagicMock(return_value=FakeResponse({"oops": 1})))

    with pytest.raises(RuntimeError, match="Authentication failed"):
        cloud_ru_mls.authenticate(
            {"x-api-key": "key1"},
            {"client_id": "cid", "client_secret": "secret"},
        )


def test_init_headers_builds_auth_headers(monkeypatch):
    monkeypatch.setattr(
        cloud_ru_mls,
        "load_config",
        lambda: {
            "auth": {"client_id": "cid", "client_secret": "sec"},
            "workspaces": {"ws": {"x-workspace-id": "wid", "x-api-key": "k"}},
        },
    )
    monkeypatch.setattr(cloud_ru_mls, "authenticate", lambda ws, auth: "TOKEN")
    printer = MagicMock()
    monkeypatch.setattr(cloud_ru_mls, "rprint", printer)

    headers = cloud_ru_mls.init_headers("ws", verbose=True)

    assert headers == {
        "authorization": "TOKEN",
        "x-workspace-id": "wid",
        "x-api-key": "k",
    }
    printer.assert_called_once()


def test_nb_list_cli_outputs_count_and_total_gpu(runner, monkeypatch):
    monkeypatch.setattr(cloud_ru_mls, "init_headers", lambda workspace: {"auth": "x"})
    monkeypatch.setattr(
        cloud_ru_mls,
        "list_notebooks",
        lambda headers: [
            {
                "name": "nb1",
                "author": "alice",
                "ageSeconds": 60,
                "notebookType": "gpu_2",
                "region": "SR008",
                "description": "main",
                "status": "running",
            },
            {
                "name": "nb2",
                "author": "bob",
                "ageSeconds": 10,
                "notebookType": "gpu_1",
                "region": "SR008",
                "description": "stopped",
                "status": "stopped",
            },
        ],
    )
    monkeypatch.setattr(cloud_ru_mls, "tabulate", lambda rows, **_: f"rows={len(rows)}")

    result = runner.invoke(cloud_ru_mls.app, ["nb-list"])

    assert result.exit_code == 0
    assert "Found 1 running notebook(s):" in result.stdout
    assert "rows=1" in result.stdout
    assert "Total GPU used: 2" in result.stdout


def test_job_list_cli_outputs_count_and_total_gpu(runner, monkeypatch):
    monkeypatch.setattr(cloud_ru_mls, "init_headers", lambda workspace: {"auth": "x"})
    monkeypatch.setattr(
        cloud_ru_mls,
        "list_jobs",
        lambda headers, region: [
            {"job_desc": "train-a", "gpu_count": 2, "duration": "120s"},
            {"job_desc": "train-b", "gpu_count": 1, "duration": "30s"},
        ],
    )
    monkeypatch.setattr(cloud_ru_mls, "tabulate", lambda rows, **_: f"rows={len(rows)}")

    result = runner.invoke(cloud_ru_mls.app, ["job-list", "--region", "SR008"])

    assert result.exit_code == 0
    assert "Found 2 running job(s):" in result.stdout
    assert "rows=2" in result.stdout
    assert "Total GPU used: 3" in result.stdout


def test_gpu_stat_cli_outputs_totals(runner, monkeypatch):
    monkeypatch.setattr(cloud_ru_mls, "init_headers", lambda workspace: {"auth": "x"})
    monkeypatch.setattr(
        cloud_ru_mls,
        "get_ws_allocactions",
        lambda headers: [
            {"id": "a1", "region_key": "SR008"},
            {"id": "a2", "region_key": "SR009"},
        ],
    )
    monkeypatch.setattr(
        cloud_ru_mls,
        "get_allocation_resources",
        lambda headers, alloc: {"gpu": {"current": 8, "available": 3}},
    )
    monkeypatch.setattr(
        cloud_ru_mls,
        "list_notebooks",
        lambda headers: [
            {"status": "running", "region": "SR008", "notebookType": "gpu_2"},
            {"status": "running", "region": "SR009", "notebookType": "gpu_4"},
        ],
    )
    monkeypatch.setattr(cloud_ru_mls, "list_jobs", lambda headers, region: [{"gpu_count": 4}])
    monkeypatch.setattr(cloud_ru_mls, "rprint", print)

    result = runner.invoke(cloud_ru_mls.app, ["gpu-stat", "--region", "SR008"])

    assert result.exit_code == 0
    assert "GPUs: 3/8" in result.stdout
    assert "Notebooks GPUs: 2, jobs GPUs: 4" in result.stdout


def test_nb_ssh_conf_prints_only_running_notebooks(runner, monkeypatch):
    monkeypatch.setattr(cloud_ru_mls, "init_headers", lambda workspace, verbose=False: {"auth": "x"})
    monkeypatch.setattr(cloud_ru_mls, "get_namespace", lambda headers: "ns1")
    monkeypatch.setattr(
        cloud_ru_mls,
        "list_notebooks",
        lambda headers: [
            {"name": "nb-run", "region": "SR008", "status": "running"},
            {"name": "nb-stop", "region": "SR008", "status": "stopped"},
        ],
    )

    result = runner.invoke(cloud_ru_mls.app, ["nb-ssh-conf"])

    assert result.exit_code == 0
    assert "Host mlspace-nb-run" in result.stdout
    assert "HostName ssh-sr008-jupyter.ai.cloud.ru" in result.stdout
    assert "User nb-run.ns1" in result.stdout
    assert "Host mlspace-nb-stop" not in result.stdout


def test_list_helpers_use_expected_request_params(monkeypatch):
    called = {}

    def fake_get(url, headers=None, params=None):
        called["url"] = url
        called["headers"] = headers
        called["params"] = params
        return FakeResponse({"jobs": [{"id": "j1"}]})

    monkeypatch.setattr(cloud_ru_mls.requests, "get", fake_get)

    jobs = cloud_ru_mls.list_jobs({"authorization": "token"}, "SR008")

    assert jobs == [{"id": "j1"}]
    assert called["url"].endswith("/jobs")
    assert called["params"] == {"status": "Running", "region": "SR008"}


def test_get_workspace_raises_when_workspaces_missing():
    with pytest.raises(ValueError, match="No workspaces found"):
        cloud_ru_mls.get_workspace({}, None)


def test_init_headers_no_verbose_does_not_print(monkeypatch):
    monkeypatch.setattr(
        cloud_ru_mls,
        "load_config",
        lambda: {
            "auth": {"client_id": "cid", "client_secret": "sec"},
            "workspaces": {"ws": {"x-workspace-id": "wid", "x-api-key": "k"}},
        },
    )
    monkeypatch.setattr(cloud_ru_mls, "authenticate", lambda ws, auth: "TOKEN")
    printer = MagicMock()
    monkeypatch.setattr(cloud_ru_mls, "rprint", printer)

    headers = cloud_ru_mls.init_headers("ws", verbose=False)

    assert headers["authorization"] == "TOKEN"
    printer.assert_not_called()


def test_list_notebooks_returns_empty_by_default(monkeypatch):
    monkeypatch.setattr(
        cloud_ru_mls.requests,
        "get",
        lambda url, headers=None: FakeResponse({}),
    )

    notebooks = cloud_ru_mls.list_notebooks({"authorization": "t"})

    assert notebooks == []


def test_get_namespace_returns_default_when_missing(monkeypatch):
    monkeypatch.setattr(
        cloud_ru_mls.requests,
        "get",
        lambda url, headers=None: FakeResponse({}),
    )

    ns = cloud_ru_mls.get_namespace({"x-workspace-id": "wid"})

    assert ns == "N/A"


def test_nb_list_cli_with_description_and_multiple_regions_keeps_columns(runner, monkeypatch):
    monkeypatch.setattr(cloud_ru_mls, "init_headers", lambda workspace: {"auth": "x"})
    monkeypatch.setattr(
        cloud_ru_mls,
        "list_notebooks",
        lambda headers: [
            {
                "name": "a",
                "author": "alice",
                "ageSeconds": 60,
                "notebookType": "gpu_2",
                "region": "SR008",
                "description": "desc-a",
                "status": "running",
            },
            {
                "name": "b",
                "author": "bob",
                "ageSeconds": 90,
                "notebookType": "gpu_1",
                "region": "SR009",
                "description": "desc-b",
                "status": "running",
            },
        ],
    )
    captured = {}

    def fake_tabulate(rows, headers="keys", floatfmt=".0f"):
        del headers, floatfmt
        captured["rows"] = rows
        return "TABLE"

    monkeypatch.setattr(cloud_ru_mls, "tabulate", fake_tabulate)

    result = runner.invoke(cloud_ru_mls.app, ["nb-list", "--description"])

    assert result.exit_code == 0
    assert "TABLE" in result.stdout
    assert len(captured["rows"]) == 2
    assert "Description" in captured["rows"][0]
    assert "Region" in captured["rows"][0]
    assert "Total GPU used: 3" in result.stdout


def test_main_invokes_app(monkeypatch):
    called = MagicMock()
    monkeypatch.setattr(cloud_ru_mls, "app", called)

    cloud_ru_mls.main()

    called.assert_called_once_with()
