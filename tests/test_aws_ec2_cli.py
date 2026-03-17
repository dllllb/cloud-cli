import io
import json
from pathlib import Path
from unittest.mock import MagicMock

import pytest
from typer.testing import CliRunner

import aws_ec2_cli


class FakeInstance:
    def __init__(
        self,
        instance_id,
        tags=None,
        public_dns_name="",
        private_dns_name="",
        lifecycle=None,
        statuses=None,
    ):
        self.id = instance_id
        self.tags = tags or {}
        self.public_dns_name = public_dns_name
        self.private_dns_name = private_dns_name
        if lifecycle is not None:
            self.instanceLifecycle = lifecycle
        self._statuses = list(statuses or [])
        self.start = MagicMock()
        self.stop = MagicMock()

    def update(self):
        if self._statuses:
            return self._statuses.pop(0)
        return "running"


class FakeEc2Conn:
    def __init__(self, instances):
        self.instances = instances
        self.get_only_instances = MagicMock(return_value=instances)


@pytest.fixture
def runner():
    return CliRunner()


def test_get_ec2_connection_uses_default_region(monkeypatch):
    conn = object()
    monkeypatch.setenv("AWS_DEFAULT_REGION", "us-east-1")
    mocked = MagicMock(return_value=conn)
    monkeypatch.setattr(aws_ec2_cli.boto.ec2, "connect_to_region", mocked)

    got_conn, region = aws_ec2_cli.get_ec2_connection(None)

    assert got_conn is conn
    assert region == "us-east-1"
    mocked.assert_called_once_with("us-east-1")


def test_get_ec2_connection_raises_when_connect_fails(monkeypatch):
    monkeypatch.setattr(aws_ec2_cli.boto.ec2, "connect_to_region", MagicMock(return_value=None))

    with pytest.raises(aws_ec2_cli.typer.BadParameter):
        aws_ec2_cli.get_ec2_connection("eu-west-1")


def test_parse_ssh_config_and_write_roundtrip(tmp_path):
    config_file = tmp_path / "config"
    config_file.write_text(
        "Host web i-123\n"
        "HostName web.example.com\n"
        "IdentityFile ~/.ssh/id_rsa\n"
        "IdentityFile ~/.ssh/id_rsa2\n\n"
        "Host db\n"
        "HostName db.example.com\n",
        encoding="utf-8",
    )

    parsed = aws_ec2_cli.parse_ssh_config(str(config_file))

    assert len(parsed) == 2
    assert parsed[0]["host"] == ["web", "i-123"]
    assert parsed[0]["config"]["IdentityFile"] == ["~/.ssh/id_rsa", "~/.ssh/id_rsa2"]

    output = io.StringIO()
    aws_ec2_cli.write_ssh_config(output, parsed)
    text = output.getvalue()
    assert "Host web i-123" in text
    assert "HostName web.example.com" in text
    assert "IdentityFile ~/.ssh/id_rsa2" in text


def test_wait_for_status_success(monkeypatch):
    inst = FakeInstance("i-1", statuses=["pending", "pending", "running"])
    sleep = MagicMock()
    monkeypatch.setattr(aws_ec2_cli.time, "sleep", sleep)

    aws_ec2_cli.wait_for_status(inst, "pending", "running", interval_sec=1)

    assert sleep.call_count == 2


def test_wait_for_status_raises_on_unexpected_state(monkeypatch):
    inst = FakeInstance("i-1", statuses=["pending", "stopped"])
    monkeypatch.setattr(aws_ec2_cli.time, "sleep", MagicMock())

    with pytest.raises(RuntimeError, match='incorrect status: "stopped"'):
        aws_ec2_cli.wait_for_status(inst, "pending", "running", interval_sec=1)


def test_get_instance_by_name_success():
    conn = FakeEc2Conn([FakeInstance("i-1")])

    inst = aws_ec2_cli.get_instance_by_name(conn, "node-a")

    assert inst.id == "i-1"
    conn.get_only_instances.assert_called_once_with(filters={"tag:Name": "node-a"})


def test_get_instance_by_name_raises_when_none():
    conn = FakeEc2Conn([])

    with pytest.raises(RuntimeError, match='no instances with name "node-a"'):
        aws_ec2_cli.get_instance_by_name(conn, "node-a")


def test_get_instance_by_name_raises_when_multiple():
    conn = FakeEc2Conn([FakeInstance("i-1"), FakeInstance("i-2")])

    with pytest.raises(RuntimeError, match='more than one instance with name "node-a"'):
        aws_ec2_cli.get_instance_by_name(conn, "node-a")


def test_ec2ansible_cli_outputs_inventory(runner, monkeypatch):
    instances = [
        FakeInstance(
            "i-1",
            tags={"Group": "app", "User": "ubuntu"},
            public_dns_name="host1.example",
        ),
        FakeInstance(
            "i-2",
            tags={"Group": "app"},
            private_dns_name="ip-10-0-0-2",
        ),
    ]
    monkeypatch.setattr(aws_ec2_cli, "get_ec2_connection", lambda region: (FakeEc2Conn(instances), "eu-west-1"))

    result = runner.invoke(aws_ec2_cli.app, ["ec2ansible"])

    assert result.exit_code == 0
    payload = json.loads(result.stdout)
    assert payload["app"] == ["host1.example", "ip-10-0-0-2"]
    assert payload["_meta"]["hostvars"]["host1.example"]["ansible_ssh_user"] == "ubuntu"
    assert payload["_meta"]["hostvars"]["host1.example"]["ec2_region"] == "eu-west-1"
    assert payload["i-1"] == ["host1.example"]


def test_ec2_ssh_config_sync_dump_filters_old_entries(runner, monkeypatch, tmp_path):
    instances = [
        FakeInstance(
            "i-new",
            tags={"Name": "Main Node", "User": "ec2-user"},
            public_dns_name="new.example",
        )
    ]
    monkeypatch.setattr(aws_ec2_cli, "get_ec2_connection", lambda region: (FakeEc2Conn(instances), "eu-west-1"))

    cfg = tmp_path / "ssh_config"
    cfg.write_text(
        "Host old i-old\nHostName old.example\n\n"
        "Host plain-host\nHostName plain.example\n",
        encoding="utf-8",
    )

    result = runner.invoke(
        aws_ec2_cli.app,
        ["ec2_ssh_config_sync", "--dump", "--config-path", str(cfg)],
    )

    assert result.exit_code == 0
    assert "Host main-node i-new" in result.stdout
    assert "HostName new.example" in result.stdout
    assert "User ec2-user" in result.stdout
    assert "Host old i-old" not in result.stdout
    assert "Host plain-host" in result.stdout


def test_ec2_ssh_config_sync_writes_file_when_not_dump(runner, monkeypatch, tmp_path):
    instances = [
        FakeInstance(
            "i-1",
            tags={"Name": "Node", "User": "ubuntu"},
            public_dns_name="node.example",
        )
    ]
    monkeypatch.setattr(aws_ec2_cli, "get_ec2_connection", lambda region: (FakeEc2Conn(instances), "eu-west-1"))

    cfg = tmp_path / "nested" / "config"
    result = runner.invoke(
        aws_ec2_cli.app,
        ["ec2_ssh_config_sync", "--create", "--config-path", str(cfg)],
    )

    assert result.exit_code == 0
    written = cfg.read_text(encoding="utf-8")
    assert "Host node i-1" in written
    assert "HostName node.example" in written
    assert "User ubuntu" in written


def test_ec2cmd_start_calls_start_path(runner, monkeypatch):
    inst = FakeInstance("i-1")
    conn = FakeEc2Conn([inst])
    monkeypatch.setattr(aws_ec2_cli, "get_ec2_connection", lambda region: (conn, "eu-west-1"))
    start = MagicMock()
    stop = MagicMock()
    monkeypatch.setattr(aws_ec2_cli, "start_instance", start)
    monkeypatch.setattr(aws_ec2_cli, "stop_instance", stop)

    result = runner.invoke(aws_ec2_cli.app, ["ec2cmd", "start", "node-a"])

    assert result.exit_code == 0
    start.assert_called_once_with(inst)
    stop.assert_not_called()


def test_ec2cmd_stop_calls_stop_path(runner, monkeypatch):
    inst = FakeInstance("i-1")
    conn = FakeEc2Conn([inst])
    monkeypatch.setattr(aws_ec2_cli, "get_ec2_connection", lambda region: (conn, "eu-west-1"))
    start = MagicMock()
    stop = MagicMock()
    monkeypatch.setattr(aws_ec2_cli, "start_instance", start)
    monkeypatch.setattr(aws_ec2_cli, "stop_instance", stop)

    result = runner.invoke(aws_ec2_cli.app, ["ec2cmd", "stop", "node-a"])

    assert result.exit_code == 0
    stop.assert_called_once_with(inst)
    start.assert_not_called()


def test_start_instance_and_stop_instance_emit_messages(monkeypatch, capsys):
    inst = FakeInstance("i-1")
    wait = MagicMock()
    monkeypatch.setattr(aws_ec2_cli, "wait_for_status", wait)

    aws_ec2_cli.start_instance(inst)
    aws_ec2_cli.stop_instance(inst)

    out = capsys.readouterr().out
    assert "Waiting for instance to start..." in out
    assert "Started" in out
    assert "Waiting for instance to stop..." in out
    assert "Stopped" in out
    inst.start.assert_called_once()
    inst.stop.assert_called_once()


def test_ec2_ssh_config_sync_skips_spot_instance_unless_with_spots(runner, monkeypatch, tmp_path):
    spot = FakeInstance(
        "i-spot",
        tags={"Name": "SpotNode", "User": "ubuntu"},
        public_dns_name="spot.example",
        lifecycle="spot",
    )
    monkeypatch.setattr(aws_ec2_cli, "get_ec2_connection", lambda region: (FakeEc2Conn([spot]), "eu-west-1"))

    cfg1 = tmp_path / "cfg1"
    result_no_spot = runner.invoke(
        aws_ec2_cli.app,
        ["ec2_ssh_config_sync", "--create", "--dump", "--config-path", str(cfg1)],
    )
    assert result_no_spot.exit_code == 0
    assert "i-spot" not in result_no_spot.stdout

    result_with_spot = runner.invoke(
        aws_ec2_cli.app,
        ["ec2_ssh_config_sync", "--create", "--dump", "--with-spots", "--config-path", str(cfg1)],
    )
    assert result_with_spot.exit_code == 0
    assert "i-spot" in result_with_spot.stdout


def test_parse_ssh_config_missing_file_returns_empty(tmp_path):
    missing = tmp_path / "missing_config"
    assert aws_ec2_cli.parse_ssh_config(str(missing)) == []


def test_parse_ssh_config_handles_equals_and_ignores_pre_host_lines(tmp_path):
    config_file = tmp_path / "config_eq"
    config_file.write_text(
        "User ubuntu\n"
        "MalformedOnlyKey\n"
        "Host web i-1\n"
        "HostName=web.example.com\n"
        "RemoteForward 9000 127.0.0.1:9000\n"
        "LocalForward 8000 127.0.0.1:8000\n",
        encoding="utf-8",
    )

    parsed = aws_ec2_cli.parse_ssh_config(str(config_file))

    assert len(parsed) == 1
    assert parsed[0]["host"] == ["web", "i-1"]
    assert parsed[0]["config"]["HostName"] == "web.example.com"
    assert parsed[0]["config"]["RemoteForward"] == ["9000 127.0.0.1:9000"]
    assert parsed[0]["config"]["LocalForward"] == ["8000 127.0.0.1:8000"]


def test_ec2_ssh_config_sync_updates_existing_entry_and_skips_instances_without_name(runner, monkeypatch, tmp_path):
    instances = [
        FakeInstance(
            "i-1",
            tags={"Name": "New Name", "User": "ec2-user"},
            public_dns_name="new.example",
        ),
        FakeInstance("i-2", tags={"User": "ubuntu"}, public_dns_name="ignored.example"),
    ]
    monkeypatch.setattr(aws_ec2_cli, "get_ec2_connection", lambda region: (FakeEc2Conn(instances), "eu-west-1"))

    cfg = tmp_path / "ssh_config_update"
    cfg.write_text(
        "Host old-name i-1\n"
        "HostName old.example\n"
        "User ubuntu\n",
        encoding="utf-8",
    )

    result = runner.invoke(
        aws_ec2_cli.app,
        ["ec2_ssh_config_sync", "--dump", "--config-path", str(cfg)],
    )

    assert result.exit_code == 0
    assert "Host new-name i-1" in result.stdout
    assert "HostName new.example" in result.stdout
    assert "User ec2-user" in result.stdout
    assert "i-2" not in result.stdout
