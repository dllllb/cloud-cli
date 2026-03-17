#!/usr/bin/env python3
import json
import os
import time
from enum import Enum
from typing import Any, Dict, List, Optional, TextIO

import boto.ec2
import typer

app = typer.Typer(no_args_is_help=True)


def get_ec2_connection(region: Optional[str]):
    region_name = region or os.environ.get("AWS_DEFAULT_REGION", "eu-west-1")
    conn = boto.ec2.connect_to_region(region_name)
    if conn is None:
        raise typer.BadParameter(f'Unable to connect to region "{region_name}"')
    return conn, region_name


@app.command("ec2ansible")
def ec2ansible(
    region: Optional[str] = typer.Option(None, "--region", help="AWS region name."),
):
    """Generate Ansible inventory JSON from running EC2 instances."""
    conn, region_name = get_ec2_connection(region)
    insts = conn.get_only_instances(
        filters={"instance-state-name": "running", "tag:Group": "*"}
    )

    groups: Dict[str, Any] = {"_meta": {"hostvars": {}}}

    for inst in insts:
        host = inst.public_dns_name or inst.private_dns_name or inst.id

        group_name = inst.tags.get("Group")
        if group_name:
            groups.setdefault(group_name, []).append(host)

            hostvars = groups["_meta"]["hostvars"].setdefault(host, {})
            if "User" in inst.tags:
                hostvars["ansible_ssh_user"] = inst.tags["User"]
            hostvars["ec2_instance_id"] = inst.id
            hostvars["ec2_region"] = region_name

        groups[inst.id] = [host]

    typer.echo(json.dumps(groups))


def parse_ssh_config(path: str) -> List[Dict[str, Any]]:
    if not os.path.exists(path):
        return []

    config: List[Dict[str, Any]] = []
    host: Dict[str, Any] = {}

    with open(path, "r", encoding="utf-8") as file_obj:
        for raw_line in file_obj:
            line = raw_line.rstrip("\n").lstrip()

            if not line or line.startswith("#"):
                continue

            if "=" in line:
                key, value = line.split("=", 1)
                key = key.strip()
                value = value.strip()
            else:
                parts = line.split(None, 1)
                if len(parts) != 2:
                    continue
                key, value = parts[0], parts[1].lstrip()

            if key == "Host":
                if host:
                    config.append(host)
                host = {"host": value.split(), "config": {}}
                continue

            if not host:
                continue

            if key in ["IdentityFile", "LocalForward", "RemoteForward"]:
                host["config"].setdefault(key, []).append(value)
            elif key not in host["config"]:
                host["config"][key] = value

    if host:
        config.append(host)

    return config


def write_ssh_config(file_obj: TextIO, config: List[Dict[str, Any]]) -> None:
    for entry in config:
        file_obj.write(f"Host {' '.join(entry['host'])}\n")
        for key, value in entry["config"].items():
            if isinstance(value, list):
                for element in value:
                    file_obj.write(f"{key} {element}\n")
            else:
                file_obj.write(f"{key} {value}\n")
        file_obj.write("\n")


@app.command("ec2_ssh_config_sync")
def ec2_ssh_config_sync(
    dump: bool = typer.Option(False, "--dump", help="Print generated config to stdout."),
    create: bool = typer.Option(False, "--create", help="Build config from scratch."),
    with_spots: bool = typer.Option(
        False, "--with-spots", help="Include spot instances."
    ),
    region: Optional[str] = typer.Option(None, "--region", help="AWS region name."),
    config_path: str = typer.Option(
        "~/.ssh/config", "--config-path", help="Path to SSH config file."
    ),
):
    """Sync SSH config entries with running EC2 instances."""
    conn, _ = get_ec2_connection(region)
    insts = conn.get_only_instances(
        filters={"instance-state-name": "running", "tag:Name": "*"}
    )

    expanded_config_path = os.path.expanduser(config_path)
    cfg = [] if create else parse_ssh_config(expanded_config_path)

    cfg_dict = {host: entry for entry in cfg for host in entry["host"]}

    for inst in insts:
        if getattr(inst, "instanceLifecycle", None) and not with_spots:
            continue

        name_tag = inst.tags.get("Name")
        if not name_tag:
            continue

        name = name_tag.lower().replace(" ", "-")
        hostname = inst.public_dns_name or inst.private_dns_name or inst.id

        if inst.id in cfg_dict:
            cfg_dict[inst.id]["config"]["HostName"] = hostname
            cfg_dict[inst.id]["host"] = [name, inst.id]
            if "User" in inst.tags:
                cfg_dict[inst.id]["config"]["User"] = inst.tags["User"]
        else:
            config = {"HostName": hostname}
            if "User" in inst.tags:
                config["User"] = inst.tags["User"]
            entry = {"host": [name, inst.id], "config": config}
            cfg.append(entry)

    inst_set = {inst.id for inst in insts}
    filtered_cfg = []
    for entry in cfg:
        aws_ids = [host for host in entry["host"] if host.startswith("i-")]
        if aws_ids:
            if set(aws_ids) & inst_set:
                filtered_cfg.append(entry)
        else:
            filtered_cfg.append(entry)

    if dump:
        write_ssh_config(typer.get_text_stream("stdout"), filtered_cfg)
        return

    parent = os.path.dirname(expanded_config_path)
    if parent:
        os.makedirs(parent, exist_ok=True)

    with open(expanded_config_path, "w", encoding="utf-8") as file_obj:
        write_ssh_config(file_obj, filtered_cfg)


def wait_for_status(instance, wait_status: str, required_status: str, interval_sec: int = 10):
    status = instance.update()
    while status == wait_status:
        time.sleep(interval_sec)
        status = instance.update()

    if status != required_status:
        raise RuntimeError(
            f'instance "{instance.id}" is in incorrect status: "{status}"'
        )


def stop_instance(instance):
    instance.stop()
    typer.echo("Waiting for instance to stop...")
    wait_for_status(instance, "stopping", "stopped")
    typer.echo("Stopped")


def start_instance(instance):
    instance.start()
    typer.echo("Waiting for instance to start...")
    wait_for_status(instance, "pending", "running")
    typer.echo("Started")


def get_instance_by_name(conn, instance_name: str):
    instances = conn.get_only_instances(filters={"tag:Name": instance_name})
    if len(instances) > 1:
        raise RuntimeError(f'more than one instance with name "{instance_name}"')
    if len(instances) == 0:
        raise RuntimeError(f'no instances with name "{instance_name}" are found')
    return instances[0]


class Ec2CmdAction(str, Enum):
    start = "start"
    stop = "stop"


@app.command("ec2cmd")
def ec2cmd(
    command: Ec2CmdAction = typer.Argument(..., help="EC2 action: start or stop."),
    node_name: str = typer.Argument(..., help="Value of Name tag."),
    region: Optional[str] = typer.Option(None, "--region", help="AWS region name."),
):
    """Start or stop EC2 instance by Name tag."""
    conn, _ = get_ec2_connection(region)
    instance = get_instance_by_name(conn, node_name)

    if command == Ec2CmdAction.start:
        start_instance(instance)
    else:
        stop_instance(instance)


def main():
    app()


if __name__ == "__main__":
    main()
