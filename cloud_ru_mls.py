#!/usr/bin/env python3
import argparse
from datetime import timedelta
import json
import os
import requests
import typer
from typing_extensions import Annotated
from tabulate import tabulate

app = typer.Typer()

BASE_URL = "https://api.ai.cloud.ru/public/v2"

CONFIG_PATH = "~/.cloudru/credentials.json"

CONFIG_FORMAT = """{
    "auth": {
        "client_id": "12345",
        "client_secret": "12345"
    },
    "workspaces": {
        "workspace-name": {
            "x-workspace-id": "uuid",
            "x-api-key": "uuid"
        }
    }
}"""

NB_TYPE_TO_NGPU = {f"gpu_{n}": n for n in range (1, 9)}
NB_TYPE_TO_NGPU["cce"] = 0


def load_config(path):
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)


def get_workspace(config, workspace_name=None):
    workspaces = config.get("workspaces", {})
    if not workspaces:
        raise ValueError("No workspaces found in configuration")

    if workspace_name:
        if workspace_name not in workspaces:
            raise ValueError(f"Workspace '{workspace_name}' not found in configuration")
        return workspace_name, workspaces[workspace_name]

    # first in dict by default
    default_name = next(iter(workspaces))
    return default_name, workspaces[default_name]


def authenticate(ws, config):
    url = f"{BASE_URL}/service_auth"
    payload = {
        "client_id": config["client_id"],
        "client_secret": config["client_secret"]
    }
    headers = {
        "x-api-key": ws["x-api-key"],
    }

    resp = requests.post(url, headers=headers, json=payload)
    resp.raise_for_status()
    data = resp.json()
    if "token" not in data:
        raise RuntimeError(f"Authentication failed: {data}")
    return data["token"]["access_token"]


def list_notebooks(headers):
    url = f"{BASE_URL}/notebooks/v2/notebooks"

    resp = requests.get(url, headers=headers)
    resp.raise_for_status()
    return resp.json().get("notebooks", [])


def list_jobs(headers, region):
    url = f"{BASE_URL}/jobs"

    resp = requests.get(url, headers=headers, params={"status": "Running", "region": region})
    resp.raise_for_status()
    return resp.json().get("jobs", [])


def get_namespace(headers):
    url = f"{BASE_URL}/workspaces/v3/{headers['x-workspace-id']}"

    resp = requests.get(url, headers=headers)
    resp.raise_for_status()
    return resp.json().get("namespace", "N/A")


def get_ws_allocactions(headers):
    url = f"{BASE_URL}/workspaces/v3/{headers['x-workspace-id']}/allocations"

    resp = requests.get(url, headers=headers)
    resp.raise_for_status()
    return resp.json()


def get_allocation_resources(headers, alloc):
    url = f"{BASE_URL}/allocations/{alloc}/resources_status"

    resp = requests.get(url, headers=headers)
    resp.raise_for_status()
    return resp.json()


def init_headers(workspace_name: str, verbose=True):
    config = load_config(os.path.expanduser(CONFIG_PATH))
    ws_name, ws = get_workspace(config, workspace_name)
    token = authenticate(ws, config['auth'])

    headers = {
        "authorization": token,
        "x-workspace-id": ws["x-workspace-id"],
        "x-api-key": ws["x-api-key"]
    }

    if verbose:
        print(f"Using workspace: {ws_name}")

    return headers


@app.command()
def nb_list(workspace: str = None):
    headers = init_headers(workspace)

    notebooks = list_notebooks(headers)

    nb_fields = [{
        "Name": nb["name"],
        "Author": nb["author"],
        "Duration": timedelta(seconds=nb["ageSeconds"]),
        "nGPU": NB_TYPE_TO_NGPU[nb["notebookType"]],
        "Region": nb["region"],
    } for nb in notebooks if nb.get("status", "unknown") == "running"]

    if len(set(nb["Region"] for nb in nb_fields)) < 2:
        for e in nb_fields:
            del e["Region"]

    print(f"Found {len(nb_fields)} running notebook(s):")
    print(tabulate(nb_fields, headers="keys", floatfmt=".0f"))
    print(f"Total GPU used: {sum(int(e['nGPU']) for e in nb_fields)}")


@app.command()
def job_list(region: Annotated[str, typer.Option()], workspace: str = None):
    headers = init_headers(workspace)

    jobs = list_jobs(headers, region)

    job_fields = [{
        "Desc": job["job_desc"],
        "nGPU": job["gpu_count"],
        "Duration": timedelta(seconds=int(job["duration"][:-1])),
    } for job in jobs]

    print(f"Found {len(job_fields)} running job(s):")
    print(tabulate(job_fields, headers="keys"))
    print(f"Total GPU used: {sum(int(e['nGPU']) for e in job_fields)}")


@app.command()
def gpu_stat(region: Annotated[str, typer.Option()], workspace: str = None):
    headers = init_headers(workspace)

    notebooks = list_notebooks(headers)

    nb_ngpu = sum(
        NB_TYPE_TO_NGPU[nb["notebookType"]] for nb in notebooks
        if nb.get("status", "unknown") == "running"
        and nb.get("region", "unknown").lower() == region.lower()
    )

    jobs = list_jobs(headers, region)

    job_ngpu = sum(int(e['gpu_count']) for e in jobs)

    allocs = get_ws_allocactions(headers)

    total_gpus = 0
    available_gpus = 0
    for al in allocs:
        if not al.get("region_key", "unknown").lower() == region.lower():
            continue

        alloc_res = get_allocation_resources(headers, al["id"])

        total_gpus += alloc_res["gpu"]["current"]
        available_gpus += alloc_res["gpu"]["available"]

    print(f"GPUs: {int(available_gpus)}/{int(total_gpus)}, % used: {(1-available_gpus/total_gpus)*100:.2f}, notebooks GPUs: {nb_ngpu}, jobs GPUs: {job_ngpu}")


@app.command()
def nb_ssh_conf(workspace: str = None):
    headers = init_headers(workspace, verbose=False)

    ns = get_namespace(headers)
    notebooks = list_notebooks(headers)

    for nb in notebooks:
        if nb.get("status", "unknown") == "running":
            continue

    ssh_entries = [[
        f"Host mlspace-{nb['name']}",
        f"HostName ssh-{nb['region'].lower()}-jupyter.ai.cloud.ru",
        f"User {nb['name']}.{ns}",
        "Port 2222",
    ] for nb in notebooks if nb.get("status", "unknown") == "running"]

    for e in ssh_entries:
        for r in e:
            print(r)
        print()


if __name__ == "__main__":
    app()
