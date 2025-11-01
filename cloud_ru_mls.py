#!/usr/bin/env python3
import argparse
from datetime import timedelta
import json
import os
import requests
from tabulate import tabulate

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

NB_TYPE_TO_NGPU = {
    "gpu_1": 1,
    "gpu_2": 2,
    "gpu_3": 3,
    "gpu_4": 4,
    "gpu_5": 5,
    "gpu_6": 6,
    "gpu_7": 7,
    "gpu_8": 8,
    "cce": 0
}


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


def list_notebooks(access_token, workspace):
    url = f"{BASE_URL}/notebooks/v2/notebooks"
    headers = {
        "authorization": access_token,
        "x-workspace-id": workspace["x-workspace-id"],
        "x-api-key": workspace["x-api-key"]
    }

    resp = requests.get(url, headers=headers)
    resp.raise_for_status()
    return resp.json().get("notebooks", [])


def list_jobs(access_token, workspace, region):
    url = f"{BASE_URL}/jobs"
    headers = {
        "authorization": access_token,
        "x-workspace-id": workspace["x-workspace-id"],
        "x-api-key": workspace["x-api-key"]
    }

    resp = requests.get(url, headers=headers, params={"status": "Running", "region": region})
    resp.raise_for_status()
    return resp.json().get("jobs", [])


def get_namespace(access_token, workspace):
    url = f"{BASE_URL}/workspaces/v3/{workspace['x-workspace-id']}"
    headers = {
        "authorization": access_token,
        "x-workspace-id": workspace["x-workspace-id"],
        "x-api-key": workspace["x-api-key"]
    }
    resp = requests.get(url, headers=headers)
    resp.raise_for_status()
    return resp.json().get("namespace", "N/A")


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('command')
    parser.add_argument('--workspace', default=None)
    parser.add_argument('--region', default=None)

    args = parser.parse_args()
    
    workspace_name = args.workspace

    config = load_config(os.path.expanduser(CONFIG_PATH))

    ws_name, ws = get_workspace(config, workspace_name)

    token = authenticate(ws, config['auth'])

    if args.command == 'nb-list':
        print(f"Using workspace: {ws_name}")

        notebooks = list_notebooks(token, ws)

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

    elif args.command == 'job-list':
        print(f"Using workspace: {ws_name}")

        jobs = list_jobs(token, ws, args.region)

        job_fields = [{
            "Desc": job["job_desc"],
            "nGPU": job["gpu_count"],
            "Duration": timedelta(seconds=int(job["duration"][:-1])),
        } for job in jobs]

        print(f"Found {len(job_fields)} running job(s):")
        print(tabulate(job_fields, headers="keys"))
        print(f"Total GPU used: {sum(int(e['nGPU']) for e in job_fields)}")
    elif args.command == 'gpu-stat':
        print(f"Using workspace: {ws_name}")
        
        notebooks = list_notebooks(token, ws)

        nb_ngpu = sum(
            NB_TYPE_TO_NGPU[nb["notebookType"]] for nb in notebooks
            if nb.get("status", "unknown") == "running"
        )

        jobs = list_jobs(token, ws, args.region)

        job_ngpu = sum(int(e['gpu_count']) for e in jobs)

        print(f"Total GPU used: {nb_ngpu + job_ngpu}, notebooks GPUs: {nb_ngpu} jobs GPUs: {job_ngpu}")

    elif args.command == 'nb-ssh-conf':
        ns = get_namespace(token, ws)
        notebooks = list_notebooks(token, ws)

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
    else:
        raise ValueError(f"Unknown command: {args.command}")

if __name__ == "__main__":
    main()
