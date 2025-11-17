This repo contains a set of small command-line tools for more convenient interaction with cloud services like AWS, Google Cloud, Cloud.ru

List Cloud.ru ML Space notebooks:
```sh
uvx --from git+https://github.com/dllllb/cloud-cli cloud-ru-mls nb-list
```

List Cloud.ru ML Space jobs:
```sh
uvx --from git+https://github.com/dllllb/cloud-cli cloud-ru-mls job-list --region SR008
```

Show Cloud.ru ML Space region GPU status:
```sh
uvx --from git+https://github.com/dllllb/cloud-cli cloud-ru-mls gpu-stat --region SR008
```

Generate a list of the SSH config entries from the running Cloud.ru notebooks:
```sh
uvx --from git+https://github.com/dllllb/cloud-cli cloud-ru-mls nb-ssh-conf > ~/.ssh/mlspace.conf
```

Add `Include mlspace.conf` directive to the `~/.ssh/config`. It should be added before the oher hosts declarations, i. e. in the beginning of the file.

Generated SSH configuraion can be used with [vllmctl](https://github.com/Adefful/vllmctl):
```sh
uvx git+https://github.com/Adefful/vllmctl gpu-idle-top --host-regex mlspace-.+
```

Generate a list of the SSH config entries from the existing AWS EC2 hosts:
```sh
uvx --from git+https://github.com/dllllb/cloud-cli ec2-ssh-conf-sync --dump --create > ~/.ssh/ec2.conf
```
Add `Include ec2.conf` directive to the `~/.ssh/config`. It should be added before the oher hosts declarations, i. e. in the beginning of the file.

## Cloud.ru credentials

Cloud.ru credentials for ML Space should be in `~/.cloudru/credentials.json`

Example:
```json
{
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
}
```
