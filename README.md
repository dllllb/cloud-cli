This repo contains a set of small command-line tools for more convenient interaction with cloud services like AWS

Generate a list of SSH config entries from the existing AWS EC2 hosts and print them:
```sh
uvx --from git+https://github.com/dllllb/cloud-cli ec2_ssh_config_sync --dump
```

Synchronize ~/.ssh/.config entries with the existing AWS EC2 hosts:
```sh
uvx --from git+https://github.com/dllllb/cloud-cli ec2_ssh_config_sync
```
