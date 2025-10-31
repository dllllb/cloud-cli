This repo contains a set of small command-line tools for more convenient interaction with cloud services like AWS, Google Cloud, Cloud.ru

List Cloud.ru ML Space notebooks:
```sh
uvx --from git+https://github.com/dllllb/cloud-cli cloud-ru-mls nb-list
```

List Cloud.ru ML Space jobs:
```sh
uvx --from git+https://github.com/dllllb/cloud-cli cloud-ru-mls job-list --region SR008
```

Generate a list of SSH config entries from the existing AWS EC2 hosts and print them:
```sh
uvx --from git+https://github.com/dllllb/cloud-cli ec2-ssh-conf-sync --dump
```

Synchronize ~/.ssh/.config entries with the existing AWS EC2 hosts:
```sh
uvx --from git+https://github.com/dllllb/cloud-cli ec2-ssh-conf-sync
```
