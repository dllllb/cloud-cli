import boto.ec2, os, json

region = os.environ.get('AWS_DEFAULT_REGION', 'eu-west-1')
conn = boto.ec2.connect_to_region(region)

insts = conn.get_only_instances(filters={'instance-state-name': "running", "tag:Group": "*"})

groups = {'_meta': {'hostvars': {}}}

for inst in insts:
    host = inst.public_dns_name
    if 'Group' in inst.tags:
        grp = inst.tags['Group']
        if grp in groups:
            groups[grp].append(host)
        else:
            groups[grp] = [host]

        groups['_meta']['hostvars'][host] = {}
        if 'User' in inst.tags:
            groups['_meta']['hostvars'][host]["ansible_ssh_user"] = inst.tags['User']

        groups['_meta']['hostvars'][host]["ec2_instance_id"] = inst.id
        groups['_meta']['hostvars'][host]["ec2_region"] = region

    groups[inst.id] = [host]

print(json.dumps(groups))
