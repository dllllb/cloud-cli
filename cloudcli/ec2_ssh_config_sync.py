import boto.ec2, os, sys, argparse

def writeConfig(f, config):
    for entry in config:
        f.write('Host %s\n' % ' '.join(entry['host']))
        for key, value in entry['config'].iteritems():
            if type(value) is list:
                for element in value:
                    f.write('%s %s\n' % (key, element))
            else:
                f.write('%s %s\n' % (key, value))
        f.write('\n')

def parseConfig(f):
    config = list()
    host = dict()
    for line in f:
        line = line.rstrip('\n').lstrip()
        if (line == '') or (line[0] == '#'):
            continue
        if '=' in line:
            key, value = line.split('=', 1)
            key = key.strip()
        else:
            key, value = line.split(None, 1)
            value = value.lstrip()
        if key == 'Host':
            if host: config.append(host)
            value = value.split()
            host = {'host': value, 'config': {}}
        elif key in ['IdentityFile', 'LocalForward', 'RemoteForward']:
            if key in host['config']:
                host['config'][key].append(value)
            else:
                host['config'][key] = [value]
        elif key not in host['config']:
            host['config'].update({key: value})
    if host: config.append(host)
    return config

parser = argparse.ArgumentParser()
parser.add_argument('--dump', action="store_true")
parser.add_argument('--create', action="store_true")
parser.add_argument('--with-spots', action="store_true")
args = parser.parse_args()

conn = boto.ec2.connect_to_region(os.environ.get('AWS_DEFAULT_REGION', 'eu-west-1'))

insts = conn.get_only_instances(filters={'instance-state-name': "running", "tag:Name": "*"})

if args.create:
    cfg = []
else:
    with open(os.path.expanduser("~/.ssh/config")) as f:
        cfg = parseConfig(f)

cfg_dict = dict([(host, entry) for entry in cfg for host in entry['host']])

for inst in insts:
    if (not hasattr(inst, 'instanceLifecycle') or args.with_spots):
        name = inst.tags['Name'].lower().replace(' ', '-')
        hostname = inst.public_dns_name
        if inst.id in cfg_dict:
            cfg_dict[inst.id]['config']['HostName'] = hostname
            cfg_dict[inst.id]['host'] = [name, inst.id]
            if 'User' in inst.tags:
                cfg_dict[inst.id]['config']['User'] = inst.tags['User']
        else:
            config = {'HostName': hostname}
            if 'User' in inst.tags:
                config['User'] = inst.tags['User']
            entry = {'host': [name, inst.id], 'config': config}
            cfg.append(entry)

inst_set = set([inst.id for inst in insts])

filtered_cfg = []
for entry in cfg:
    aws_ids = [host for host in entry['host'] if host.startswith('i-')]
    if len(aws_ids) > 0:
        if len(set(aws_ids) & inst_set) > 0:
            filtered_cfg.append(entry)
    else:
        filtered_cfg.append(entry)

if args.dump:
    writeConfig(sys.stdout, filtered_cfg)
else:
    with open(os.path.expanduser("~/.ssh/config"), 'w') as f:
        writeConfig(f, filtered_cfg)
