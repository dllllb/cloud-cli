import boto.ec2, os.path, time, argparse

def waitForStatus(inst, waitStatus, requiredStatus):
    status = inst.update()
    while status == waitStatus:
        time.sleep(10)
        status = inst.update()

    if status != requiredStatus:
        raise Exception('instance "%s" is in incorrect status: "%s"' % (inst.id, status))

def stop(inst):
    inst.stop()
    print('Waiting for instance to stop...')
    waitForStatus(inst, 'stopping', 'stopped')
    print('Stopped')

def start(inst):
    inst.start()

    print('Waiting for instance to start...')
    waitForStatus(inst, 'pending', 'running')
    print('Started')

def getInstanceByName(conn, instanceName):
    insts = conn.get_only_instances(filters={'tag:Name': instanceName})
    if len(insts) > 1:
        raise Exception('more than one instance with name "%s"' % instanceName)
    elif len(insts) == 0:
        raise Exception('no instances with name "%s" are found' % instanceName)
    return insts[0]    

parser = argparse.ArgumentParser()
parser.add_argument('command')
parser.add_argument('nodeName')

args = parser.parse_args()

conn = boto.ec2.connect_to_region(os.environ.get('AWS_DEFAULT_REGION', 'eu-west-1'))

inst = getInstanceByName(conn, args.nodeName)
if args.command == 'start':
    start(inst)
elif args.command == 'stop':
    stop(inst)
else:
    raise Exception('unknown command "%s"' % args.command)
