import boto3
import botocore
import click
import datetime

#session = boto3.Session(profile_name='aws_snapshot_tool')
#ec2 = session.resource('ec2')

def filter_instances(project, instance=False):
    instances = []

    if instance:
        instances.append(ec2.Instance(instance))
    elif project:
        filters = [{'Name': 'tag:Project', 'Values':[project]}]
        instances = ec2.instances.filter(Filters=filters)
    else:
        instances = ec2.instances.all()

    return instances

def instances_as_table(instances, get_volumes=True, get_snapshots=True):
    instance_rows = []
    for i in instances:
        instance_row = {}
        instance_row['instance_id'] = i.id
        instance_row['instance_type'] = i.instance_type
        instance_row['instance_state'] = i.state['Name']
        instance_row['instance_placement'] = i.placement['AvailabilityZone']
        instance_row['instance_public_dns_name'] = i.public_dns_name
        instance_row['instance_tags'] = i.tags
        if get_volumes:
            volume_rows = []
            for v in i.volumes.all():
                volume_row = {}
                volume_row['volume_id'] = v.id
                volume_row['volume_state'] = v.state
                volume_row['volume_size'] = v.size
                volume_row['volume_encrypted'] = v.encrypted
                volume_row['volume_device'] = v.attachments[0]['Device']
                if get_snapshots:
                    snapshot_rows = []
                    for s in sorted(list(v.snapshots.all()), key=lambda k: k.start_time, reverse=True):
                        snapshot_row = {}
                        snapshot_row['snapshot_id'] = s.id
                        snapshot_row['snapshot_state'] = s.state
                        snapshot_row['snapshot_progress'] = s.progress
                        snapshot_row['snapshot_start_time'] = s.start_time
                        snapshot_rows.append(snapshot_row)
                    volume_row['snapshots'] = snapshot_rows
                volume_rows.append(volume_row)
            instance_row['volumes'] = volume_rows
        instance_rows.append(instance_row)

    return instance_rows

def has_pending_snapshot(volume):
    snapshots = list(volume.snapshots.all())
    return snapshots and snapshots[0].state == 'pending'

@click.group()
@click.option('--profile', default='aws_snapshot_tool',
    help="Specify a different profile from the default 'aws_snapshot_tool'")
def cli(profile):
    """AWS Snapshot Tool manages snapshots"""

    global session, ec2

    session = boto3.Session(profile_name=profile)
    ec2 = session.resource('ec2')

@cli.group('snapshots')
def snapshots():
    """Commands for snapshots"""

@snapshots.command('list')
@click.option('--instance', default=None,
    help="List snapshots for a specific instance")
@click.option('--project', default=None,
    help="Only snapshots for project (tag Project:<name>)")
@click.option('--all', 'list_all', default=False, is_flag=True,
    help="List all snapshots for each volume not just the most recent")
def list_snapshots(project, instance, list_all):
    "List EC2 snapshots"

    instance_rows = instances_as_table(filter_instances(project, instance))

    for instance_row in instance_rows:
        for volume_row in instance_row['volumes']:
            for snapshot_row in volume_row['snapshots']:
                print(", ".join((
                    snapshot_row['snapshot_id'],
                    volume_row['volume_id'],
                    instance_row['instance_id'],
                    snapshot_row['snapshot_state'],
                    snapshot_row['snapshot_progress'],
                    snapshot_row['snapshot_start_time'].strftime("%c")
                )))

                if snapshot_row['snapshot_state'] == 'completed' and not list_all: break

    return

@cli.group('volumes')
def volumes():
    """Commands for volumes"""

@volumes.command('list')
@click.option('--instance', default=None,
    help="List snapshots for a specific instance")
@click.option('--project', default=None,
    help="Only volumes for project (tag Project:<name>)")
def list_volumes(project, instance):
    "List EC2 volumes"

    instance_rows = instances_as_table(filter_instances(project, instance), True, False)

    for instance_row in instance_rows:
        for volume_row in instance_row['volumes']:
            print(", ".join((
                volume_row['volume_id'],
                instance_row['instance_id'],
                volume_row['volume_device'],
                volume_row['volume_state'],
                str(volume_row['volume_size']) + "GiB",
                volume_row['volume_encrypted'] and "Encrypted" or "Not Encrypted"
            )))

    return

@cli.group('instances')
def instances():
    """Commands for instances"""

@instances.command('snapshot',
    help="Create snapshots of all volumes")
@click.option('--instance', default=None,
    help="Create snapshot for a specific instance")
@click.option('--project', default=None,
    help="Only instances for project (tag Project:<name>)")
@click.option('--force', 'force_run', default=False, is_flag=True,
    help="Force snapshot of instances if project or instance is not specified")
@click.option('--age', default=None,
    help="Only create snapshot if the last successful snapshot is older than the specified number of days")
def create_snapshot(project, instance, force_run, age):
    "Create snapshots for EC2 instances"

    if (project or force_run or instance):
        instance_rows = instances_as_table(filter_instances(project, instance))

        for instance_row in instance_rows:
            restart_instance = False

            for volume_row in instance_row['volumes']:
                ok_to_snapshot = True

                for snapshot_row in volume_row['snapshots']:
                    if snapshot_row and snapshot_row['snapshot_state'] == 'pending':
                        print("Skipping {0}, snapshot already in progress".format(volume_row['volume_id']))
                        ok_to_snapshot = False
                    elif snapshot_row and (age and (snapshot_row['snapshot_state'] == 'completed') and (datetime.timedelta(days=int(age)) > datetime.datetime.now(datetime.timezone.utc) - snapshot_row['snapshot_start_time'])):
                        print("Skipping {0}, snapshot younger than {1} days".format(volume_row['volume_id'], age))
                        ok_to_snapshot = False

                    break

                if ok_to_snapshot:
                    print("Creating snapshot of {0}".format(volume_row['volume_id']))

                    if instance_row['instance_state'] == 'running':
                        print("  Stopping {0}...".format(instance_row['instance_id']))
                        ec2.Instance(instance_row['instance_id']).stop()
                        ec2.Instance(instance_row['instance_id']).wait_until_stopped()
                        instance_row['instance_state'] = 'stopped'
                        restart_instance = True

                    try:
                        ec2.Volume(volume_row['volume_id']).create_snapshot(Description="Created by aws_snapshot_tool")
                    except botocore.exceptions.ClientError as e:
                        print("  Could not snapshot volume {0}. ".format(volume_row['volume_id']) + str(e))
                        continue

            if restart_instance:
                print("  Starting {0}...".format(instance_row['instance_id']))
                ec2.Instance(instance_row['instance_id']).start()
                ec2.Instance(instance_row['instance_id']).wait_until_running()
                instance_row['instance_state'] = 'running'

        print("Finished")
    else:
        print("Error: project must be set unless force is set.")

    return

@instances.command('list')
@click.option('--project', default=None,
    help="Only instances for project (tag Project:<name>)")
def list_instances(project):
    "List EC2 instances"

    instance_rows = instances_as_table(filter_instances(project), False, False)

    for instance_row in instance_rows:
        tags = { t['Key']: t['Value'] for t in instance_row['instance_tags'] or [] }
        print(', '.join((
            instance_row['instance_id'],
            instance_row['instance_type'],
            instance_row['instance_placement'],
            instance_row['instance_state'],
            instance_row['instance_public_dns_name'],
            tags.get('Project','<no project>'))
        ))

    return

@instances.command('start')
@click.option('--instance', default=None,
    help="Start a specific instance")
@click.option('--project', default=None,
    help="Only instances for project (tag Project:<name>)")
@click.option('--force', 'force_run', default=False, is_flag=True,
    help="Force start of instances if project or instance is not specified")
def start_instances(project, instance, force_run):
    "Start EC2 instances"

    if (project or force_run or instance):
        instance_rows = instances_as_table(filter_instances(project, instance), False, False)

        for instance_row in instance_rows:
            if instance_row['instance_state'] == 'stopped':
                print("Starting {0}...".format(instance_row['instance_id']))
                try:
                    ec2.Instance(instance_row['instance_id']).start()
                except botocore.exceptions.ClientError as e:
                    print("  Could not start instance {0}. ".format(instance_row['instance_id']) + str(e))
                    continue
    else:
        print("Error: project must be set unless force is set.")

    return

@instances.command('stop')
@click.option('--instance', default=None,
    help="Stop a specific instance")
@click.option('--project', default=None,
    help="Only instances for project (tag Project:<name>)")
@click.option('--force', 'force_run', default=False, is_flag=True,
    help="Force stop of instances if project or instance is not specified")
def stop_instances(project, instance, force_run):
    "Stop EC2 instances"

    if (project or force_run or instance):
        instance_rows = instances_as_table(filter_instances(project, instance), False, False)

        for instance_row in instance_rows:
            if instance_row['instance_state'] == 'running':
                print("Stopping {0}...".format(instance_row['instance_id']))
                try:
                    ec2.Instance(instance_row['instance_id']).stop()
                except botocore.exceptions.ClientError as e:
                    print("  Could not stop instance {0}. ".format(instance_row['instance_id']) + str(e))
                    continue
    else:
        print("Error: project must be set unless force is set.")

    return

@instances.command('reboot')
@click.option('--instance', default=None,
    help="Reboot a specific instance")
@click.option('--project', default=None,
    help="Only instances for project (tag Project:<name>)")
@click.option('--force', 'force_run', default=False, is_flag=True,
    help="Force reboot of instances if project or instance is not specified")
def reboot_instances(project, instance, force_run):
    "Reboot EC2 instances"

    if (project or force_run or instance):
        instance_rows = instances_as_table(filter_instances(project, instance), False, False)

        for instance_row in instance_rows:
            if instance_row['instance_state'] == 'running':
                print("Rebooting {0}...".format(instance_row['instance_id']))
                try:
                    ec2.Instance(instance_row['instance_id']).reboot()
                except botocore.exceptions.UnauthorizedOperation as e:
                    print("  Could not reboot instance {0}. ".format(instance_row['instance_id']) + str(e))
                    continue
    else:
        print("Error: project must be set unless force is set.")

    return

if __name__ == '__main__':
    cli()
