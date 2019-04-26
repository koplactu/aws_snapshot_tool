import boto3
import botocore
import click

session = boto3.Session(profile_name='aws_snapshot_tool')
ec2 = session.resource('ec2')

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

def has_pending_snapshot(volume):
    snapshots = list(volume.snapshots.all())
    return snapshots and snapshots[0].state == 'pending'

@click.group()
def cli():
    """AWS Snapshot Tool manages snapshots"""

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

    instances = filter_instances(project, instance)

    for i in instances:
        for v in i.volumes.all():
            for s in v.snapshots.all():
                print(", ".join((
                    s.id,
                    v.id,
                    i.id,
                    s.state,
                    s.progress,
                    s.start_time.strftime("%c")
                )))

                if s.state == 'completed' and not list_all: break

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

    instances = filter_instances(project, instance)

    for i in instances:
        for v in i.volumes.all():
            print(", ".join((
                v.id,
                i.id,
                v.state,
                str(v.size) + "GiB",
                v.encrypted and "Encrypted" or "Not Encrypted"
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
def create_snapshot(project, instance, force_run):
    "Create snapshots for EC2 instances"

    if ((project or force_run == True) or instance):
        instances = filter_instances(project, instance)

        for i in instances:
            print("Stopping {0}...".format(i.id))

            i.stop()
            i.wait_until_stopped()

            for v in i.volumes.all():
                if has_pending_snapshot(v):
                    print("  Skipping {0}, snapshot already in progress".format(v.id))
                    continue

                print("  Creating snapshot of {0}".format(v.id))
                v.create_snapshot(Description="Created by aws_snapshot_tool")

            print("Starting {0}...".format(i.id))
            i.start()
            i.wait_until_running()

        print("Finished")
    else:
        print("Error: project must be set unless force is set.")

    return

@instances.command('list')
@click.option('--project', default=None,
    help="Only instances for project (tag Project:<name>)")
def list_instances(project):
    "List EC2 instances"

    instances = filter_instances(project)

    for i in instances:
        tags = { t['Key']: t['Value'] for t in i.tags or [] }
        print(', '.join((
            i.id,
            i.instance_type,
            i.placement['AvailabilityZone'],
            i.state['Name'],
            i.public_dns_name,
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

    if ((project or force_run == True) or instance):
        instances = filter_instances(project, instance)

        for i in instances:
            print("Starting {0}...".format(i.id))
            try:
                i.start()
            except botocore.exceptions.ClientError as e:
                print("  Could not start instance {0}. ".format(i.id) + str(e))
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

    if ((project or force_run == True) or instance):
        instances = filter_instances(project, instance)

        for i in instances:
            print("Stopping {0}...".format(i.id))
            try:
                i.stop()
            except botocore.exceptions.ClientError as e:
                print("  Could not stop instance {0}. ".format(i.id) + str(e))
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

    if ((project or force_run == True) or instance):
        instances = filter_instances(project, instance)

        for i in instances:
            print("Rebooting {0}...".format(i.id))
            try:
                i.reboot()
            except botocore.exceptions.UnauthorizedOperation as e:
                print("  Could not reboot instance {0}. ".format(i.id) + str(e))
                continue
    else:
        print("Error: project must be set unless force is set.")

    return

if __name__ == '__main__':
    cli()
