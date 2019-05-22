"""aws_snapshot_tool"""
import datetime
import boto3
import botocore
import click

def filter_instances(ctx, project, instance=False):
    """filter_instances"""
    instances_list = []

    if instance:
        instances_list.append(ctx.obj['ec2_resource'].Instance(instance))
    elif project:
        filters = [{'Name': 'tag:Project', 'Values':[project]}]
        instances_list = ctx.obj['ec2_resource'].instances.filter(Filters=filters)
    else:
        instances_list = ctx.obj['ec2_resource'].instances.all()

    return instances_list

def instances_as_table(instance_list, get_volumes=True, get_snapshots=True):
    """instances_as_table"""
    instance_rows = []
    for ins in instance_list:
        instance_row = {}
        instance_row['instance_id'] = ins.id
        instance_row['instance_type'] = ins.instance_type
        instance_row['instance_state'] = ins.state['Name']
        instance_row['instance_placement'] = ins.placement['AvailabilityZone']
        instance_row['instance_public_dns_name'] = ins.public_dns_name
        instance_row['instance_tags'] = ins.tags
        if get_volumes:
            volume_rows = []
            for vol in ins.volumes.all():
                volume_row = {}
                volume_row['volume_id'] = vol.id
                volume_row['volume_state'] = vol.state
                volume_row['volume_size'] = vol.size
                volume_row['volume_encrypted'] = vol.encrypted
                volume_row['volume_device'] = vol.attachments[0]['Device']
                if get_snapshots:
                    snapshot_rows = []
                    for snp in sorted(list(vol.snapshots.all()), key=lambda k: k.start_time, \
                                                                 reverse=True):
                        snapshot_row = {}
                        snapshot_row['snapshot_id'] = snp.id
                        snapshot_row['snapshot_state'] = snp.state
                        snapshot_row['snapshot_progress'] = snp.progress
                        snapshot_row['snapshot_start_time'] = snp.start_time
                        snapshot_rows.append(snapshot_row)
                    volume_row['snapshots'] = snapshot_rows
                volume_rows.append(volume_row)
            instance_row['volumes'] = volume_rows
        instance_rows.append(instance_row)

    return instance_rows

# def has_pending_snapshot(volume):
#     """has_pending_snapshot"""
#     snapshots_list = list(volume.snapshots.all())
#     return snapshots_list and snapshots_list[0].state == 'pending'

@click.group()
@click.option('--profile', default='aws_snapshot_tool', \
    help="Specify a different profile from the default 'aws_snapshot_tool'")
@click.option('--region', default='ap-southeast-2', \
    help="Specify a different region from the default 'ap-southeast-2'")
@click.pass_context
def cli(ctx, profile, region):
    """AWS Snapshot Tool manages snapshots"""

    session = boto3.Session(profile_name=profile, region_name=region)
    ctx.obj = {
        'ec2_resource': session.resource('ec2'),
        'ec2_client': session.client('ec2')
    }

@cli.group('snapshots')
def snapshots():
    """Commands for snapshots"""

@snapshots.command('list')
@click.option('--instance', default=None, \
    help="List snapshots for a specific instance")
@click.option('--project', default=None, \
    help="Only snapshots for project (tag Project:<name>)")
@click.option('--all', 'list_all', default=False, is_flag=True, \
    help="List all snapshots for each volume not just the most recent")
@click.pass_context
def list_snapshots(ctx, project, instance, list_all):
    "List EC2 snapshots"

    instance_rows = instances_as_table(filter_instances(ctx, project, instance))

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

                if snapshot_row['snapshot_state'] == 'completed' and not list_all:
                    break

    return 0

@cli.group('volumes')
def volumes():
    """Commands for volumes"""

@volumes.command('list')
@click.option('--instance', default=None, \
    help="List snapshots for a specific instance")
@click.option('--project', default=None, \
    help="Only volumes for project (tag Project:<name>)")
@click.pass_context
def list_volumes(ctx, project, instance):
    "List EC2 volumes"

    instance_rows = instances_as_table(filter_instances(ctx, project, instance), True, False)

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

    return 0

@cli.group('instances')
def instances():
    """Commands for instances"""

@instances.command('snapshot', \
    help="Create snapshots of all volumes")
@click.option('--instance', default=None, \
    help="Create snapshot for a specific instance")
@click.option('--project', default=None, \
    help="Only instances for project (tag Project:<name>)")
@click.option('--force', 'force_run', default=False, is_flag=True, \
    help="Force snapshot of instances if project or instance is not specified")
@click.option('--age', default=None, \
    help="Only create snapshot if the last successful snapshot is older than the \
            specified number of days")
@click.pass_context
def create_snapshot(ctx, project, instance, force_run, age):
    "Create snapshots for EC2 instances"

    if (project or force_run or instance):
        instance_rows = instances_as_table(filter_instances(ctx, project, instance))

        for instance_row in instance_rows:
            restart_instance = False

            for volume_row in instance_row['volumes']:
                ok_to_snapshot = True

                for snapshot_row in volume_row['snapshots']:
                    if snapshot_row and snapshot_row['snapshot_state'] == 'pending':
                        print("Skipping {0}, snapshot already in progress".format( \
                                                                        volume_row['volume_id']))
                        ok_to_snapshot = False
                    elif snapshot_row and (age and \
                        (snapshot_row['snapshot_state'] == 'completed') and \
                        (datetime.timedelta(days=int(age)) > \
                            datetime.datetime.now(datetime.timezone.utc) - \
                                snapshot_row['snapshot_start_time'])):
                        print("Skipping {0}, snapshot younger than {1} days".format( \
                                                                    volume_row['volume_id'], age))
                        ok_to_snapshot = False

                    break

                if ok_to_snapshot:
                    print("Creating snapshot of {0}".format(volume_row['volume_id']))

                    if instance_row['instance_state'] == 'running':
                        print("  Stopping {0}...".format(instance_row['instance_id']))
                        ctx.obj['ec2_resource'].Instance(instance_row['instance_id']).stop()
                        ctx.obj['ec2_resource'].Instance( \
                                            instance_row['instance_id']).wait_until_stopped()
                        instance_row['instance_state'] = 'stopped'
                        restart_instance = True

                    try:
                        ctx.obj['ec2_resource'].Volume(volume_row['volume_id']).create_snapshot( \
                                            Description="Created by aws_snapshot_tool")
                    except botocore.exceptions.ClientError as exc:
                        print("  Could not snapshot volume {0}. ".format( \
                                                        volume_row['volume_id']) + str(exc))
                        continue

            if restart_instance:
                print("  Starting {0}...".format(instance_row['instance_id']))
                ctx.obj['ec2_resource'].Instance(instance_row['instance_id']).start()
                ctx.obj['ec2_resource'].Instance(instance_row['instance_id']).wait_until_running()
                instance_row['instance_state'] = 'running'

        print("Finished")
    else:
        print("Error: project must be set unless force is set.")

    return 0

@instances.command('list')
@click.option('--project', default=None, \
    help="Only instances for project (tag Project:<name>)")
@click.pass_context
def list_instances(ctx, project):
    "List EC2 instances"

    instance_rows = instances_as_table(filter_instances(ctx, project), False, False)

    for instance_row in instance_rows:
        tags = {t['Key']: t['Value'] for t in instance_row['instance_tags'] or []}
        print(', '.join((
            instance_row['instance_id'],
            instance_row['instance_type'],
            instance_row['instance_placement'],
            instance_row['instance_state'],
            instance_row['instance_public_dns_name'],
            tags.get('Project', '<no project>'))))

    return 0

@instances.command('start')
@click.option('--instance', default=None, \
    help="Start a specific instance")
@click.option('--project', default=None, \
    help="Only instances for project (tag Project:<name>)")
@click.option('--force', 'force_run', default=False, is_flag=True, \
    help="Force start of instances if project or instance is not specified")
@click.pass_context
def start_instances(ctx, project, instance, force_run):
    "Start EC2 instances"

    if (project or force_run or instance):
        instance_rows = instances_as_table(filter_instances(ctx, project, instance), False, False)

        for instance_row in instance_rows:
            if instance_row['instance_state'] == 'stopped':
                print("Starting {0}...".format(instance_row['instance_id']))
                try:
                    ctx.obj['ec2_resource'].Instance(instance_row['instance_id']).start()
                except botocore.exceptions.ClientError as exc:
                    print("  Could not start instance {0}. ".format( \
                                                    instance_row['instance_id']) + str(exc))
                    continue
    else:
        print("Error: project must be set unless force is set.")

    return 0

@instances.command('stop')
@click.option('--instance', default=None, \
    help="Stop a specific instance")
@click.option('--project', default=None, \
    help="Only instances for project (tag Project:<name>)")
@click.option('--force', 'force_run', default=False, is_flag=True, \
    help="Force stop of instances if project or instance is not specified")
@click.pass_context
def stop_instances(ctx, project, instance, force_run):
    "Stop EC2 instances"

    if (project or force_run or instance):
        instance_rows = instances_as_table(filter_instances(ctx, project, instance), False, False)

        for instance_row in instance_rows:
            if instance_row['instance_state'] == 'running':
                print("Stopping {0}...".format(instance_row['instance_id']))
                try:
                    ctx.obj['ec2_resource'].Instance(instance_row['instance_id']).stop()
                except botocore.exceptions.ClientError as exc:
                    print("  Could not stop instance {0}. ".format( \
                                                    instance_row['instance_id']) + str(exc))
                    continue
    else:
        print("Error: project must be set unless force is set.")

    return 0

@instances.command('reboot')
@click.option('--instance', default=None, \
    help="Reboot a specific instance")
@click.option('--project', default=None, \
    help="Only instances for project (tag Project:<name>)")
@click.option('--force', 'force_run', default=False, is_flag=True, \
    help="Force reboot of instances if project or instance is not specified")
@click.pass_context
def reboot_instances(ctx, project, instance, force_run):
    "Reboot EC2 instances"

    if (project or force_run or instance):
        instance_rows = instances_as_table(filter_instances(ctx, project, instance), False, False)

        for instance_row in instance_rows:
            if instance_row['instance_state'] == 'running':
                print("Rebooting {0}...".format(instance_row['instance_id']))
                try:
                    ctx.obj['ec2_resource'].Instance(instance_row['instance_id']).reboot()
                except botocore.exceptions.ClientError as exc:
                    print("  Could not reboot instance {0}. ".format( \
                                                    instance_row['instance_id']) + str(exc))
                    continue
    else:
        print("Error: project must be set unless force is set.")

    return 0

@instances.command('teardown', \
    help="Teardown an instance and all its associated snapshots and volumes")
@click.option('--instance', default=None, \
    help="Teardown a specific instance")
@click.option('--project', default=None, \
    help="Teardown all instances for project (tag Project:<name>)")
@click.option('--force', 'force_run', default=False, is_flag=True, \
    help="Force teardown of instances if project or instance is not specified")
@click.pass_context
def teardown_instance(ctx, project, instance, force_run):
    "Teardown EC2 instances"

    if (project or force_run or instance):
        instance_rows = instances_as_table(filter_instances(ctx, project, instance))

        for instance_row in instance_rows:
            if instance_row['instance_state'] == 'running':
                print("Stopping {0}...".format(instance_row['instance_id']))
                try:
                    ctx.obj['ec2_resource'].Instance(instance_row['instance_id']).stop()
                    ctx.obj['ec2_resource'].Instance( \
                                        instance_row['instance_id']).wait_until_stopped()
                except botocore.exceptions.ClientError as exc:
                    print("  Could not stop instance {0}. ".format( \
                                                    instance_row['instance_id']) + str(exc))
                    continue
            for volume_row in instance_row['volumes']:
                for snapshot_row in volume_row['snapshots']:
                    try:
                        print("Deleting snapshot {0}...".format(snapshot_row['snapshot_id']))
                        response = ctx.obj['ec2_client'].delete_snapshot( \
                                                SnapshotId=snapshot_row['snapshot_id'])
                    except botocore.exceptions.ClientError as exc:
                        print("  Could not delete snapshot {0}. ".format( \
                                                        snapshot_row['snapshot_id']) + \
                                                        str(exc) + '\n  ' + str(response))
                        continue
                try:
                    print("Detaching volume {0}...".format(volume_row['volume_id']))
                    response = ctx.obj['ec2_client'].detach_volume( \
                                            VolumeId=volume_row['volume_id'])
                    ctx.obj['ec2_client'].get_waiter('volume_available').wait( \
                                                        VolumeIds=[volume_row['volume_id']])
                except botocore.exceptions.ClientError as exc:
                    print("  Could not detach volume {0}. ".format( \
                                                    volume_row['volume_id']) + \
                                                    str(exc) + '\n  ' + str(response))
                    continue
                try:
                    print("Deleting volume {0}...".format(volume_row['volume_id']))
                    response = ctx.obj['ec2_client'].delete_volume( \
                                            VolumeId=volume_row['volume_id'])
                except botocore.exceptions.ClientError as exc:
                    print("  Could not delete volume {0}. ".format( \
                                                    volume_row['volume_id']) + \
                                                    str(exc) + '\n  ' + str(response))
                    continue
            try:
                print("Terminating {0}...".format(instance_row['instance_id']))
                response = ctx.obj['ec2_client'].terminate_instances( \
                                        InstanceIds=[instance_row['instance_id']])
            except botocore.exceptions.ClientError as exc:
                print("  Could not terminate instance {0}. ".format( \
                                                instance_row['instance_id']) + \
                                                str(exc) + '\n  ' + str(response))
                continue

        print("Finished")
    else:
        print("Error: project must be set unless force is set.")

    return 0

if __name__ == '__main__':
    cli(None, None, None)
