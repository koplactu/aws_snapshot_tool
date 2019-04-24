# aws_snapshot_tool

Demo project to manage AWS EC2 instance snapshots

## About

This is a demo project that uses boto3 to manage AWS EC2 instance snapshots.

## Configuring

aws_snapshot_tool uses the configuration file created by the AWS cli e.g.

`aws configure --profile aws_snapshot_tool`

## Running

`pipenv run python snapsnot/aws_snapshot_tool.py <command> <subcommand> <--project=PROJECT>`

*command* is instances, volumes or snapshots
*subcommand* depends on command
*project* is optional
