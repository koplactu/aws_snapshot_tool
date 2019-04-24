from setuptools import setup

setup(
    name='aws_snapshot_tool',
    version='0.1',
    author='koplactu',
    #author_email='koplactu@bigpond.com',
    description='aws_snapshot_tool is a tool to manage AWS EC2 snapshots',
    license='GPLv3+',
    packages=['snapshot'],
    url='https://github.com/koplactu/aws_snapshot_tool',
    install_requires=[
        'click',
        'boto3'
    ],
    entry_points='''
        [console_scripts]
        aws_snapshot_tool=snapshot.aws_snapshot_tool:cli
    '''
)
