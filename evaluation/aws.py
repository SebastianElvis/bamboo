import boto3
import socket
import json
import collections
import dataclasses
import enum
import itertools
import os
import socket
import sys
import time
import pssh.clients
import gevent
import subprocess
import random
import math
import pathlib
from pprint import pprint
from datetime import datetime, timedelta
from typing import Any, Callable, Dict, Iterable, List, Optional, Union


################################################################
# Parameters

REFRESH_INTERVAL = 5.0
REGIONS = {
    'us-east-1': 'N. Virginia',
    'us-west-1': 'N. California',
    'ap-south-1': 'Mumbai',
    'ap-northeast-2': 'Seoul',
    'ap-southeast-2': 'Sydney',
    'ap-northeast-1': 'Tokyo',
    'ca-central-1': 'Canada',
    'eu-west-1': 'Ireland',
    'sa-east-1': 'Sao Paulo',
    'us-west-2': 'Oregon',
    'us-east-2': 'Ohio',
    'ap-southeast-1': 'Singapore',
    'eu-west-2': 'London',
    'eu-central-1': 'Frankfurt',
}

AWS_DIR = os.path.abspath(os.path.dirname(__file__))
MINING_ADDRS_PATH = os.path.abspath(os.path.join(
    AWS_DIR, '..', '..', 'tools', 'address-list.txt'))
NODES_PATH = os.path.abspath(os.path.join(AWS_DIR, 'nodes.txt'))
# SETUP_INSTANCE_SCRIPT_PATH = os.path.join(AWS_DIR, "setup-instance.sh")

# paths for outputs (dstat data and log)
DATA_PATH = os.path.join(AWS_DIR, '..', 'data')
LOG_PATH = os.path.join(AWS_DIR, '..', 'log')


AMI_IMAGE_ID_PER_REGION: Dict[str, str] = {}

NUM_NODES = 256

################################################################
# utils


def get_instance_count_per_region(regions=REGIONS):
    instance_count_per_region = dict()
    _t_num_nodes = NUM_NODES
    while _t_num_nodes:
        for r in REGIONS:
            if r in instance_count_per_region:
                instance_count_per_region[r] += 1
            else:
                instance_count_per_region[r] = 1
            _t_num_nodes -= 1
            if _t_num_nodes == 0:
                break
    return instance_count_per_region


def get_mining_addrs():
    addrs = list()
    with open(MINING_ADDRS_PATH, 'r') as f:
        for i in range(NUM_NODES):
            line = f.readline()
            addr = line.strip()
            addrs.append(addr)
    return addrs


def Popen(args, stdout=subprocess.PIPE, stderr=subprocess.PIPE, **kwargs):
    p = subprocess.Popen(args, stdout=stdout, stderr=stderr, **kwargs)
    output, _ = p.communicate()
    assert p.returncode == 0, f"failed to execute {args}"
    return output.decode().strip()


def load_ami_image_ids():
    if AMI_IMAGE_ID_PER_REGION:
        return
    print()
    for region in REGIONS:
        print(
            f"{REGIONS[region] + ':': <41} search for amazon machine image... ", end="", flush=True)
        images = ec2[region].images.filter(
            Owners=['amazon'],
            Filters=[{
                'Name': 'name',
                'Values': ['amzn2-ami-hvm-2.0.????????.1-x86_64-gp2'],
            }])
        image = sorted(images, key=lambda i: i.creation_date)[-1]
        AMI_IMAGE_ID_PER_REGION[region] = image.id
        print(f"done ({image.id})")


####################################################################################
# Security group, key pair and node list stuff

def assign_security_group_to_all_instances(group_name):
    # directly set at launch of instance for now
    raise NotImplementedError


def create_or_update_security_groups():
    for region, region_name in REGIONS.items():
        for g in ec2[region].security_groups.all():
            if g.group_name == 'orazor':
                print(f"{region_name}: deleting security group...",
                      end='', flush=True)
                g.delete()
                print(" done")

        print(f"{region_name}: creating new security group...",
              end='', flush=True)
        g = ec2[region].create_security_group(
            GroupName='orazor', Description='ORazor security group (script generated)')
        print(" done")

        print(f"{region_name}: updating permissions...", end='', flush=True)
        g.authorize_ingress(
            FromPort=22,
            ToPort=22,
            IpProtocol='tcp',
            CidrIp='0.0.0.0/0',
        )
        g.authorize_ingress(
            FromPort=18555,
            ToPort=18555,
            IpProtocol='tcp',
            CidrIp='0.0.0.0/0',
        )
        g.authorize_ingress(
            FromPort=3735,
            ToPort=3735,
            IpProtocol='tcp',
            CidrIp='0.0.0.0/0',
        )
        g.authorize_ingress(
            FromPort=8070,
            ToPort=8070,
            IpProtocol='tcp',
            CidrIp='0.0.0.0/0',
        )
        print(" done")

################################################################
# definitions on instances


class InstanceState(enum.IntEnum):
    PENDING = enum.auto()
    RUNNING = enum.auto()
    SHUTTING_DOWN = enum.auto()
    TERMINATED = enum.auto()
    STOPPING = enum.auto()
    STOPPED = enum.auto()

    @staticmethod
    def parse(name):
        return InstanceState[name.upper().replace("-", "_")]


class Instance:
    def __init__(self, id: str, region: str, dnsname: str = None, state: InstanceState = None):
        self.id = id
        self.region = region
        self.ssh_ok = False
        self.status = None
        self.state = state
        self.raw_info = None
        self.raw_status = None
        self.dnsname = dnsname or None
        self.public_ip = None

    @property
    def state(self):
        return self._state

    @state.setter
    def state(self, value):
        if value == InstanceState.RUNNING:
            self.ssh_ok = self.ssh_ok
        else:
            self.ssh_ok = False
        self._state = value

    def load_properties(self, instance_dict, status_dict):
        if self.id:
            assert self.id == instance_dict['InstanceId']
        else:
            self.id = instance_dict['InstanceId']
        self.raw_info = instance_dict
        self.raw_status = status_dict
        self.dnsname = instance_dict['PublicDnsName']
        self.state = InstanceState.parse(instance_dict['State']['Name'])
        self.status = status_dict['InstanceStatus']['Status'] if status_dict else None

    def __repr__(self):
        return (
            f"Instance(id='{self.id}', region='{self.region}', dnsname='{self.dnsname}', "
            f"state='{self.state.name}', ssh_ok='{self.ssh_ok}')"
        )


class Instances:
    def __init__(self, instances_dict: Dict[str, Instance] = None):
        self._instances_dict: Dict[str, Instance] = instances_dict or {}

    @property
    def ids(self):
        return [key for key in self._instances_dict]

    @property
    def all(self):
        return self

    @property
    def running(self):
        return Instances({i.id: i for i in self._instances_dict.values() if i.state == InstanceState.RUNNING})

    @property
    def pending(self):
        return Instances({i.id: i for i in self._instances_dict.values() if i.state == InstanceState.PENDING})

    @property
    def stopped(self):
        return Instances({i.id: i for i in self._instances_dict.values() if i.state == InstanceState.STOPPED})

    @property
    def stopping(self):
        return Instances({i.id: i for i in self._instances_dict.values() if i.state == InstanceState.STOPPING})

    @property
    def terminated(self):
        return Instances({i.id: i for i in self._instances_dict.values() if i.state == InstanceState.TERMINATED})

    def __len__(self):
        return len(self._instances_dict)

    def __getitem__(self, index_or_key: Union[int, str]):
        if isinstance(index_or_key, int):
            if index_or_key < 0:
                index_or_key += len(self._instances_dict)
            for i, value in enumerate(self._instances_dict.values()):
                if i == index_or_key:
                    return value
            raise IndexError
        else:
            return self._instances_dict[index_or_key]

    def __setitem__(self, key: str, value: Instance):
        if key in self._instances_dict:
            raise KeyError(
                'Cannot set the same instance twice, use refresh_infos() to update the existing instance.')
        self._instances_dict[key] = value

    def __repr__(self):
        return repr(list(self._instances_dict.values()))

    def get(self, key, default=None):
        return self._instances_dict.get(key, default)

    def by_region(self, return_dict=False):
        d = collections.defaultdict(Instances)
        for item in self._instances_dict.values():
            d[item.region][item.id] = item
        if return_dict:
            return d
        return d.items()

    def get_by_region(self, region):
        return [i for i in self if i.region == region]

    def lookup(self, what):
        if isinstance(what, Instances):
            return what
        if isinstance(what, Instance):
            return Instances({what.id: what})
        if isinstance(what, str) or isinstance(what, int):
            i = self[what]
            return Instances({i.id: i})
        if isinstance(what, Iterable):
            d = {}
            for x in what:
                i = x if isinstance(x, Instance) else self[x]
                d[i.id] = i
            return Instances(d)
        raise TypeError()

    def get_id(self, dnsname):
        for i in self:
            if i.dnsname == dnsname:
                return i.id
        raise ValueError(f"no instance with dnsname {dnsname} found")

    def status(self):
        self.refresh()
        print(f"number of running instances: {len(self.running)}")
        for x, v in self.running.by_region():
            print(f"    {x} {REGIONS[x]}: {len(v)}")

    def refresh(self, what=None):
        """ Uses the AWS API to refresh the information for all instances, considering all regions.
            If the parameter 'what' is provided, only the specified instances are queried.
        """
        if what is None:
            what = self
            grouped_instances = zip(REGIONS, itertools.repeat(None))
        else:
            what = self.lookup(what)
            grouped_instances = what.by_region()

        for region, instances_per_region in grouped_instances:
            ids = [] if instances_per_region is None else instances_per_region.ids
            infos = []
            for reservation in ec2_clients[region].describe_instances(InstanceIds=ids)['Reservations']:
                for i in reservation['Instances']:
                    infos.append(i)
            statuses = {}
            for status in ec2_clients[region].describe_instance_status(InstanceIds=ids)['InstanceStatuses']:
                statuses[status['InstanceId']] = status
            for info in infos:
                i = what.get(info['InstanceId'])
                if not i:
                    assert self.get(info['InstanceId']
                                    ) is None and what is self
                    i = Instance(info['InstanceId'], region)
                    self[i.id] = i
                i.load_properties(info, statuses.get(i.id))
        self.load_peers()

    def refresh_until(self, break_condition: Callable[[], bool], verbose: bool = True):
        while not break_condition():
            time.sleep(REFRESH_INTERVAL)
            self.refresh()
            if verbose:
                print(end=".", flush=True)

    def load_peers(self):
        for i in self.running:
            i.public_ip = socket.gethostbyname(i.dnsname)

    def get_peers(self):
        return [f"{i.public_ip}:18555" for i in self.running]

    def save_peers(self):
        current_path = pathlib.Path().resolve()
        with open(current_path / 'public_ips.txt', 'w') as f:
            for i in self.running:
                f.write(f"{i.public_ip}\n")

    def wait_for_startup(self, what=None):
        what = self if what is None else self.lookup(what)
        print(f"waiting for startup...", end='', flush=True)
        self.refresh_until(lambda: all(i.ssh_ok for i in what))
        print(" done")

    def stop(self, what=None):
        what = self.running if what is None else self.lookup(what)
        if not all(i.state == InstanceState.RUNNING for i in what):
            raise ValueError("instance(s) in invalid state")
        print(
            f"stopping instance(s): {', '.join(what.ids)}...", end='', flush=True)
        for region, self in what.by_region():
            ec2_clients[region].stop_instances(InstanceIds=self.ids)
            for i in self:
                i.state = InstanceState.STOPPING

        self.refresh_until(
            lambda: all(
                i.state in [InstanceState.STOPPED, InstanceState.TERMINATED] for i in what)
        )
        print(" done")

    def terminate(self, what=None):
        if what is None:
            what = [i for i in self if i.state != InstanceState.TERMINATED]
        what = self.lookup(what)
        print(
            f"terminating instance(s): {', '.join(what.ids)}...", end='', flush=True)
        for region, self in what.by_region():
            ec2_clients[region].terminate_instances(
                InstanceIds=self.ids)
            for i in self:
                i.state = InstanceState.SHUTTING_DOWN

        self.refresh_until(lambda: all(
            i.state == InstanceState.TERMINATED for i in what))
        print(" done")

    def create(self):
        instance_count_per_region = get_instance_count_per_region()

        print("launch initiated, aiming to launch the following instances:")
        print()
        for k in instance_count_per_region:
            print("{:<20s}{:>10d}".format(k, instance_count_per_region[k]))
        print()
        try:
            r = input("type 'y' and press enter to continue: ")
            if r != 'y':
                print('aborted')
                return
        except KeyboardInterrupt:
            print('\naborted')
            return

        print()
        print("performing launch...")
        print()

        # find image ids
        load_ami_image_ids()

        # load instance script
        # with open(SETUP_INSTANCE_SCRIPT_PATH, 'r') as f:
        #     setup_instance_script = f.read()
        setup_instance_script = ""

        # launch instances region by region
        for region, num_instances in instance_count_per_region.items():
            if num_instances > 0:
                Instances._create(region, num_instances,
                                  setup_instance_script)

        self.refresh()

    @classmethod
    def _create(cls, region, num_instances, setup_instance_script, instance_type='t2.micro'):
        assert instance_type in ['t2.nano',
                                 't2.micro', 't2.small', 't2.medium']

        print(
            f"    {REGIONS[region] + ':': <41} launching {num_instances: >3} instances... ", end="", flush=True)
        result = ec2[region].create_instances(
            ImageId=AMI_IMAGE_ID_PER_REGION[region],
            InstanceType='t2.micro',
            KeyName='ebft',
            MinCount=num_instances,
            MaxCount=num_instances,
            UserData=setup_instance_script,
            SecurityGroups=['orazor'],
            InstanceInitiatedShutdownBehavior='terminate',
            IamInstanceProfile={
                'Arn': 'arn:aws:iam::350454502080:instance-profile/SystemManager',
                # 'Name': 'SystemManager',
            },
        )
        print("done")
        return result


class Operator:
    def __init__(self, instances, ssm_clients):
        self.instances = instances.running
        self.ssm_clients = ssm_clients
        self.ssh_client: pssh.clients.ParallelSSHClient = None

    def _run_command(self, cmds):
        command_ids = dict()
        instances_dict = self.instances.by_region(
            return_dict=True)
        # launch the command one by one
        i = 0
        for region in REGIONS:
            instance_ids = instances_dict[region].ids
            for instance_id in instance_ids:
                print("Executing the command below on instance %s:\n %s" %
                      (instance_id, cmds[i]))
                response = self.ssm_clients[region].send_command(
                    InstanceIds=[instance_id],
                    DocumentName="AWS-RunShellScript",
                    Parameters={'commands': [cmds[i]]},
                )
                command_id = response['Command']['CommandId']
                command_ids[instance_id] = command_id
                i += 1
        return command_ids

    def _run_command_blocking(self, cmds):
        cids = self._run_command(cmds)
        outputs = self._get_outputs(cids)
        while True:
            finished = True
            for iid, out in outputs.items():
                if out['ResponseCode'] == -1:
                    finished = False
            if finished == True:
                break
            else:
                time.sleep(5)
                outputs = self._get_outputs(cids)
        return outputs

    def _run_same_command(self, cmd):
        command_ids = dict()
        instances_dict = self.instances.by_region(
            return_dict=True)
        # launch the command one by one
        for region in REGIONS:
            instance_ids = instances_dict[region].ids
            print("Executing the command below on instance %s:\n %s" %
                  (instance_ids, cmd))
            response = self.ssm_clients[region].send_command(
                InstanceIds=instance_ids,
                DocumentName="AWS-RunShellScript",
                Parameters={'commands': [cmd]},
            )
            command_id = response['Command']['CommandId']
            command_ids[region] = command_id

        return command_ids

    def _run_same_command_blocking(self, cmd):
        cids = self._run_same_command(cmd)
        outputs = self._get_outputs(cids)
        while True:
            finished = True
            for iid, out in outputs.items():
                if out['ResponseCode'] == -1:
                    finished = False
            if finished == True:
                break
            else:
                time.sleep(5)
                outputs = self._get_outputs(cids)
        return outputs

    def _get_outputs(self, command_ids):
        outputs = dict()
        for region in REGIONS:
            instanceIds = self.instances.by_region(
                return_dict=True)[region].ids
            for iid in instanceIds:
                output = self.ssm_clients[region].get_command_invocation(
                    CommandId=command_ids[iid],
                    InstanceId=iid,
                )
                outputs[iid] = output
        return outputs

    def _get_benchmark_cmds(self, extension, committee_size, latency, minerblocksize, block_interval=4, epoch_size=1):
        mining_addrs = get_mining_addrs()
        peers = self.instances.get_peers()
        if extension == 'syncebft' or extension == 'psyncebft':
            peers_str = ' '.join(['--connect=%s' %
                                  x for x in peers])
            cmds = [
                f'/home/ec2-user/main.sh {extension} {committee_size} {latency} {mining_addr} {minerblocksize*1024} {epoch_size} {peers_str}' for mining_addr in mining_addrs]
            # in the last cmd, further insert simulated-miner
            cmds[-1] += f' & sleep 10 & nohup /home/ec2-user/simulated-miner.sh 10000 {block_interval*latency} {committee_size} > /home/ec2-user/simulated-miner.log &'
        else:
            cmds = list()
            for mining_addr in mining_addrs:
                random_peers = random.choices(peers, k=8)
                peers_str = ' '.join(['--connect=%s' %
                                      x for x in random_peers])
                cmd = f'/home/ec2-user/main.sh simnet {committee_size} {latency} {mining_addr} {minerblocksize*1024} {epoch_size} {peers_str}'
                cmds.append(cmd)
            cmds[-1] += f' & sleep 10 & nohup /home/ec2-user/simulated-miner.sh 10000 {block_interval*latency} {committee_size} > /home/ec2-user/simulated-miner.log &'

        return cmds

    def refresh(self):
        self.instances.status()
        self.instances = self.instances.running

    def if_deployed(self):
        cmd = "ls /home/ec2-user/main.sh"
        cmds = [cmd] * NUM_NODES
        outputs = self._run_command_blocking(cmds)
        for iid, out in outputs.items():
            if out['ResponseCode'] == 0:
                print(f"{out['InstanceId']}: Deployed")
            else:
                print(f"{out['InstanceId']}: Not deployed yet, please wait")
        print()

    def run_benchmark(self, extension, committee_size, latency, minerblocksize, block_interval, epoch_size):
        self.stop_benchmark()
        self.clean()
        time.sleep(5)

        print("starting nodes")
        cmds = self._get_benchmark_cmds(
            extension, committee_size, latency, minerblocksize, block_interval, epoch_size)
        self._run_command(cmds)

        # print("starting miners")
        # mining_cmd = f'nohup /home/ec2-user/simulated-random-miner.sh 10000 {block_interval*latency*committee_size} {committee_size} > /home/ec2-user/simulated-miner.log'
        # self._run_same_command(mining_cmd)

        print("done")

    def stop_benchmark(self, blocking=False):
        print("Killing processes")
        cmd = "pkill -9 -f btcd & pkill -9 -f dstat & pkill -9 -f simulated-miner.sh"
        if blocking == False:
            self._run_command([cmd] * NUM_NODES)
        else:
            outputs = self._run_command_blocking([cmd] * NUM_NODES)
            for iid, out in outputs.items():
                if out['ResponseCode'] == 0:
                    print("Done at %s" % iid)
                else:
                    print("Error (or already done) at %s: %s" %
                          iid, out['StandardOutputContent'])

    def collect_logs(self, extension, committee_size, latency, minerblocksize, block_interval=4, epoch_size=1):
        os.makedirs(LOG_PATH, exist_ok=True)
        log_dir = f"{LOG_PATH}/{extension}_{committee_size}_{latency}_{minerblocksize}_{block_interval}_{epoch_size}"
        os.makedirs(log_dir, exist_ok=True)
        print("Collecting logs")
        procs = []
        for idx, i in enumerate(self.instances.running):
            for remote_path in ['/home/ec2-user/main.log', '/home/ec2-user/stats.csv']:
                file_fullpath = f'{log_dir}/{i.dnsname}_{remote_path.split("/")[-1]}'
                if os.path.exists(file_fullpath):
                    # if exist, remove the file
                    os.remove(file_fullpath)
                print(
                    f"Downloading {remote_path} from {i.dnsname+'...': <65} {idx + 1}/{len(self.instances.running)} ")
                # Filename: dnsname_blocktime_numnodes_numminers_{stats.csv/main.log}
                cmd = ' '.join([
                    f'rsync -z -e "ssh -i ~/.ssh/ebft.pem -oStrictHostKeyChecking=accept-new"',
                    f'ec2-user@{i.dnsname}:{remote_path}',
                    f'"{file_fullpath}"',
                ])
                proc = subprocess.Popen(
                    cmd, shell=True, stderr=subprocess.DEVNULL)
                procs.append(proc)

        # collect status of all subprocesses
        outputs = [p.wait() for p in procs]
        print("Done")

    def clean(self, blocking=False):
        print("Removing everything...")
        cmd = "rm -rf /home/ec2-user/stats.csv /home/ec2-user/main.log /home/ec2-user/simulated-miner.log /home/ec2-user/.local/share/btcd/ /root/.btcd/"
        if blocking == False:
            self._run_command([cmd] * NUM_NODES)
        else:
            outputs = self._run_command_blocking([cmd] * NUM_NODES)
            for iid, out in outputs.items():
                if out['ResponseCode'] == 0:
                    print("Done at %s" % iid)
                else:
                    print("Error (or already done) at %s: %s" %
                          iid, out['StandardOutputContent'])


if __name__ == '__main__':
    if len(sys.argv) >= 2:
        NUM_NODES = int(sys.argv[1])
        print("NUM_NODES = %d" % NUM_NODES)
    else:
        print('please specify arguments')
        exit(0)

    ec2 = {
        region: boto3.resource("ec2", region_name=region) for region in REGIONS
    }
    ec2_clients = {
        region: boto3.client("ec2", region_name=region) for region in REGIONS
    }
    ssm_clients = {
        region: boto3.client("ssm", region_name=region) for region in REGIONS
    }

    # security group
    # create_or_update_security_groups()

    instances = Instances()
    # instances.create()

    # instances.status()
    # instances.running

    # instances.stop()
    # instances.terminate()
