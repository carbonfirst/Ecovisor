from os import wait
import os
import pylxd
from pylxd import Client
import logging
from Job import Container
import time
import six
import re
import platform

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

_architectures = {
    'unknown': '0',
    'i686': '1',
    'x86_64': '2',
    'armv7l': '3',
    'aarch64': '4',
    'ppc': '5',
    'ppc64': '6',
    'ppc64le': '7',
    's390x': '8'
}

_architecture_alias = {
    'aarch64': 'arm64',
    'arm64': 'arm64',
    'x86_64': 'amd64',
    'amd64': 'amd64' 
}

class RMAdapter():
    def __init__(self, endpoint, cert=None, key=None, orchestrator="LXD", cores_per_node=4):
        logging.basicConfig(format="[%(levelname)s] %(asctime)s %(message)s")
        self.endpoint = endpoint
        self.certicate = cert
        self.key = key
        self.orchestrator = orchestrator
        self.cores_per_node = cores_per_node
        self.client = self.connect(self.endpoint, self.certicate, self.key)
        self.containers = {}
        self.default_quantum = 512.0 # in ms. Max allowed is 1024 as per the Linux Kernel.
        self.default_cpu = 1.0
        self.alias_architecture = _architecture_alias[self.checkArchitecture()]

    def checkArchitecture(self):
        return platform.machine()
    
    def connect(self, endpoint, cert=None, key=None):
        if cert == None:
            try:
                client = Client(endpoint=endpoint, verify=False)
                return client
            except Exception as e:
                logger.error("Error connecting to LXD cluster (without verification): {}".format(e))
                return Client()
        else:
            try:
                client = Client(endpoint=endpoint, cert=(cert, key), verify=False)
                return client
            except Exception as e:
                logger.error("Error connecting to LXD cluster (with verification): {}".format(e))
                return Client()

    def update_instances(self, instance=None, delete=False):
        try:
            container_list = self.client.containers.all()
            for container in container_list:
                if container.name not in self.containers:
                    print("creating container and adding to self.containers: {}".format(container.name))
                    container_obj = Container(container.name)
                    self.containers[container_obj.name] = container_obj
                else:
                    if delete == True:
                        print("deleting container: {}".format(container.name))
                        if container == instance:
                            self.containers.pop(instance)
        except Exception as e:
            logger.error("Error querying containers for {}: {}".format(self.orchestrator, e))

    def get_all_containers(self, update=False):
        try:
            if self.containers == None or update == True:
                self.update_instances()
            return self.containers
        except Exception as e:
            logger.error("Error while fetching {} containers [UPDATE FLAG: {}]: {}".format(self.orchestrator, update, e))
            return False

    def get_instance(self, container):
        try:
            #inst = self.client.containers.get(container)
            inst = self.containers[container]
            return inst
        except Exception as e:
            #logger.error("Error while fetching container {}: {}".format(container, e))
            return False

    def parse_cpu_str(self, cpu_allowance_string, cores_per_con, con_var_cpu_util, time_interval=1.0, relative=False):
        con_relative_cpu_util = None
        try:
            relative_allowance_str = cpu_allowance_string.split('/')
            if len(relative_allowance_str) == 2:
                if relative == True:
                    max_cpu_allowance_time = float(relative_allowance_str[0].replace('ms', '')) / float(relative_allowance_str[1].replace('ms', '')) * time_interval
                    con_relative_cpu_util = float(con_var_cpu_util / max_cpu_allowance_time)
                else:
                    con_relative_cpu_util = float(con_var_cpu_util / self.cores_per_node * time_interval)
            else:
                relative_allowance_str = cpu_allowance_string.replace('%', '')
                if relative == True:
                    max_cpu_allowance_time = float(relative_allowance_str) * 0.01 * cores_per_con
                else:
                    max_cpu_allowance_time = float(relative_allowance_str) * 0.01 * self.cores_per_node
                con_relative_cpu_util = float(con_var_cpu_util / max_cpu_allowance_time)
            return con_relative_cpu_util
        except Exception as e:
            logger.error("Error while parsing container CPU limits: {}".format(e))
            return con_relative_cpu_util

    def get_instance_allocation(self, instance, cores_per_node=32):
        try:
            con = self.client.containers.get(instance)
            con_alloc = 0.0
            if con:
                if con.status_code == 103: # Too LXD specific. Change it in future.
                    if 'limits.cpu' in con.config and 'limits.cpu.allowance' not in con.config:
                        con_alloc = float(con.config['limits.cpu'])
                    if 'limits.cpu' not in con.config and 'limits.cpu.allowance' in con.config:
                        con_alloc = self.parse_cpu_str(con.config['limits.cpu.allowance'], cores_per_node, cores_per_node, relative=False)
                    if 'limits.cpu' in con.config and 'limits.cpu.allowance' in con.config:
                        con_alloc = self.parse_cpu_str(con.config['limits.cpu.allowance'], float(con.config['limits.cpu']), cores_per_node, relative=False)
                    if 'limits.cpu' not in con.config and 'limits.cpu.allowance' not in con.config:
                        con_alloc = cores_per_node
            else:
                    status = con.status_code
                    logger.warn("Instance {} is not running. State: {}".format(instance, status))
            return con_alloc
        except Exception as e:
            logger.error("Error while fetching {}'s CPU % information: {}".format(instance, e))
            return con_alloc

    def get_cpu_util(self, instance, cores_per_node=32, relative=False):
        try:
            con = self.client.containers.get(instance)
            #con = self.container_get(container.name)
            #logger.info("Getting CPU for {}".format(con))
            con_relative_cpu_util = 0.0
            container = self.get_instance(instance)
            if con:
                if con.status_code == 103: # Too LXD specific. Change it in future.
                    con_curr_query_time = time.time()
                    con_metadata = self.client.api.containers[instance].state.get().json()['metadata']
                    con_last_cpu_util = container.last_cpu_util
                    con_curr_cpu_util = float(con_metadata['cpu']['usage']) / 1000000000 # 1ns = 1^e-9s
                    con_var_cpu_util = max(0.0, float(con_curr_cpu_util - con_last_cpu_util))
                    con_last_query_time = container.last_query_time
                    time_interval = con_curr_query_time - con_last_query_time
                    container.last_query_time = con_curr_query_time
                    container.last_cpu_util = con_curr_cpu_util

                    if 'limits.cpu' in con.config and 'limits.cpu.allowance' not in con.config:
                        max_cpu_time = time_interval * float(con.config['limits.cpu'])
                        con_relative_cpu_util = float(con_var_cpu_util / max_cpu_time)
                    if 'limits.cpu' not in con.config and 'limits.cpu.allowance' in con.config:
                        con_relative_cpu_util = self.parse_cpu_str(con.config['limits.cpu.allowance'], cores_per_node, con_var_cpu_util, time_interval, relative)
                    if 'limits.cpu' in con.config and 'limits.cpu.allowance' in con.config:
                        con_relative_cpu_util = self.parse_cpu_str(con.config['limits.cpu.allowance'], float(con.config['limits.cpu']), con_var_cpu_util, time_interval, relative)
                    if 'limits.cpu' not in con.config and 'limits.cpu.allowance' not in con.config:
                        max_cpu_time = time_interval * cores_per_node
                        con_relative_cpu_util = float(con_var_cpu_util / max_cpu_time)
                else:
                    status = con.status_code
                    logger.warn("Instance {} is not running. State: {}".format(container, status))
            return con_relative_cpu_util
        except Exception as e:
            logger.error("Error while fetching {}'s CPU \% information: {}".format(container, e))


    def is_part_of_cluster(self):
        try:
            member = self.client.cluster.get().members.all()
            if member == None:
                return False
            else:
                return True
        except:
            print("Issue while fetching cluster details")
            return False


    def parse_container(self, container, image, pull_server, protocol, instance_hosts=None, devices=None, env_vars=[], cpu_quota=None, cpu_max_time=None, default_dir='/mnt/nfs/lxd/'):
        envvars = None
        if cpu_max_time == None:
            cpu_max_time = self.default_quantum
        if cpu_quota == None:
            cpu_quota = self.default_cpu
        config = {}
        config['name'] = container
        config['config'] = {
                    'limits.cpu.allowance': str(str(int(float(cpu_quota) * cpu_max_time)) + 'ms/' + str(int(cpu_max_time)) + 'ms'),
                    #'raw.apparmor': 'mount fstype=nfs,nfs4,nfsd,rpc_pipefs,', # Obelix
                    'security.privileged': "true", # Jetson
                    'environment.LXD_HOSTS': instance_hosts,
                    'environment.lxd_hosts.blast.env': None
                }
        config['devices'] = devices
        if pull_server is not None and protocol is not None:
            logger.info("Pulling server {} using protocol {}".format(pull_server, protocol))
            config['source'] = {
                            'type': 'image', 
                            'mode': 'pull', 
                            'server': pull_server, 
                            'protocol': protocol, 
                            'alias': 'lts/' + self.alias_architecture
                        }
        else:
            logger.info("Fetching image {}".format(image))
            config['source'] = {
                    'type': 'image', 
                    'protocol': 'lxd', 
                    'alias': image
                }

        if isinstance(env_vars, list):
            if (env_vars and
                'key' in env_vars[0] and
                'value' in env_vars[0]):
                envvars = {d['key']: d['value'] for d in env_vars}
            else:
                envvars = {}

            if envvars is not None:
                for k, v in six.iteritems(envvars):
                    if v == "$LXD_HOSTS":
                        v = instance_hosts
                    #if bool(re.match(r"\$HOST-[0-9]+", v)): # If value of a key matches instance $HOST-X, set key=$HOST-X
                    #    host_num = int(v.split('-')) - 1
                    #    v = instance_hosts.split(',')[host_num]

                    # The following is mostly useful for cases when we cannot update specific env. vars. of processes
                    if os.path.isfile(default_dir + k): # If a key in default_dir is a file... 
                        env_file = open(default_dir + k, "r+",)
                        env_cont = env_file.readlines()
                        key = env_cont[0].strip().split("=")[0] # ... complete the file's 1st line...
                        env_file.seek(0)
                        env_file.write(key + "=" + v) # ... with v 
                        env_file.truncate()
                    else:
                        config['config']['environment.'+ k] = v
        logger.info("Instance with config: {}".format(config))
        return config

    def create_new_instance(self, container, cpu_quota, image, pull_server, protocol, instance_hosts, devices, env_vars=[], wait=True, update=True):
        try:
            # parse container config to a dict
            # def parse_container(self, container, image, instance_hosts, env_vars=[], cpu_quota=None, cpu_max_time=None, default_dir='/mnt/nfs/lxd/'):
            container_config = self.parse_container(container, image, pull_server, protocol, instance_hosts, devices, env_vars, cpu_quota)
            try:
                instance = self.client.containers.get(container)
            #if instance:
                logger.info("Instance already exists")
                self.update_instance_allocation(container, cpu_quota)
            except Exception as e:
                if self.client == None:
                    client = Client()
                    instance = client.instances.create(container_config, wait=wait)
                else:
                    instance = self.client.instances.create(container_config, wait=wait)
            if update:
                self.update_instances()
            return instance
        except Exception as e:
            logger.error("Error while creating instance {}:\n\n{}".format(container, e))
            return None

    def update_instance_allocation(self, container, cpu_quota):
        return self.container_config_set(container, 'limits.cpu.allowance', str(str(int(float(cpu_quota) * self.default_quantum)) + 'ms/' + str(int(self.default_quantum)) + 'ms'))

    def get_instance_ip(self, instance, wait=True):
        inst = self.client.containers.get(instance)
        inst_ip = inst.state().network['eth0']['addresses'][0]['address']
        if (wait == True) and (inst_ip == None or inst_ip == ''):
            time.sleep(1) # Arbitrary time before requesting again
            return self.get_instance_ip(instance, True)
        return inst_ip

    def start_instance(self, container, wait=True, update=False):
        try:
            instance = self.container_start(container, self.endpoint, wait=wait)
            if update:
                self.update_instances()
            return instance
        except Exception as e:
            logger.error("Error while starting container {}: {}".format(container, e))
            #instance = self.get_instance(container)
            return None

    def container_start(self, name, remote_addr=None,
                    cert=None, key=None, verify_cert=False, wait=True):
 
        container = self.container_get(
            name, remote_addr, cert, key, verify_cert, _raw=True
        )
        container.start(wait=wait)
        return _pylxd_model_to_dict(container)

    def stop_instance(self, container_name, timeout=30, wait=True, update=True, force=True):
        try:
            #instance = self.get_instance(container_name)
            if update:
                self.update_instances(container_name, delete=True)
            instance = self.client.containers.get(container_name)
            instance.stop(timeout, force, wait=wait)
            return instance
        except Exception as e:
            logger.error("Error while stopping instance {}: {}".format(container_name, e))
            return None

    def container_get(self, name=None, remote_addr=None,
                  cert=None, key=None, verify_cert=False, _raw=False):

        #client = pylxd_client_get(remote_addr, cert, key, verify_cert)
        client = self.client

        if name is None:
            containers = client.containers.all()
            if _raw:
                return containers
        else:
            containers = []
            try:
                containers = [client.containers.get(name)]
            except pylxd.exceptions.LXDAPIException:
                logger.error(
                    'Container \'{0}\' not found'.format(name)
                )
                return None
            if _raw:
                return containers[0]

        infos = []
        for container in containers:
            infos.append(dict([
                (container.name, _pylxd_model_to_dict(container))
            ]))
        return infos

    def container_create(self, name, source, profiles=None,
                     config=None, devices=None, architecture='x86_64',
                     ephemeral=False, wait=True,
                     remote_addr=None, cert=None, key=None, verify_cert=True,
                     _raw=False):

        if profiles is None:
            profiles = ['default']

        if config is None:
            config = {}

        if devices is None:
            devices = {}

        client = pylxd_client_get(remote_addr, cert, key, verify_cert)

        if not isinstance(profiles, (list, tuple, set,)):
            logger.error(
            "'profiles' must be formatted as list/tuple/set."
            )

        if architecture not in _architectures:
            logger.error(
                ("Unknown architecture '{0}' "
                "given for the container '{1}'").format(architecture, name)
            )

        if isinstance(source, six.string_types):
            source = {'type': 'image', 'alias': source}

            config, devices = normalize_input_values(
            config,
            devices
        )

        try:
            container = self.client.containers.create(
            {
                'name': name,
                'architecture': _architectures[architecture],
                'profiles': profiles,
                'source': source,
                'config': config,
                'ephemeral': ephemeral
            },
            wait=wait
        )
        except pylxd.exceptions.LXDAPIException as e:
            logger.error(
            six.text_type(e)
        )

        if not wait:
            return container.json()['operation']

        # Add devices if not wait and devices have been given.
        if devices:
            for dn, dargs in six.iteritems(devices):
                self.container_device_add(name, dn, **dargs)

        if _raw:
            return container

        return _pylxd_model_to_dict(container)

    def container_device_add(self, name, device_name, device_type='disk',
                         remote_addr=None,
                         cert=None, key=None, verify_cert=True,
                         **kwargs):
        container = self.container_get(
            name, remote_addr, cert, key, verify_cert, _raw=True
        )

        kwargs['type'] = device_type
        return _set_property_dict_item(
            container, 'devices', device_name, kwargs
        )

    def container_execute(self, name, cmd, remote_addr=None,
                      cert=None, key=None, verify_cert=False):
        '''
        Execute a command list on a container.
        name :
            Name of the container
        cmd :
            Command to be executed (as a list)
            Example :
                '["ls", "-l"]'
        '''
        container = self.container_get(
            name, remote_addr, cert, key, verify_cert, _raw=True
        )
        try:
            result = container.execute(cmd)
            result_output = {}
            if not hasattr(result, 'exit_code'):
                result_output = dict(
                    exit_code=0,
                    stdout=result[0],
                    stderr=result[1],
                )
            else:
                result_output = dict(
                    exit_code=result.exit_code,
                    stdout=result.stdout,
                    stderr=result.stderr,
                )
        except pylxd.exceptions.NotFound as e:
            # TODO: Using exit_code 0 here is not always right.
            # See: https://github.com/lxc/pylxd/issues/280
            result_output = dict(exit_code=0, stdout="", stderr=six.text_type(e))

        if int(result_output['exit_code']) > 0:
            result_output['result'] = False
        else:
            result_output['result'] = True

        return result_output

    def container_config_get(self, name, config_key, remote_addr=None,
                         cert=None, key=None, verify_cert=False):
        '''
        Get a container config value
        name :
            Name of the container
        config_key :
            The config key to retrieve
        '''
        container = self.container_get(
            name, remote_addr, cert, key, verify_cert, _raw=True
        )
        return _get_property_dict_item(container, 'config', config_key)


    def container_config_set(self, name, config_key, config_value, remote_addr=None,
                            cert=None, key=None, verify_cert=False):
        '''
        Set a container config value
        name :
            Name of the container
        config_key :
            The config key to set
        config_value :
            The config value to set
        '''
        container = self.container_get(
            name, remote_addr, cert, key, verify_cert, _raw=True
        )

        return _set_property_dict_item(
            container, 'config', config_key, config_value
        )


    def container_config_delete(self, name, config_key, remote_addr=None,
                                cert=None, key=None, verify_cert=False):
        '''
        Delete a container config value
        name :
            Name of the container
        config_key :
            The config key to delete
        '''
        container = self.container_get(
            name, remote_addr, cert, key, verify_cert, _raw=True
        )

        return _delete_property_dict_item(
            container, 'config', config_key
        )


    def container_device_get(self, name, device_name, remote_addr=None,
                            cert=None, key=None, verify_cert=False):
        '''
        Get a container device
        name :
            Name of the container
        device_name :
            The device name to retrieve
        '''
        container = self.container_get(
            name, remote_addr, cert, key, verify_cert, _raw=True
        )

        return _get_property_dict_item(container, 'devices', device_name)


    def container_device_add(self, name, device_name, device_type='disk',
                            remote_addr=None,
                            cert=None, key=None, verify_cert=False,
                            **kwargs):
        '''
        Add a container device
        name :
            Name of the container
        device_name :
            The device name to add
        device_type :
            Type of the device
        ** kwargs :
            Additional device args
        '''
        container = self.container_get(
            name, remote_addr, cert, key, verify_cert, _raw=True
        )

        kwargs['type'] = device_type
        return _set_property_dict_item(
            container, 'devices', device_name, kwargs
        )


    def container_device_delete(self, name, device_name, remote_addr=None,
                                cert=None, key=None, verify_cert=False):
        '''
        Delete a container device
        name :
            Name of the container
        device_name :
            The device name to delete
        '''
        container = self.container_get(
            name, remote_addr, cert, key, verify_cert, _raw=True
        )

        return _delete_property_dict_item(
            container, 'devices', device_name
        )


    def container_file_put(self, name, src, dst, recursive=False, overwrite=False,
                        mode=None, uid=None, gid=None, saltenv='base',
                        remote_addr=None,
                        cert=None, key=None, verify_cert=False):
        '''
        Put a file into a container
        name :
            Name of the container
        src :
            The source file or directory
        dst :
            The destination file or directory
        recursive :
            Decent into src directory
        overwrite :
            Replace destination if it exists
        mode :
            Set file mode to octal number
        uid :
            Set file uid (owner)
        gid :
            Set file gid (group)
        CLI Example:
        .. code-block:: bash
            salt '*' lxd.container_file_put <container name> /var/tmp/foo /var/tmp/
        '''
        # Possibilities:
        #  (src, dst, dir, dir1, and dir2 are directories)
        #  cp /src/file1 /dst/file1
        #  cp /src/file1 /dst/file2
        #  cp /src/file1 /dst
        #  cp /src/file1 /dst/
        #  cp -r /src/dir /dst/
        #  cp -r /src/dir/ /dst/
        #  cp -r /src/dir1 /dst/dir2 (which is not /src/dir1 /dst/dir2/)
        #  cp -r /src/dir1 /dst/dir2/

        # Fix mode. Salt commandline doesn't use octals, so 0600 will be
        # the decimal integer 600 (and not the octal 0600). So, it it's
        # and integer, handle it as if it where a octal representation.
        mode = six.text_type(mode)
        if not mode.startswith('0'):
            mode = '0{0}'.format(mode)

        container = self.container_get(
            name, remote_addr, cert, key, verify_cert, _raw=True
        )

        src = os.path.expanduser(src)

        if not os.path.isabs(src):
            if src.find('://') >= 0:
                #cached_file = __salt__['cp.cache_file'](src, saltenv=saltenv)
                cached_file = src
                if not cached_file:
                    logger.error("File '{0}' not found".format(src))
                if not os.path.isabs(cached_file):
                    logger.error('File path must be absolute.')
                src = cached_file

        # Make sure that src doesn't end with '/', unless it's '/'
        src = src.rstrip(os.path.sep)
        if not src:
            src = os.path.sep

        if not os.path.exists(src):
            logger.error(
                'No such file or directory \'{0}\''.format(src)
            )

        if os.path.isdir(src) and not recursive:
            logger.error(
                ("Cannot copy overwriting a directory "
                "without recursive flag set to true!")
            )

        try:
            dst_is_directory = False
            container.files.get(os.path.join(dst, '.'))
        except pylxd.exceptions.NotFound:
            pass
        except pylxd.exceptions.LXDAPIException as why:
            if six.text_type(why).find('Is a directory') >= 0:
                dst_is_directory = True

        if os.path.isfile(src):
            # Source is a file
            if dst_is_directory:
                dst = os.path.join(dst, os.path.basename(src))
                if not overwrite:
                    found = True
                    try:
                        container.files.get(os.path.join(dst))
                    except pylxd.exceptions.NotFound:
                        found = False
                    except pylxd.exceptions.LXDAPIException as why:
                        if six.text_type(why).find('not found') >= 0:
                            # Old version of pylxd
                            found = False
                        else:
                            raise
                    if found:
                        logger.error(
                            "Destination exists and overwrite is false"
                        )
            if mode is not None or uid is not None or gid is not None:
                # Need to get file stats
                stat = os.stat(src)
                if mode is None:
                    mode = oct(stat.st_mode)
                if uid is None:
                    uid = stat.st_uid
                if gid is None:
                    gid = stat.st_gid

            with open(src, 'rb') as src_fp:
                container.files.put(
                    dst, src_fp.read(),
                    mode=mode, uid=uid, gid=gid
                )
            return True
        elif not os.path.isdir(src):
            logger.error(
                "Source is neither file nor directory"
            )

        # Source is a directory
        # idx for dstdir = dst + src[idx:]
        if dst.endswith(os.sep):
            idx = len(os.path.dirname(src))
        elif dst_is_directory:
            idx = len(src)
        else:
            # Destination is not a directory and doesn't end with '/'
            # Check that the parent directory of dst exists
            # and is a directory
            try:
                container.files.get(os.path.join(os.path.dirname(dst), '.'))
            except pylxd.exceptions.NotFound:
                pass
            except pylxd.exceptions.LXDAPIException as why:
                if six.text_type(why).find('Is a directory') >= 0:
                    dst_is_directory = True
                    # destination is non-existent
                    # cp -r /src/dir1 /scr/dir1
                    # cp -r /src/dir1 /scr/dir2
                    idx = len(src)
                    overwrite = True

        # Copy src directory recursive
        if not overwrite:
            logger.error(
                "Destination exists and overwrite is false"
            )

        # Collect all directories first, to create them in one call
        # (for performance reasons)
        dstdirs = []
        for path, _, files in os.walk(src):
            dstdir = os.path.join(dst, path[idx:].lstrip(os.path.sep))
            dstdirs.append(dstdir)
        container.execute(['mkdir', '-p'] + dstdirs)

        set_mode = mode
        set_uid = uid
        set_gid = gid
        # Now transfer the files
        for path, _, files in os.walk(src):
            dstdir = os.path.join(dst, path[idx:].lstrip(os.path.sep))
            for name in files:
                src_name = os.path.join(path, name)
                dst_name = os.path.join(dstdir, name)

                if mode is not None or uid is not None or gid is not None:
                    # Need to get file stats
                    stat = os.stat(src_name)
                    if mode is None:
                        set_mode = oct(stat.st_mode)
                    if uid is None:
                        set_uid = stat.st_uid
                    if gid is None:
                        set_gid = stat.st_gid

                with open(src_name, 'rb') as src_fp:
                    container.files.put(
                        dst_name, src_fp.read(),
                        mode=set_mode, uid=set_uid, gid=set_gid
                    )

        return True


    def container_file_get(self, name, src, dst, overwrite=False,
                        mode=None, uid=None, gid=None, remote_addr=None,
                        cert=None, key=None, verify_cert=False):
        '''
        Get a file from a container
        name :
            Name of the container
        src :
            The source file or directory
        dst :
            The destination file or directory
        mode :
            Set file mode to octal number
        uid :
            Set file uid (owner)
        gid :
            Set file gid (group)
        '''
        # Fix mode. Salt commandline doesn't use octals, so 0600 will be
        # the decimal integer 600 (and not the octal 0600). So, it it's
        # and integer, handle it as if it where a octal representation.

        # Do only if mode is not None, otherwise we get 0None
        if mode is not None:
            mode = six.text_type(mode)
            if not mode.startswith('0'):
                mode = '0{0}'.format(mode)

        container = self.container_get(
            name, remote_addr, cert, key, verify_cert, _raw=True
        )

        dst = os.path.expanduser(dst)
        if not os.path.isabs(dst):
            raise logger.error('File path must be absolute.')

        if os.path.isdir(dst):
            dst = os.path.join(dst, os.path.basename(src))
        elif not os.path.isdir(os.path.dirname(dst)):
            logger.error(
                "Parent directory for destination doesn't exist."
            )

        if os.path.exists(dst):
            if not overwrite:
                logger.error(
                    'Destination exists and overwrite is false.'
                )
            if not os.path.isfile(dst):
                logger.error(
                    'Destination exists but is not a file.'
                )
        else:
            dst_path = os.path.dirname(dst)
            if not os.path.isdir(dst_path):
                logger.error(
                    'No such file or directory \'{0}\''.format(dst_path)
                )
            # Seems to be duplicate of line 1794, produces /path/file_name/file_name
            #dst = os.path.join(dst, os.path.basename(src))

        with open(dst, 'wb') as df:
            df.write(container.files.get(src))

        if mode:
            os.chmod(dst, mode)
        if uid == '0' or uid == '0':
            uid = int(uid)
        else:
            uid = -1
        if gid == '0' or gid == '0':
            gid = int(gid)
        else:
            gid = -1
        if uid != -1 or gid != -1:
            os.chown(dst, uid, gid)
        return True

    
def pylxd_client_get(remote_addr=None, cert=None, key=None, verify_cert=False):
    try:
        if remote_addr is None or remote_addr == '/var/lib/lxd/unix.socket':
            logger.debug('Trying to connect to the local unix socket')
            client = pylxd.Client()
        else:
            if remote_addr.startswith('/'):
                client = pylxd.Client(remote_addr)
            else:
                if cert is None or key is None:
                    logger.error(
                        ('You have to give a Cert and '
                         'Key file for remote endpoints.')
                    )

                cert = os.path.expanduser(cert)
                key = os.path.expanduser(key)

                if not os.path.isfile(cert):
                    logger.error(
                        ('You have given an invalid cert path: "{0}", '
                         'the file does not exists or is not a file.').format(
                            cert
                        )
                    )

                if not os.path.isfile(key):
                    logger.error(
                        ('You have given an invalid key path: "{0}", '
                         'the file does not exists or is not a file.').format(
                            key
                        )
                    )

                logger.debug((
                    'Trying to connecto to "{0}" '
                    'with cert "{1}", key "{2}" and '
                    'verify_cert "{3!s}"'.format(
                        remote_addr, cert, key, verify_cert)
                ))
                client = pylxd.Client(
                    endpoint=remote_addr,
                    cert=(cert, key,),
                    verify=verify_cert
                )
    except pylxd.exceptions.ClientConnectionFailed:
        logger.error(
            "Failed to connect to '{0}'".format(remote_addr)
        )

    except TypeError as e:
        # Happens when the verification failed.
        logger.error(
            ('Failed to connect to "{0}",'
             ' looks like the SSL verification failed, error was: {1}'
             ).format(remote_addr, e)
        )

    return client

def normalize_input_values(config, devices):
    # It translates:
    #    [{key: key1, value: value1}, {key: key2, value: value2}]
    # to:
    #    {key1: value1, key2: value2}
    #
    if isinstance(config, list):
        if (config and
                'key' in config[0] and
                'value' in config[0]):
            config = {d['key']: d['value'] for d in config}
        else:
            config = {}

    if isinstance(config, six.string_types):
        logger.error(
            "config can't be a string, validate your YAML input."
        )

    if isinstance(devices, six.string_types):
        logger.error(
            "devices can't be a string, validate your YAML input."
        )

    # Golangs wants strings
    if config is not None:
        for k, v in six.iteritems(config):
            config[k] = six.text_type(v)
    if devices is not None:
        for dn in devices:
            for k, v in six.iteritems(devices[dn]):
                devices[dn][k] = v

    return (config, devices,)

def _pylxd_model_to_dict(obj):
    """Translates a plyxd model object to a dict"""
    marshalled = {}
    for key in obj.__attributes__.keys():
        if hasattr(obj, key):
            marshalled[key] = getattr(obj, key)
    return marshalled

def pylxd_save_object(obj):
    ''' Saves an object (profile/image/container) and
        translate its execpetion on failure
    obj :
        The object to save
    This is an internal method, no CLI Example.
    '''
    try:
        obj.save()
    except pylxd.exceptions.LXDAPIException as e:
        logger.error(six.text_type(e))

    return True

def _set_property_dict_item(obj, prop, key, value):
    ''' Sets the dict item key of the attr from obj.
        Basicaly it does getattr(obj, prop)[key] = value.
        For the disk device we added some checks to make
        device changes on the CLI saver.
    '''
    attr = getattr(obj, prop)
    if prop == 'devices':
        device_type = value['type']

        if device_type == 'disk':

            if 'path' not in value:
                logger.error(
                    "path must be given as parameter"
                )

            if value['path'] != '/' and 'source' not in value:
                logger.error(
                    "source must be given as parameter"
                )

        for k in value.keys():
            if k.startswith('__'):
                del value[k]

        attr[key] = value

    else:  # config
        attr[key] = six.text_type(value)
    pylxd_save_object(obj)
    return _pylxd_model_to_dict(obj)

def sync_config_devices(self, obj, newconfig, newdevices, test=False):
        ''' Syncs the given config and devices with the object
            (a profile or a container)
            returns a changes dict with all changes made.
            obj :
                The object to sync with / or just test with.
            newconfig:
                The new config to check with the obj.
            newdevices:
                The new devices to check with the obj.
            test:
                Wherever to not change anything and give "Would change" message.
        '''
        changes = {}

        #
        # config changes
        #
        if newconfig is None:
            newconfig = {}

        newconfig = dict(list(zip(
            map(six.text_type, newconfig.keys()),
            map(six.text_type, newconfig.values())
        )))
        cck = set(newconfig.keys())

        obj.config = dict(list(zip(
            map(six.text_type, obj.config.keys()),
            map(six.text_type, obj.config.values())
        )))
        ock = set(obj.config.keys())

        config_changes = {}
        # Removed keys
        for k in ock.difference(cck):
            # Ignore LXD internals.
            if k.startswith('volatile.') or k.startswith('image.'):
                continue

            if not test:
                config_changes[k] = (
                    'Removed config key "{0}", its value was "{1}"'
                ).format(k, obj.config[k])
                del obj.config[k]
            else:
                config_changes[k] = (
                    'Would remove config key "{0} with value "{1}"'
                ).format(k, obj.config[k])

        # same keys
        for k in cck.intersection(ock):
            # Ignore LXD internals.
            if k.startswith('volatile.') or k.startswith('image.'):
                continue

            if newconfig[k] != obj.config[k]:
                if not test:
                    config_changes[k] = (
                        'Changed config key "{0}" to "{1}", '
                        'its value was "{2}"'
                    ).format(k, newconfig[k], obj.config[k])
                    obj.config[k] = newconfig[k]
                else:
                    config_changes[k] = (
                        'Would change config key "{0}" to "{1}", '
                        'its current value is "{2}"'
                    ).format(k, newconfig[k], obj.config[k])

        # New keys
        for k in cck.difference(ock):
            # Ignore LXD internals.
            if k.startswith('volatile.') or k.startswith('image.'):
                continue

            if not test:
                config_changes[k] = (
                    'Added config key "{0}" = "{1}"'
                ).format(k, newconfig[k])
                obj.config[k] = newconfig[k]
            else:
                config_changes[k] = (
                    'Would add config key "{0}" = "{1}"'
                ).format(k, newconfig[k])

        if config_changes:
            changes['config'] = config_changes

        #
        # devices changes
        #
        if newdevices is None:
            newdevices = {}

        dk = set(obj.devices.keys())
        ndk = set(newdevices.keys())

        devices_changes = {}
        # Removed devices
        for k in dk.difference(ndk):
            # Ignore LXD internals.
            if k == u'root':
                continue

            if not test:
                devices_changes[k] = (
                    'Removed device "{0}"'
                ).format(k)
                del obj.devices[k]
            else:
                devices_changes[k] = (
                    'Would remove device "{0}"'
                ).format(k)

        # Changed devices
        for k, v in six.iteritems(obj.devices):
            # Ignore LXD internals also for new devices.
            if k == u'root':
                continue

            if k not in newdevices:
                # In test mode we don't delete devices above.
                continue

            if newdevices[k] != v:
                if not test:
                    devices_changes[k] = (
                        'Changed device "{0}"'
                    ).format(k)
                    obj.devices[k] = newdevices[k]
                else:
                    devices_changes[k] = (
                        'Would change device "{0}"'
                    ).format(k)

        # New devices
        for k in ndk.difference(dk):
            # Ignore LXD internals.
            if k == u'root':
                continue

            if not test:
                devices_changes[k] = (
                    'Added device "{0}"'
                ).format(k)
                obj.devices[k] = newdevices[k]
            else:
                devices_changes[k] = (
                    'Would add device "{0}"'
                ).format(k)

        if devices_changes:
            changes['devices'] = devices_changes

        return changes

def _get_property_dict_item(obj, prop, key):
    attr = getattr(obj, prop)
    if key not in attr:
        logger.error(
            "'{0}' doesn't exists".format(key)
        )

    return attr[key]

def _delete_property_dict_item(obj, prop, key):
    attr = getattr(obj, prop)
    if key not in attr:
        logger.error(
            "'{0}' doesn't exists".format(key)
        )

    del attr[key]
    pylxd_save_object(obj)

    return True