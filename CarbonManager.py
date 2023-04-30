from os import kill
from rpyc.utils.factory import ssh_connect
from Job import Job, Container
from apps import *
import rpyc
from RMAdapter import *
import sys
from threading import Timer, Thread
import time
import datetime
import socket
try:
    from CARBONFIRST_client import CarbonFirstClient
except Exception as e:
    # Error handling
    print("QueryAPI Client not found: {}".format(e))
    pass
import urllib3
import argparse
import csv

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

def signal_handler(sig, frame):
    print('Ctrl+C pressed! Exiting...')
    sys.exit(0)

class RepeatedTimer(object): # https://stackoverflow.com/a/38317060
    def __init__(self, obj, interval, *args, **kwargs):
        self._timer     = None
        self.obj = obj
        self.interval   = interval
        #self.function   = function
        self.args       = args
        self.kwargs     = kwargs
        self.is_running = False
        self.start()

    def _run(self):
        self.is_running = False
        self.start()
        #self.function(*self.args, **self.kwargs)
        self.obj.run(*self.args, **self.kwargs)

    def start(self):
        if not self.is_running:
            self._timer = Timer(self.interval, self._run)
            self._timer.start()
            self.is_running = True

    def stop(self):
        self._timer.cancel()
        self.is_running = False

def connect(host, port):
    conn = rpyc.connect(host, port, config={'allow_all_attrs':True,})
    conn._config["sync_request_timeout"] = None
    return conn.root

class RM(rpyc.Service):

    def __init__(self, rm_endpoint, cert, key, nodes, density, query_api_server, cores_per_node, power_per_core, idle_core_power, trace_file=None, trace_interval=30):
        logging.basicConfig(format="[%(levelname)s] %(asctime)s %(message)s")
        self.hostname = None
        self.ip = None

        self.density = density
        self.nodes = {} # Compute resources
        for node in nodes:
            self.nodes[node] = []
        self.cores_per_node = cores_per_node
        self.power_per_core = power_per_core # 
        self.idle_core_power = idle_core_power # (Obelix = 25, Cube = 1.5?)
        self.battery = None
        self.jobs = {} 
        self.generators = [] # Mostly composed of panels
        self.emissions = {}
        self.dc_total_emission = 0.0
        self.rm_endpoint = rm_endpoint
        self.cert = cert
        self.key = key
        self.resource_manager = RMAdapter(self.rm_endpoint, cert=self.cert, key=self.key)
        self.default_quota = str(1.0)
        self.connect_rm()

        self.query_api = None
        if query_api_server != None:
            self.query_api = self.connect_query_api(query_api_server)
        else:
            logger.warning("No QueryAPI endpoint provided.")
            logger.warning("Using a linear-function and CPU% to estimate power usage for instances")
        
        # Start main run() for account and notification
        self.time_interval = trace_interval
        self.inst_interval = 5
        self.iteration = 0
        self.co2_trace = self.trace_reader(trace_file)
        self.co2_trace_offset = 0 # 160 = Descending in a duck-trace (Carbon_trace.csv) / 250 = Ascending in a duck-trace
        self.co2_trace_speed = 5
        #self.manager_loop = RepeatedTimer(self, self.time_interval) # it auto-starts, no need of start()
        #self.run_thread = Thread(target=self.run, args=())
        #self.run_thread.start()
        self.house_keeping = Thread(target=self.house_keep, args=())
        self.house_keeping.start()
        self.accounting = {}
        logger.info("ecoVisor is instatiated. Ready to receive requests.")

    def trace_reader(self, trace_file):
        with open(trace_file) as csv_file:
            csv_reader = csv.reader(csv_file, delimiter=',')
            rows = list(csv_reader)
        return rows

    def connect_query_api(self, query_api_server):
        query_api = None
        try:
            query_api_url = query_api_server.split(':')
            query_api_host = query_api_url[0]
            if len(query_api_url) == 2:
                query_api_port = int(query_api_url[1])
            if len(query_api_url) == 1:
                query_api_port = 8000
            query_api = CarbonFirstClient(query_api_host, query_api_port)
            logger.info("Connected to the QueryAPI server at {}".format(query_api_server))
        except Exception as e:
            logger.error("Error while connecting to the QueryAPI server at {}: {}".format(query_api_server, e))
            logger.warning("Using a linear-function and CPU% to estimate power usage for instances")
        return query_api

    def add_query_app(self, app_name):
        if self.query_api != None:
            job = self.jobs[app_name]['job']
            containers = list(job.containers)
            logger.info("containerst_list = {}".format(containers))
            self.query_api.add_application(app_name, containers, job.creation_time, job.vsolar_panels, job.vbattery)
            logger.info("Registered with QueryAPI")

    def on_connect(self, conn):
        print("New connection: %s" % (conn))
        #conn.modules.sys.stdout.write("Hello, you are connected\n")
        #self.client = conn
        pass

    def on_disconnect(self, conn):
        #print "Disconnection: %s" % conn
        pass

    def exposed_get_power_per_core(self):
        return self.power_per_core

    def exposed_get_methods(self):
        service_index = dir(self)
        exposed_methods = [x.replace('exposed_', '') for x in service_index if x.startswith('exposed_')]
        return exposed_methods

    def connect_rm(self):
        try:
            self.resource_manager.connect(self.rm_endpoint)
            logger.info("Connected to orchestrator {} through endpoint {}".format(self.resource_manager.orchestrator, self.rm_endpoint))
        except Exception as e:
            logger.error("Error while connecting to orchestrator {} at endpoint {}.\nPlease check if the address is correct. It is not possible to continue.\nError: {}".format(self.resource_manager.orchestrator, self.rm_endpoint, e))
            exit -1

    def exposed_register(self, app_name, app_ip, port):
        #try:
            conn = connect(app_ip, port)
            self.jobs[app_name] = { 
                'name': app_name, 
                'ip': app_ip, 
                'port': port, 
                'connection': conn, 
                'job': None, 
                'instances': {},
                'inst_cmd_threads': {},
                'insts_rsrc_usage': {},
                'rsrc_allocation': [],
                'rsrc_usage': [],
                'power_alloc': {},
                'power_usage': [],
                'energy_usage': [],
                'co2_emission': [],
                'iteration': 0,
                'co2_samples': []
            }
            logger.info("Application {} registred with the CarbonManager at {}:{}".format(app_name, app_ip, port))
            return 1
        #except Exception as e:
        #    logger.error("Error while registring application {} at {}:{} - \n {}".format(app_name, app_ip, port, e))
        #    return e

    def update_hosts_file(self, job, instance_hosts, default_dir='/mnt/nfs/lxd/'):
        env_var = job.vars
        if isinstance(env_var, list):
            if (env_var and
                'key' in env_var[0] and
                'value' in env_var[0]):
                envvars = {d['key']: d['value'] for d in env_var}
        else:
            envvars = {}
        for k, v in six.iteritems(envvars):
            if v == "$LXD_HOSTS":
                v = instance_hosts
            # The following is mostly useful for cases when we cannot update specific env. vars. of processes
            if os.path.isfile(default_dir + k): # If a key in default_dir is a file... 
                env_file = open(default_dir + k, "r+",)
                env_cont = env_file.readlines()
                key = env_cont[0].strip().split("=")[0] # ... complete the file's 1st line...
                env_file.seek(0)
                env_file.write(key + "=" + v) # ... with v 
                env_file.truncate()
                logger.info("Hosts file ({}) for job {} updated!".format(default_dir+k, job.name))

    def hosts_list(self, job, update_hosts_file=False, use_ip=True):
        instance_hosts = None
        for instance in job.containers.keys():
            if use_ip == True:
                # We will need to implement this in the future
                host_name = self.resource_manager.get_instance_ip(instance, wait=True)
                #pass
            else:
                host_name = instance + '.lxd'
            if instance_hosts == None:
                instance_hosts = host_name
            else:
                instance_hosts = instance_hosts + ',' + host_name
        if update_hosts_file == True:
            self.update_hosts_file(job, instance_hosts)
        return instance_hosts

    def house_keep(self, sleep=None):
        if sleep == None:
            sleep = self.time_interval

        for app_name in self.jobs:
            try:
                threads = list(self.jobs[app_name]['inst_cmd_threads'])
                if len(threads) > 0:
                    #logger.info("Threads for {}: {}".format(app_name, threads))
                    pthread = self.jobs[app_name]['inst_cmd_threads'][threads[0]]
                    if not pthread.is_alive():
                        logger.info("Main command for job {} has ended {}. Stopping all instances...".format(app_name, pthread))
                        for t in threads:
                            self.jobs[app_name]['inst_cmd_threads'][t].join()
                            self.jobs[app_name]['inst_cmd_threads'].pop(t, None)
                        self.jobs[app_name]['job'].status = 'FINISHED'
                        self.exposed_stop_job(app_name)
                    else:
                        if self.jobs[app_name]['job'].status == 'RUNNING':
                            self.exposed_get_job_cpu_util(app_name)
                            self.run()
            except Exception as e:
                continue
        time.sleep(sleep) # This is to not overload LXD with requests. It might be useful to use the Metrics Server system to implement this.
        self.house_keep()

    def push_util_data(self, app_name, container_list):
        if self.query_api != None:
            logger.info("Pushing CPU util for {} into DB".format(app_name))
            self.query_api.add_cpu_utilization(app_name, container_list, datetime.datetime.now())

    def create_instance(self, app_name, instance):
        job = self.jobs[app_name]['job']
        #instance_hosts = self.hosts_list(job)
        instance_hosts = None
        inst = job.containers[instance]
        env_vars = job.vars
        inst_obj = self.resource_manager.create_new_instance(instance, inst.cpu_quota, inst.container_image, inst.pull_server, inst.protocol, instance_hosts, inst.devices, env_vars)
        self.jobs[app_name]['instances'][instance] = inst_obj
        if self.resource_manager.is_part_of_cluster():
            self.nodes[inst_obj.location].append(inst_obj)
        logger.info("Container instance {} created for job {}.".format(instance, job.name))
        return inst_obj

    def exposed_submit_job(self, app_name, job_json):
        if app_name in self.jobs.keys():
            job = self.create_application(app_name, job_json)
            self.jobs[app_name]['job'] = job
            num_inst = 0
            for instance in job.containers:
                logger.info("Creating container instance {} for job {}".format(instance, self.jobs[app_name]['job'].name))
                try:
                    self.exposed_add_instance(app_name, instance)
                    num_inst = num_inst + 1
                except Exception as e:
                    logger.error("Error creating instance {} for job {}: {}".format(instance, self.jobs[app_name]['job'].name, e))
            return num_inst
        else:
            logger.error("Job {} has already been submitted.".format(self.jobs[app_name]['job'].name))
            return None

    def exposed_start_job(self, app_name, use_ip=True):
        job = self.jobs[app_name]['job']
        if app_name in self.jobs.keys() and job:
            logger.info("Starting job {} execution.".format(app_name))
            num_instances = 0

            for instance in job.containers:
                try:
                    inst = self.jobs[app_name]['instances'][instance]
                    logger.info("Starting instance {}...".format(instance))
                    self.jobs[app_name]['instances'][instance] = self.resource_manager.start_instance(instance)
                    num_instances = num_instances + 1
                except Exception as e:
                    logger.info("Error while starting instance {}: {}".format(instance, e))
                    self.jobs[app_name]['instances'][instance] = None
                    continue
            if self.query_api != None:
                logger.info("Registering {} with QueryAPI".format(app_name))
                self.add_query_app(app_name)
            #time.sleep(self.inst_interval) # This is to allow instances to bootup. Check about how to detect it automatically later!
            time.sleep(2*self.inst_interval) # This problem is worse on Jetsons
            if use_ip == True:
                host_ips = self.hosts_list(job, use_ip=True)
                for instance in job.containers:
                    logger.info("Getting instance {} IP.".format(instance, self.jobs[app_name]['job'].name))
                    try:
                        logger.info("Appling host IP {} list into instance {}".format(host_ips, instance))
                    except Exception as e:
                        logger.error("Error creating instance {} for job {}: {}".format(instance, self.jobs[app_name]['job'].name, e))
            for instance in job.containers:
                cmd_ret = self.exposed_instance_cmd(app_name, instance)
                time.sleep(self.inst_interval)

            job.status = 'RUNNING'
            logger.info("{} instances started for job {}".format(num_instances, app_name))
            return num_instances
        else:
            logger.error("Job {} not found in the registry. Submit it before starting execution.".format(app_name))

    def exposed_instance_cmd(self, app_name, instance, cmd=None, notification=False):
        try:
            job = self.jobs[app_name]
            if cmd == None:
                cmd = job['job'].containers[instance].start_cmd
            cmd = cmd.split()
            logger.info("Executing command {} on instance {}".format(cmd, instance))
            t = Thread(target=self.resource_manager.container_execute, args=(instance, cmd,))
            if not notification: # Not clever way to not disturb house_keep(). Fix it in the future!
                job['inst_cmd_threads'][instance] = t
                job['inst_cmd_threads'][instance].start()
            else:
                t.start()
            return 1
        except Exception as e:
            logger.info("Error executing command on instance {}: {}".format(instance, e))
            job['inst_cmd_threads'][instance] = None
            return 0

    def exposed_stop_instance(self, app_name, instance=None):
        try:
            job = self.jobs[app_name]
            if instance == None:
                instance = self.get_non_primary_container(app_name)
            job['instances'][instance] = self.resource_manager.stop_instance(instance, update=True)
            logger.info("Container instance {} stopped for job {}.".format(instance, app_name))
            return instance
        except Exception as e:
            logger.info("Error while stopping instance {}: {}".format(instance, e))
            job['instances'][instance] = None
            return None

    def save_job_results(self, app_name, csv_file=None):
        if csv_file == None:
            import time
            timestr = time.strftime("%Y%m%d-%H%M%S")
            csv_file = app_name + '_results_' + timestr + '.csv'

        job = self.jobs[app_name]
        with open(csv_file, mode='w') as results_file:
            results_writer = csv.writer(results_file, delimiter=',')
            results_writer.writerow(['Iteration', 'CO2_Intensity', 'CO2_Target', 'CO2_Footprint', 'Pwr_Use', 'Energy_Use', 'Resource_Alloc', 'Resource_Util',])

            for iteration in range(job['iteration']):
                csv_row = []
                csv_row.append(iteration)
                csv_row.append(job['co2_samples'][iteration])
                csv_row.append(job['co2_emission'][1])
                csv_row.append(job['co2_emission'][iteration])
                csv_row.append(job['power_usage'][iteration])
                csv_row.append(job['energy_usage'][iteration])
                csv_row.append(job['rsrc_allocation'][iteration])
                csv_row.append(job['rsrc_usage'][iteration])
                #print(csv_row)
                results_writer.writerow(csv_row)

    def exposed_stop_job(self, app_name, dt=None):
        if app_name in self.jobs.keys() and self.jobs[app_name]['job']:
            logger.info("Stopping job {} execution.".format(app_name))
            job = self.jobs[app_name]['job']
            if dt == None:
                dt = datetime.datetime.now()
            job.stop_time = dt
            num_instances = 0
            for instance in job.containers:
                try:
                    inst = self.exposed_stop_instance(app_name, instance)
                    num_instances = num_instances + 1
                except:
                    continue

            total_time = job.stop_time - job.creation_time
            co2_emissions = 0.0
            for co2_sample in self.jobs[app_name]['co2_emission']:
                co2_emissions = co2_emissions + co2_sample
            co2_emissions = co2_emissions # in gCO2eq
            logger.info("\n{} instances stopped for job {}.\nDuration: {}\nTotal CO2 Emissions (gCO2eq): {}".format(num_instances, app_name, total_time, co2_emissions))
            self.save_job_results(app_name)
            logger.info("Results saved for job {}".format(app_name))
            try:
                terminate = self.jobs[app_name]['connection'].job_stopped(str("MAIN_CMD_FINISHED"), str(total_time), str(co2_emissions))
            except Exception as e:
                logger.error("Error while informing JOB: {}".format(e))
            return num_instances
        else:
            logger.error("Job {} not found in the registry. Submit it before stopping execution.".format(app_name))            

    def exposed_get_inst_cpu_util(self, app_name, instance, relative=False):
        job = self.jobs[app_name]['job']
        resource_usage = self.resource_manager.get_cpu_util(instance, relative=relative)
        return resource_usage

    def exposed_get_job_cpu_util(self, app_name, relative=False):
        num_instances = 0
        sum_cpu_util = 0.0
        logger.info("Getting CPU% for job {}".format(app_name))
        job = self.jobs[app_name]['job']
        container_dict = {}
        for instance in job.containers:
            num_instances = num_instances + 1
            rm_instance_util = self.resource_manager.get_cpu_util(instance, relative=relative)
            curr_instance_util = min(float(job.containers[instance].cpu_quota), rm_instance_util) # Check why we need to use min() here
            container_dict[instance] = curr_instance_util
            self.jobs[app_name]['insts_rsrc_usage'][instance] = curr_instance_util
            sum_cpu_util = sum_cpu_util + curr_instance_util
            logger.info("CPU% for {}: {}".format(instance, curr_instance_util))
        container_list = [x for x in container_dict.items()]
        self.push_util_data(app_name, container_list)
        avg_cpu_util = sum_cpu_util
        self.jobs[app_name]['rsrc_usage'].append(avg_cpu_util)
        #pw_usage = self.exposed_get_job_estimate_power_usage(app_name)
        logger.info("Rscr% for {} is {}".format(app_name, avg_cpu_util))
        return avg_cpu_util

    def exposed_get_job_resource_usage_time_series(self, app_name):
        return self.jobs[app_name]['rsrc_usage']

    def exposed_get_inst_resource_allocation(self, app_name, instance=None):
        inst = self.get_instance(app_name, instance)
        return float(inst.cpu_quota)

    def exposed_get_job_resource_allocation(self, app_name):
        try:
            job = self.jobs[app_name]['job']
            total_resources = 0.0
            for instance in job.containers:
                total_resources = total_resources + self.exposed_get_inst_resource_allocation(app_name, instance)
            return total_resources
        except Exception as e:
            logger.error("Error retrieving {} resource allocation information: {}".format(app_name, e))
            return 0.0

    def exposed_get_job_estimate_power_usage(self, app_name):
        try:
            job = self.jobs[app_name]['job']
            total_power_usage = 0.0
            for instance in job.containers:
                try:
                    inst_pwr_use = self.exposed_get_instance_estimate_power_usage(app_name, instance)
                except Exception as e:
                    logger.error("Error retrieving {}'s information, using 0.0W: {}".format(instance, e))
                    inst_pwr_use = 0.0
                total_power_usage = total_power_usage + inst_pwr_use
            return total_power_usage
        except Exception as e:
            logger.error("Error calculating {}'s power usage estimate, returning 0.0W: {}".format(app_name, e))
            return 0.0

    def exposed_get_instance_estimate_power_usage(self, app_name, instance=None):
        job = self.jobs[app_name]
        power_consumption = 0.0
        if instance == None:
            instance = self.get_non_primary_container(app_name, lowest_util=False, return_primary=True)
        try:
            inst = job['job'].containers[instance]
            resource_usage = float(job['insts_rsrc_usage'][instance])
            # PWD_CONSUMPTION = POWER_PER_CORE + ((IDLE_SERVER_POWER / NUM_CORES) * INSTANCE_RSRC_ALLOCATION))
            power_consumption = self.power_per_core * resource_usage + ((self.idle_core_power / self.cores_per_node) * float(inst.cpu_quota))
        except Exception as e:
            logger.warning("Error getting {} power reading".format(instance))
        return power_consumption

    def get_insts_per_node(self, instance):
        ''' Given a running instance (string), it returns the total number of instances running in the same node'''
        target_node, _ = self.get_inst_node(instance)
        if target_node != None:
            return len(self.nodes[target_node]) # We should handle this differently
        return 1

    def get_inst_node(self, instance):
        for node in self.nodes:
            ind = 0
            for inst in self.nodes[node]:
                if inst.name == instance:
                    return (node, ind)
                ind = ind + 1
        logger.warning("Instance {}'s node not found!".format(instance))
        return (None, None) # Not found

    def create_application(self, app_name, app_json):
        try:
            if app_name in self.jobs.keys():
                app_template = app_json
                job_name = app_name
                job = Job(job_name)
                job.template = app_template
                job.regex = app_template['regex']
                job.container_image = app_template['image']
                job.pull_server = app_template['server']
                job.protocol = app_template['protocol']
                job.update_cmd = app_template['update_cmd']
                job.vbattery = float(app_template['vbattery_cap'])
                job.vsolar_panels = float(app_template['vsolar_panels'])
                job.total_co2_quota = float(app_template['total_co2_quota'])
                job.total_grid_quota = float(app_template['total_grid_quota'])
                job.creation_time = datetime.datetime.now()

                for v in app_template['vars']:
                    job.vars.append(v)
                logger.info("Job vars: {}".format(job.vars))

                for job_container in app_template['instances']:
                    num_containers = job_container['replicas']
                    for replica in range(int(num_containers)):
                        cont = self.create_containerObj(job, job_container)
                        if len(job.containers) == 0:
                            job.primary_instance = cont.name
                        job.containers[cont.name] = cont
                logger.info("Job template {} with {} containers created for application {}".format(job_name, job.num_containers, job_name))
            return job
        except Exception as e:
            logger.error("Error loading application's json template: {}".format(e))
            return None
 
    def get_non_primary_container(self, app_name, lowest_util=True, return_primary=False):
        '''Returns the first non-primary container it can find, else None. Set return_primary=True if it should also be considered.'''
        job_obj = self.jobs[app_name]['job']
        container_name = None
        if lowest_util == True:
            import copy
            instances = copy.deepcopy(self.jobs[app_name]['insts_rsrc_usage'])
            instances.pop(self.get_name_primary_instance(app_name))
            try:
                container_name = min(instances, key=instances.get)
            except Exception as e:
                logger.warning("Error while finding instance with lowest resource utilization: {}".format(e))
        else:
            for inst in job_obj.containers:
                if self.get_name_primary_instance(app_name) != inst:
                    container_name = inst
                    #break
        if container_name == None:
            if return_primary == True:
                container_name = self.get_name_primary_instance(app_name)
                logger.warning("return_primary=True. Returning primary instance {}".format(container_name))
            else:
                logger.warning("No non-primary containers found, None being returned. Set return_primary if the primary-container should be returned!")
        return container_name

    def create_containerObj(self, job_obj, container=None, size=None):
        if container == None:
            replica_inst_name = job_obj.template['replica_instance']
            for inst in job_obj.template['instances']:
                if replica_inst_name == inst['container_name']:
                    container = inst
                    if size == None:
                        size = self.default_quota
                    container['cpu_quota'] = size
                    break
            if container == None:
                container['container_name'] = replica_inst_name
                if size == None:
                    size = self.default_quota
                container['cpu_quota'] = size
                # container['pid_file'] = "pid_file"
                container['start_cmd'] = None

        num_cont = len(job_obj.containers)
        if num_cont != 0:
            num_cont = 1
        container_name = self.get_inst_name(job_obj, container['container_name'], num_cont)
        while container_name in job_obj.containers.keys():
            num_cont = num_cont + 1
            container_name = self.get_inst_name(job_obj, container['container_name'], num_cont)
        container_obj = Container(container_name, job_obj)
        container_obj.container_image = job_obj.container_image
        container_obj.pull_server = job_obj.pull_server
        container_obj.protocol = job_obj.protocol
        container_obj.cpu_quota = container['cpu_quota']
        # container_obj.pid_file = container['pid_file']

        if container['start_cmd']:
            container_obj.start_cmd = container['start_cmd']
        else:
            container_obj.start_cmd = 'echo You container is running'
        if 'devices' in container:
            container_obj.devices = container['devices']
        else:
            container_obj.devices = None

        return container_obj

    def exposed_enlarge_instance_rsrc(self, app_name, container=None, size=0.0):
        container = self.get_instance(app_name, container)
        current_allocation = self.exposed_get_inst_resource_allocation(app_name, container.name)
        if current_allocation == None:
            current_allocation = 0.0
        final_allocation = float(current_allocation) + float(size)
        container.cpu_quota = final_allocation
        return self.resource_manager.update_instance_allocation(container.name, final_allocation)

    def exposed_reduce_instance_rsrc(self, app_name, container=None, size=0.0):
        if container == None:
            container = self.get_instance(app_name, container)
        current_allocation = self.exposed_get_inst_resource_allocation(app_name, container)
        if current_allocation == None:
            current_allocation = 0.0
        final_allocation = max(0.0, float(current_allocation) - float(size))
        return self.resource_manager.update_instance_allocation(container, final_allocation)

    def exposed_add_instance(self, app_name, container=None, container_size=None, start=False, notify_primary=False):
        job_obj = self.jobs[app_name]['job']
        if container == None:
            container = self.create_containerObj(job_obj, size=container_size)
            job_obj.containers[container.name] = container
            inst_obj = self.create_instance(app_name, container.name)
        else:
            inst_obj = self.create_instance(app_name, container)

        job_obj.num_containers = job_obj.num_containers + 1

        if start == True:
            logger.info("Starting job {}'s instance {}...".format(app_name, inst_obj.name))
            self.jobs[app_name]['instances'][inst_obj.name] = self.resource_manager.start_instance(inst_obj.name, update=True)
            time.sleep(self.inst_interval) # We need to fix this in future
            cmd_ret = self.exposed_instance_cmd(app_name, inst_obj.name)
            logger.info("Instance {} started".format(inst_obj.name))


        if notify_primary == True:
            for instance in job_obj.containers:
                cmd_ret = self.notify_container(app_name, instance)
                if cmd_ret == 1 or cmd_ret == -1:
                    logger.info("Notification sent to {}".format(instance))
                    break

        return inst_obj

    def get_primary_instance(self, app_name):
        try:
            job = self.jobs[app_name]['job']
            inst = job.containers[job.primary_instance]
            return inst
        except Exception as e:
            logger.error("Error while fetching primary instance! Returning None: {}".format(e))
            return None

    def get_name_primary_instance(self, app_name):
        job = self.jobs[app_name]['job']
        name = self.get_primary_instance(app_name).name
        return name

    def get_instance(self, app_name, instance=None):
        '''Returns an instance object. If instance is None, it returns the primary container.'''
        inst = None
        if instance == None:
            inst = self.get_primary_instance(app_name)
            if inst == None:
                logger.error("There are no instances running for job {}".format(app_name))
        else:
            try:
                inst = self.jobs[app_name]['job'].containers[instance]
            except Exception as e:
                logger.error("Error getting instance: {}".format(e))
        return inst

    def get_inst_name(self, job, instance_name, post=0, separator='-'):
        name = job.regex + job.name + separator + instance_name + str(post)
        return name

    def notify_container(self, app_name, instance_name=None, terminate=False):
        job = self.jobs[app_name]['job']
        if instance_name == None:
            instance_name = self.get_name_primary_instance(app_name)
        instance = job.containers[instance_name]
        # pid_file = instance.pid_file
        # if terminate == True:
        #     pid_file = pid_file + "." + instance_name
        # if os.path.isfile(pid_file):
        #     pid_fd = open(pid_file, "r",)
        #     pid_out = pid_fd.readlines()
        #     pid = pid_out[0].strip()
        # else:
        #     logger.warning("PID file for {} not found, no containers will be notified! Make sure to output the PID of your main process to file {}!".format(instance_name, instance.pid_file))
        #     return -1
        # update_cmd = job.update_cmd + ' ' + pid
        # if terminate == True:
        #     update_cmd = 'kill -9 ' + pid
        # logger.info("Notifying container {}".format(instance_name))
        # #logger.info("Notifying primary container {}".format(instance_name))
        # try:
        #     cmd_ret = self.exposed_instance_cmd(app_name, instance_name, update_cmd, notification=True)
        #     logger.info("Notification sent!")
        #     time.sleep(self.inst_interval)
        #     return cmd_ret
        # except Exception as e:
        #     logger.error("Error when notifying {}: {}".format(instance_name, e))
        #     return 0
        logger.warning("PID file for {} not found, no containers will be notified! Make sure to output the PID of your main process to file!".format(instance_name))
        return -1


    def exposed_del_instance(self, app_name, instance=None, terminate=True, notify_primary=True, terminate_primary=False):
        job = self.jobs[app_name]['job']
        if instance == None:
            instance = self.get_non_primary_container(app_name, return_primary=terminate_primary)
        try:
            # Notify
            if terminate == True:
                terminate_inst = self.notify_container(app_name, instance, terminate=True)
                logger.info("Main {}'s parent process terminated".format(instance))
            job.containers.pop(instance)
            self.jobs[app_name]['insts_rsrc_usage'].pop(instance) # Check if there are better
            self.jobs[app_name]['instances'].pop(instance)        # places to put these
            if notify_primary == True:
                #for instance in job.containers.keys():
                logger.info("Notification for deletion")
                self.hosts_list(job, update_hosts_file=True)
                cmd_ret = self.notify_container(app_name)
                if cmd_ret == 1 or cmd_ret == -1:
                    logger.info("Notification sent to primary to tell about {}".format(instance))
                    time.sleep(self.inst_interval)
                    #break
            inst = self.exposed_stop_instance(app_name, instance)
            job.num_containers = job.num_containers - 1
            inst_node, inst_ind = self.get_inst_node(instance)
            self.nodes[inst_node].pop(inst_ind)
            return job.num_containers
        except Exception as e:
            logger.error("Error while deleting job {}'s instance {}: {}".format(app_name, instance, e))
            return job.num_containers

    def exposed_del_application(self, app):
        try:
            index = self.jobs.index(app)
            self.jobs.pop(index)
        except ValueError:
            print("Application {} does not exist".format(app))

    def add_generator(self, gen):
        self.generators.append(gen)

    def del_generator(self, gen):
        try:
            index = self.generators.index(gen)
            self.generators.pop(index)
        except ValueError:
            print("Generator does not exist")

    def add_resource(self, rscr):
        try:
            index = self.resources.index(rscr)
            print("Resource %s has already been added!" % (self.resources[index].name))
        except ValueError:
            self.resources.append(rscr)

    def del_resource(self, rscr):
        try:
            index = self.resources.index(rscr)
            self.resources.pop(index)
        except ValueError:
            print("Resource does not exist")

    def update_resource(self, curr_rscr, new_rscr):
        try:
            curr_index = self.resources.index(curr_rscr)
            self.resources[curr_index] = new_rscr
        except ValueError:
            print("Current resource is not present. Add it as a resource before updating it.")

    def update_resource_parameter(self, rscr, parameter, para_value=None, model_method=None, positional_arguments=None, keyword_arguments=None):
        try:
            rscr_ind = self.resources.index(rscr)
            self.resources[rscr_ind].update_parameter(parameter, para_value, model_method, *positional_arguments, **keyword_arguments)
        except ValueError:
            print("Error while updating resource parameter")

    def gen_emission(self):
        total_emission = 0.0
        for gen in self.generators:
            #print(next(gen.model)[1])
            total_emission = total_emission + next(gen.model)[1]

        self.emissions.append(total_emission)
        return total_emission

    def sample(self, iteration):
        index = self.co2_trace_offset + int(iteration) * self.co2_trace_speed
        s = self.co2_trace[index][1]
        return float(s)

    def run(self):
        if len(self.jobs) > 0:
            iteration = self.iteration
            # Get CO2 intensity
            co2_intensity_kWh = self.sample(iteration) # gCO2eq/kWh
            #co2_intensity = (co2_intensity_kWh * (0.000000277777778)) # gCO2eq/Ws
            co2_intensity = (co2_intensity_kWh * (1.0/(1000.0*3600.0))) # in gCO2eq/Ws
            co2_int_next = (self.sample(iteration+1) * (1.0/(1000.0*3600.0)))
            # Receive generator emissions
            #total_emission = self.gen_emission()

            # Receive total solar power generated
            total_solar = 1.0
            # Calculate physical battery levels
            total_battery = 0.0001

            apps_new_reqs = {}
            # Reallocate apps resources according to apps constraints and supply
            density = 0.0
            #logger.info(self.jobs.items())
            for app_name in self.jobs: # Update Accounting for app
                job = self.jobs[app_name]['job']
                cpu_util_samples = len(self.jobs[app_name]['rsrc_usage']) - 1
                if job.status == 'RUNNING' and cpu_util_samples >= 0:
                    try:
                        logger.info("Energy Iteration for job {}".format(app_name))
                        self.jobs[app_name]['rsrc_allocation'].append(self.exposed_get_job_resource_allocation(app_name))
                        #now = datetime.datetime.now()
                        #query_app = carbonfirst_client.Application(app, self.query_api)
                        #query_app = self.query_api.get_application(app)
                        job_iteration = self.jobs[app_name]['iteration']
                        self.jobs[app_name]['co2_samples'].append(co2_intensity)
                        # Update app's virtual battery-levels
                        # Update app's co2_quotas
                        # Update app's power_caps
                        # Get app's Energy usage for last time-interval
                        #app_energy_use = query_app.get('ENERGY', start=(now - datetime.timedelta(seconds=iter_duration)), stop=now)
                        app_pwr_use = self.exposed_get_job_estimate_power_usage(app_name) # in W
                        self.jobs[app_name]['power_usage'].append(app_pwr_use) 
                        app_energy_use = app_pwr_use * self.time_interval # in J (Ws)
                        self.jobs[app_name]['energy_usage'].append(app_energy_use)
                        # Get CO2 intensity and calculate app's Carbon Emission
                        #app_co2_emission = query_app.get('CARBON_INTENSITY', start=(now - iter_duration), stop=now, agg='mean')
                        app_co2_emission = app_energy_use * co2_intensity # in gCO2eq
                        self.jobs[app_name]['co2_emission'].append(app_co2_emission)
                        # Get app's solar allocation
                        #app_solar = float(self.jobs[app]['job']['vsolar_panels']) / total_solar
                        app_solar = 0.0
                        # Calculate apps demand & Update resources according to demand
                        job_util = self.jobs[app_name]['rsrc_usage'][cpu_util_samples]

                        #for util_sample in self.jobs[app_name]['rsrc_usage']:
                        #        avg_job_util = avg_job_util + util_sample

                        #avg_job_util = avg_job_util / len(job.containers)
                        #logger.info("AVG JOB UTIL: {}".format(avg_job_util))
                        app_new_rscrs, app_new_battery_alloc, app_new_solar_alloc, app_new_grid_alloc = self.jobs[app_name]['connection'].co2_scaler(app_energy_use, co2_intensity, app_solar, job_iteration, job_util, co2_int_next)
                        apps_new_reqs[app_name] = (app_new_rscrs, app_new_battery_alloc, app_new_solar_alloc, app_new_grid_alloc)
                    except Exception as e:
                        logger.warning("Error notifying {} (run()): {}".format(app_name, e))
                        continue
                    self.jobs[app_name]['iteration'] = job_iteration + 1
                    # Allocate app_new_rscrs
                    #self.horizontal_scaler(app_name, app_new_rscrs)
                    #density = density + app_new_rscrs

            # Sum all app grids, and check 
            self.iteration = iteration + 1
        #time.sleep(self.time_interval)
        #self.run()

    def horizontal_scaler(self, app, app_new_rscrs):
        return None

def argument_parser():
    parser = argparse.ArgumentParser(__name__)
    parser.add_argument("--rm-endpoint", dest="rm_endpoint", type=str, default="https://127.0.0.1:8443", help="Resource Manager API endpoint to connect to") # Jetson
    parser.add_argument("--lxd-cert", dest="cert", type=str, default=None,
                        help="Location of LXD certificate if the containers are going to be created locally")
    parser.add_argument("--lxd-key", dest="key", type=str, default=None,
                        help="Location of LXD key if the containers are going to be created locally")
    parser.add_argument("--nodes-list", dest="nodes", nargs='+', default=[socket.getfqdn()],
                        help="Nodes list composing the cluster")
    parser.add_argument("--queryapi-endpoint", dest="query_endpoint", type=str, default='obelix11:8000', help="QueryAPI endpoint to account for, and collect energy metrics from")
    parser.add_argument("--cores-per-node", dest="cores_node", type=float, default="8.0", # Jetson
                        help="Cores in each cluster node (This will automatic in the future)")
    parser.add_argument("--power-per-core", dest="power_core", type=float, default="1.125", # Jetson profile
                        help="Power usage per CPU-core (in W) when CPU%% is 100%%. Together with idle-power, this var. will be used to calculate amount of resources quota to give an application based on its power requirements. Jobs (multiple containers) power usage is calculated through CPU%%.")
    parser.add_argument("--idle-power", dest="idle_power", type=float, default="9.0", # Jetson specs
                        help="Idle power in W. Together w/ power-per-core, this var. will be used to calculate amount of resources to give an application based on its power requirements")
    parser.add_argument("--density", dest="density", type=float, default="10000.0",
                        help="Max amount of power in W that can flow in the entire cluster (we know standard is kW, but for now we normalize with apps)")
    parser.add_argument("--trace", dest="trace", type=str, default="traces/carbon_trace.csv",
                        help="Carbon Intensitiy trace file")
    parser.add_argument("--trace-interval", dest="trace_interval", type=float, default="10",
                        help="Carbon Intensitiy time interval. This defines the ecoVisor's main loop interval (when it notifies all applications about changes in the energy system)")

    args = parser.parse_args()
    return args

if __name__ == "__main__":
    print('ecoVisor (v0.01)')
    args = argument_parser()
    rm = RM(args.rm_endpoint, args.cert, args.key, args.nodes, args.density, args.query_endpoint, args.cores_node, args.power_core, args.idle_power, args.trace, args.trace_interval)
    from rpyc.utils.server import ThreadedServer
    t = ThreadedServer(rm, port=18861, protocol_config={'allow_all_attrs':True,})
    t.start()
