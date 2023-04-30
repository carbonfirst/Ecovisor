from Job import *
import rpyc
import logging
import json
import socket
import asyncio
from rpyc.utils.server import ThreadPoolServer, ForkingServer
from threading import Thread
import datetime
import time

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

def get_open_port():
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s.bind(('', 0))
    return s.getsockname()

def connect(host, port, service=''):
    try:
        conn = rpyc.connect(host, port, config={'allow_all_attrs':True,}, service=service)
        conn._config["sync_request_timeout"] = None
        return conn.root
    except Exception as e:
        logger.error("Error connecting to {}:{}. Error: {}".format(host, port, e))
        return None

class ApplicationController(rpyc.Service):
    def __init__(self, app_name="blast", app_template="application.json", cm_host="127.0.0.1", cm_port=18861):
        self.hostname = socket.getfqdn()
        self.ip = None
        self.port = None
        self.conn = None
        self.reg = None
        self.t = None

        self.app_json = app_template
        self.app_name = app_name
        self.job = None
        self.status = None
        self.app_template = app_template
        self.notifications = {}
        self.reg = None
        self.energy = {}
        self.co2 = {}
        self.resources = {}
        self.primary_cpu_util = {}
        self.click = False
        self.energy_baseline = None
        self.co2_target_rate = None

        # Connect and register to RM Server
        self.cm_host = cm_host
        self.cm_port = cm_port
        try:
            # Get a free socket port
            s = get_open_port()
            self.port = s[1]

            #Start AppControllerThreadServer
            self.t = Thread(target=start_server_thread, args=(self,))
            self.t.start()
            # Connect to CM
            self.carbon_manager = connect(self.cm_host, self.cm_port, service=self)
            logger.info("Connected to CarbonManager at {}:{}".format(self.cm_host, self.cm_port))
            # Register with CM
            self.reg = self.register(self.app_name, self.hostname, self.port)
            logger.info("Registered with CarbonManager!")
        except Exception as e:
            logger.error("Error while connecting and registring with the CarbonManager: {}", e)

    def on_connect(self, conn):
        print("New connection: %s" % (conn))
        #conn.modules.sys.stdout.write("Hello, you are connected\n")
        #self.client = conn
        pass

    def on_disconnect(self, conn):
        #print "Disconnection: %s" % conn
        pass

    def register(self, app_name, ip, port):
        return self.carbon_manager.register(app_name, ip, port)

    def create_application(self, app_json_file):
        json_file = open(app_json_file,)
        self.app_template = json.load(json_file)
        self.exposed_update_status("LOADED")

    def submit_job(self):
        try:
            submission = self.carbon_manager.submit_job(self.app_name, self.app_template)
            if submission >= 1 or submission == None:
                self.exposed_update_status("SUBMITTED")
            else:
                self.exposed_update_status("SUBMIT_FAILED")
                logger.error("Error submitting application's job: {}".format(submission))
        except Exception as e:
            self.exposed_update_status("SUBMIT_FAILED")
            logger.error("Error submitting application's job: {}".format(e))

    def start_job(self):
        start = self.carbon_manager.start_job(self.app_name)
        if start > 0:
            self.exposed_update_status("STARTED")
        return start

    def exposed_job_stopped(self, reason, total_time, total_co2):
        r = str(reason)
        tt = str(total_time)
        co2 = str(total_co2)
        logger.info("Job {} was stopped by the resource manager.\nReason: {}\nTotal time: {}\nTotal CO2 Emissions: {}".format(self.app_name, r, tt, co2))
        return 1

    def get_query_server(self, query_url):
        self.query_server_url = query_url

        #establish connection

        #set it
        self.query_server_obj = None
        return self.query_server_obj

    def create_notification(self, notification, callback=None):
        if self.query_server_obj == None:
            logger.error("No query server connection established.")
            return -1
        else:
            try:
                notifier_id = hash(notification)
                notifier_obj = self.query_server_obj.notify("5", notifier_id)
                if callback == None:
                    callback = self.notifier_callback
                self.notifications[notifier_id] = callback
            except Exception as e:
                logger.error("Error creating notification")

    def exposed_get_notification(self, notification, notifier_id=None):
        return None

    def horiz_scaler(self):
        return None

    def exposed_co2_scaler(self, app_energy_use, co2_intensity, app_solar, iteration, cpu_util, co2_int_next, vert=True):
        self.energy[iteration] = app_energy_use
        self.co2[iteration] = app_energy_use * co2_intensity # in gCO2eq
        curr_rsrc_alloc = self.carbon_manager.get_job_resource_allocation(self.app_name)
        logger.info("co2_scaler:\n app_energy_use: {}\n co2_intensity: {}\n co2_emission: {}g\n app_solar: {}\n curr_allocation: {}\n cpu_util: {}\n iteration: {}".format(app_energy_use, co2_intensity, self.co2[iteration], app_solar, curr_rsrc_alloc, cpu_util, iteration))
        inst_pwd = 0.0
        if iteration == 1:
            self.energy_baseline = app_energy_use
            self.rsrc_baseline = 16.0
            self.co2_target_rate = (self.energy_baseline * co2_intensity)

        if iteration > 1:
            debit = 0
            rsrc_util = round(cpu_util - curr_rsrc_alloc)
            if rsrc_util < 0.0:
            #    debit = abs(rsrc_util)
                logger.info("Resource Util. ({}) lower than allocation ({}). Debit of {} resources.".format(cpu_util, curr_rsrc_alloc, debit))
            #self.carbon_manager.add_instance(self.app_name, start=True, notify_primary=True)
            #self.carbon_manager.add_instance(self.app_name, start=True, notify_primary=False)
            #self.carbon_manager.del_instance(self.app_name, notify_primary=False)

            energy_target = self.co2_target_rate / co2_intensity
            target_num_rsrcs = (energy_target * self.rsrc_baseline) / self.energy_baseline
            target_num_rsrcs_diff = round(target_num_rsrcs - curr_rsrc_alloc)
            inst_pwd = self.carbon_manager.get_instance_estimate_power_usage(self.app_name)
            co2_overhead = inst_pwd * co2_int_next * 10.0
            #if co2_overhead == self.co2[iteration]:
            #    co2_overhead = co2_overhead / 8.0
            logger.info("co2 emission: {}\nco2 target: {}\ntarger_num_rsrcs: {}\ndiff: {}\nOverhead: {}".format(self.co2[iteration], self.co2_target_rate, target_num_rsrcs, target_num_rsrcs_diff, co2_overhead))
            #if (target_num_rsrcs_diff >= 1.0) or (self.co2[iteration] + co2_overhead) < (self.co2_target_rate):

            new_rsrcs = abs(round((self.co2[iteration] - self.co2_target_rate) / co2_overhead))
            if (self.co2[iteration] + co2_overhead) < (self.co2_target_rate):
                if vert == True:
                    rsrcs = abs((self.co2[iteration] - self.co2_target_rate) / co2_overhead)
                    diff_rsrcs = rsrcs - new_rsrcs
                    if diff_rsrcs > 0:
                        new_rsrcs = new_rsrcs + 1

                while (self.co2[iteration] + new_rsrcs*co2_overhead) > self.co2_target_rate:
                    new_rsrcs = max(1, new_rsrcs - 1)
                new_rsrcs = max(0, new_rsrcs - debit)
                logger.info("Scaling UP by {} resources".format(new_rsrcs))
                for i in range(new_rsrcs):
                    self.carbon_manager.add_instance(self.app_name, start=True, notify_primary=False)
                    if i != (new_rsrcs - 1):
                        time.sleep(0.5)
                    else:
                        if vert == True:
                            self.carbon_manager.enlarge_instance_rsrc(self.app_name, size=str((abs(diff_rsrcs))))
            #if (target_num_rsrcs_diff <= -1.0) or ((self.co2[iteration]) > (self.co2_target_rate + 0.1*co2_overhead)):
            if ((self.co2[iteration]) > (1.05*self.co2_target_rate)):
                tmp_rsrcs = 1
                if self.co2[iteration] / self.co2_target_rate > 1.2:
                    tmp_rsrcs = 6
                while (self.co2[iteration] - tmp_rsrcs*co2_overhead) > self.co2_target_rate:
                    tmp_rsrcs = tmp_rsrcs + 1
                #tmp_rsrcs = tmp_rsrcs + debit
                logger.info("Scaling DOWN by {} resources".format(tmp_rsrcs))
                for i in range(tmp_rsrcs):
                    try:
                        logger.info("Terminating instance {}".format(i+1))
                        self.carbon_manager.del_instance(self.app_name, notify_primary=False)
                        if i != (tmp_rsrcs - 1):
                            time.sleep(0.5)
                    except Exception as e:
                        logger.warning(e)
            self.primary_cpu_util[iteration] = cpu_util
                # print!
        logger.info("co2_scaler() returns")
        return 0.0, 0.0, 0.0, 0.0

    def check_to_stop(self, back_samples=5, min_threshold=3.0):
        num_samples = len(self.primary_cpu_util)
        if num_samples > back_samples:
            avg_util = 0.0
            for cpu_util in list(self.primary_cpu_util)[(num_samples - back_samples):(num_samples)]:
                avg_util = avg_util + self.primary_cpu_util[cpu_util]
            avg_util = avg_util / back_samples
            logger.info("Avg CPU Util: {}".format(avg_util))
            if avg_util < min_threshold:
                if self.click:
                    return True
                else:
                    self.click = True
            else:
                self.click = False
        return False

    def exposed_update_status(self, status):
        logger.info("New App {} status update: {}".format(self.app_name, status))
        self.status = status
        
    def notifier_callback(self, notification, notifier_id):
        #if notifier_id == hash("Battery-50%"):
        #    save_battery()
        #if notifier == "Carbon":
        #    save_Carbon()
        return None

    def exposed_update_job_parameter(self, parameter, value):
        self.job.update_parameter(parameter, value)

def start_server_thread(app_controller):
    # Then, open up a
    #t = ThreadPoolServer(Host(), port=18861, protocol_config={
    #t = ForkingServer(app_controller, port=app_controller.port, protocol_config={'allow_public_attrs':True,})
    t = ThreadPoolServer(app_controller, port=app_controller.port, protocol_config={'allow_all_attrs':True,})
    t.start()
    #return t

async def main():
    print('Carbon First - Application Controller (v0.1)')
    logging.basicConfig(format="[%(levelname)s] %(asctime)s %(message)s")

    app_controller = ApplicationController()
    app_controller.create_application("application.json")
    if app_controller.status == "LOADED":
        app_controller.submit_job()
        if app_controller.status == "SUBMITTED":
            start = app_controller.start_job()

    # Then, open up a 
    logger.info("Submit")

if __name__ == "__main__":
    asyncio.run(main())
