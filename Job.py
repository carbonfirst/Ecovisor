import time
import logging

class Job():
    def __init__(self, name):
        self.name = name
        self.regex = None
        self.container_image = None
        self.pull_server = None
        self.protocol = None
        self.project = None
        self.num_containers = 0
        self.containers = {}
        self.update_cmd = None
        self.primary_instance = None
        self.min_rscrs = None
        self.max_rscrs = None
        self.total_work = 0
        self.vsolar_panels = None
        self.vbattery = None
        self.virtual_generator = None
        self.total_co2_quota = None
        self.total_grid_quota = None
        self.creation_time = None
        self.stop_time = None
        self.vars = []
        self.template = None
        self.status = None

        self.nominal_allocs = []
        self.nominal_rscr_alloc = 0.0

        self.remaining_work = self.total_work
        #self.runnin_rscr = rscr
        self.carbon_footprint = []
        self.max_footprint = 30
        self.app_perf = []

        # Non&Defer_Perf = cte * Power^exp
        self.cte_power_perf_non_defer = 0.0
        self.exp_power_perf_non_defer = 0.0

        self.cte_power_perf_defer = 0.0
        self.exp_power_perf_defer = 0.0

        # Non&Defer_Power = cte * Perf^exp
        self.cte_perf_power_non_defer = 0.0
        self.exp_perf_power_non_defer =0.0

        self.cte_perf_model_defer = 0.0
        self.exp_perf_model_defer = 0.0

        global logger
        logger = logging.getLogger(__name__)
        logger.setLevel(logging.INFO)
        logging.basicConfig(format="[%(levelname)s] %(asctime)s %(message)s")

    def co2_autoscale(self, footprint, iteration, scaler="default", dc_density=10000.0,):
        allocation = 0.0
        #curr_footprint = footprint[len(footprint)]
        curr_footprint = footprint
        #print("Foot: %s" % footprint)
        if (self.app_type() == "defer"):
            if scaler == "default": # Relate max_footprint with curr_footprint and remaining work
                if curr_footprint == 0.0:
                    allocation = self.max_rscrs
                else:
                    rsc_range = self.max_rscrs - self.min_rscrs

                    L = 1.0
                    #x0 = 75.0
                    x0 = 75.0
                    k = -0.1
                    x_x0 = curr_footprint - x0
                    lf = L / (1.0 + (2.71828 ** (k * x_x0)))
                    #print(lf)
                    allocation = self.min_rscrs + (1.0 - lf)*rsc_range
        else:
            #print("Non-deferrable apps cannot be autoscaled")
            allocation = self.max_rscrs

        self.nominal_rscr_alloc = allocation
        #print(self.nominal_rscr_alloc)
        self.nominal_allocs.append(self.nominal_rscr_alloc)
        return self.nominal_rscr_alloc

    def app_type(self):
        if (self.min_rscrs != self.max_rscrs):
            return "defer"
        else: 
            return "non_defer"

    def run(self, footprint, W=10000.0):
        perf = 0.0

        if self.app_type() == "defer": # The performance depends on the app type
            perf = self.non_defer_power_perf_model(self.nominal_rscr_alloc)
            #print(self.nominal_rscr_alloc)
            #print(perf)
        else:
            perf = self.non_defer_power_perf_model(self.nominal_rscr_alloc)

        #print(perf)
        self.remaining_work = max(self.remaining_work - (perf*180.0), 0.0)
        self.app_perf.append(self.remaining_work)
        if self.remaining_work <= 0:
            self.carbon_footprint.append(0)
        else:
            #print(self.remaining_work)
            #self.app_perf.append(self.remaining_work)
            run_footprint = (self.nominal_rscr_alloc * footprint) / (self.max_rscrs + self.min_rscrs)
            #print(run_footprint)
            self.carbon_footprint.append(run_footprint)


    def model(self, cte, exp, num_samples):
        return cte * (num_samples ** exp)

    def non_defer_power_perf_model(self, power): # Given power, it returns how many batched samples per second it processes
        return self.model(self.cte_power_perf_non_defer, self.exp_power_perf_non_defer, power)

    def defer_power_perf_model(self, power): # Given power, it returns how many queries per second it can process
        return self.model(self.cte_power_perf_defer, self.exp_power_perf_defer, power)

    def non_defer_perf_power_model(self, perf): # Given performance (samples per sec), it returns how much power is needed to process it
        return self.model(self.cte_perf_power_non_defer, self.exp_perf_power_non_defer, perf)

    def defer_perf_power_model(self, num_queries): # Given performance (queries per sec), it returns how much power is needed to process it
        return self.model(self.x_perf_model_defer, self.exp_perf_model_defer, num_queries)

    def update_parameter(self, parameter, value):
        try:
            param = getattr(self, str(parameter))
            param = value
        except Exception as e:
            logger.error("Error while updating Job {}'s parameter {}: {}".format(self.name, parameter, e))

class Container():
    def __init__(self, name, job=None, rm_obj=None):
        self.name = name
        self.container_image = None
        self.pull_server = None
        self.protocol = None
        self.job = job
        self.obj = rm_obj
        self.cpu_quota = None
        self.start_cmd = None
        self.pid_file = None
        self.creation_time = time.time()
        self.last_query_time = self.creation_time
        self.last_cpu_util = 0.0