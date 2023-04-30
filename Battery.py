class Battery():
    def __init__(self, capacity, model, name=None, daemon=False):
        self.name = name
        self.capacity = capacity
        self.demand = 0.0
        self.rate = 0.0
        self.rate_footprint = 0.0
        self.last_rate = []
        self.curr_charge = 0.0 # float between 0.0 and 1.0
        self.last_charge = 0.0 # Last charge level before new supply
        self.discharge_rate = None
        self.max_discharge_rate = 0.0
        self.max_charge_rate = 0.0
        self.model = model

    def update_capacity(self, capacity=None, model_method=None, positional_arguments=None, keyword_arguments=None):
        if model_method == None:
            if capacity != None:
                self.capacity = capacity
        else:
            self.capacity = getattr(self.model, str(model_method))(*positional_arguments, **keyword_arguments)

    # To be called by a RM
    def refresh(self, rate):
        self.last_rate.add(self.rate)
        self.rate = rate
        charge_level = (100.0*self.rate) / self.capacity
        demand_level = (100.0*self.demand) / self.capacity
        self.last_charge = self.curr_charge
        if self.rate > 0.0:
            self.curr_charge = max(1.0, self.curr_charge + min(self.max_charge_rate, charge_level))
        else:
            self.curr_charge = min(0.0, self.curr_charge + charge_level)

        return self.curr_charge

    def update_parameter(self, parameter, value=None, model_method=None, positional_arguments=None, keyword_arguments=None):
        param = getattr(self, str(parameter))
        if model_method == None:
            param = value
        else:
            method_out = getattr(self.model, str(model_method))(*positional_arguments, **keyword_arguments)
            param = method_out
