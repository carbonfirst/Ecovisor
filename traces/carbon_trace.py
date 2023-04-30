import csv
import numpy as np
import pandas as pd
import datetime as dt
from Application import Application
import apps.COCO as COCO
from apps import ImgNet as ImgNet
from CarbonManager import RM
from Generator import Generator

def ReadCarbonTrace(file_path=None):
    try:
        carbon_trace_df = pd.read_csv(file_path, header=None)
        carbon_trace_df.columns = ["seconds", "carbon_emissions"]
        start_time = dt.datetime(2021, 5, 21, 0, 0)
        carbon_trace_df["time"] = start_time + pd.to_timedelta(
            carbon_trace_df["seconds"], unit="s"
        )
        carbon_trace_df = carbon_trace_df[["time", "carbon_emissions"]]

    except Exception as error:
        print("Caught this error: " + repr(error))

    return carbon_trace_df


def SampleCarbonTrace(trace_df=None, sampling_granularity_min=60):

    trace_df.index = trace_df.time
    resampled_trace_df = (
        trace_df.resample("{minutes}T".format(minutes=sampling_granularity_min))
        .mean()
        .interpolate(method="linear")
    )

    return resampled_trace_df.reset_index().iterrows()

def PandasToNumpyIterator(pd_iterator=None):
    for index, row in pd_iterator:
        yield np.array([np.datetime64(row.time), float(row.carbon_emissions)])


if __name__ == "__main__":
    trace_df = ReadCarbonTrace(file_path="../traces/carbon_trace.csv")
    pd_trace_iterator = SampleCarbonTrace(trace_df=trace_df, sampling_granularity_min=1)
    np_iterator = PandasToNumpyIterator(pd_iterator=pd_trace_iterator)
    carb_gen = Generator("CarbonTrace", np_iterator)

    datacenter = RM()
    datacenter.add_generator(carb_gen)

    max_power_allocation = 5000.0
    min_power_allocation = 2500.0

    non_def_app = ImgNet.ImgNet("ImgNet-NonDefer", max_power_allocation, max_power_allocation)
    total_work_non_def = non_def_app.non_defer_power_perf_model(max_power_allocation) * 24 * 3600 # 24h of work according
    #print(total_work_non_def)
    non_def_app.total_work = total_work_non_def
    non_def_app.remaining_work = total_work_non_def
    datacenter.add_application(non_def_app)

    def_app = ImgNet.ImgNet("ImgNet-Defer", min_power_allocation, max_power_allocation)
    total_work_def = def_app.non_defer_power_perf_model(max_power_allocation) * 24 * 3600
    #print(total_work_def)
    def_app.total_work = total_work_def
    def_app.remaining_work = total_work_def
    datacenter.add_application(def_app)

#    for iteration in range(num_samples):
#    with open("../traces/carbon_trace.csv") as csv_file:
    with open("../traces/carbon_trace_inverted.csv") as csv_file:
        csv_reader = csv.reader(csv_file, delimiter=',')
        iteration = 0

        i = 0
        for row in csv_reader:
            if iteration % 60 == 0:
                i = i + 1
                print(i)
                #if i == 1:
                if i == 11:
                    datacenter.applications[0].max_rscrs = 4000.0
                    datacenter.applications[0].min_rscrs = 4000.0
                    #datacenter.applications[1].max_rscrs = 4000.0
                    datacenter.applications[1].min_rscrs = 2000.0
                #if i == 2:
                if i == 8:
                    datacenter.applications[0].max_rscrs = 5000.0
                    datacenter.applications[0].min_rscrs = 5000.0
                    #datacenter.applications[1].max_rscrs = 5000.0
                    datacenter.applications[1].min_rscrs = 2500.0
                #if i == 3:
                if i == 7:
                    datacenter.applications[0].max_rscrs = 3000.0
                    datacenter.applications[0].min_rscrs = 3000.0
                    #datacenter.applications[1].max_rscrs = 3000.0
                    datacenter.applications[1].min_rscrs = 1500.0
                if i == 6:
                    datacenter.applications[0].max_rscrs = 5000.0
                    datacenter.applications[0].min_rscrs = 5000.0
                    #datacenter.applications[1].max_rscrs = 5000.0
                    datacenter.applications[1].min_rscrs = 2500.0

                #if i == 7:
                if i == 3:
                    datacenter.applications[0].max_rscrs = 4000.0
                    datacenter.applications[0].min_rscrs = 4000.0
                    #datacenter.applications[1].max_rscrs = 4000.0
                    datacenter.applications[1].min_rscrs = 2000.0

                #if i == 8:
                if i == 2:
                    datacenter.applications[0].max_rscrs = 3000.0
                    datacenter.applications[0].min_rscrs = 3000.0
                    #datacenter.applications[1].max_rscrs = 3000.0
                    datacenter.applications[1].min_rscrs = 1500.0

                #if i == 11:
                if i == 1:
                    datacenter.applications[0].max_rscrs = 5000.0
                    datacenter.applications[0].min_rscrs = 5000.0
                    #datacenter.applications[1].max_rscrs = 5000.0
                    datacenter.applications[1].min_rscrs = 2500.0

            total_emission = float(row[1])
            #total_emission = next(np_iterator)[1]
            #print(total_emission)
            datacenter.run(total_emission, iteration)
            iteration = iteration + 1

    #csv_row = "CarbonFootprint,NonDefer_App,NonDefer_Power,NonDefer_Footprint,NonDefer_RemaingWork,Defer_App,Defer_Power,Defer_Footprint,Defer_RemainingWork"
    
    with open('results.csv', mode='w') as results_file:
        results_writer = csv.writer(results_file, delimiter=',')
        #results_writer.writerow(['CarbonFootprint', 'NonDefer_App', 'NonDefer_Power', 'NonDefer_Footprint', 'NonDefer_RemaingWork', 'Defer_App', 'Defer_Power', 'Defer_Footprint', 'Defer_RemainingWork'])     
        results_writer.writerow(['CarbonFootprint', 'NonDefer_Power', 'NonDefer_Footprint', 'NonDefer_RemaingWork', 'Defer_Power', 'Defer_Footprint', 'Defer_RemainingWork'])

        for iteration in range(iteration):
            csv_row = []
            csv_row.append(datacenter.emissions[iteration])
            #print(datacenter.emissions[iteration]) 
            for app in datacenter.applications:
                #csv_row.append(app.name)
                csv_row.append(app.nominal_allocs[iteration])
                csv_row.append(app.carbon_footprint[iteration]) 
                csv_row.append(app.app_perf[iteration])

            #print(csv_row[0])
            results_writer.writerow(csv_row)

#    print(next(np_iterator)[1])
#    print(next(np_iterator))
