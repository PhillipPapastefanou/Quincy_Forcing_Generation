import sys
import os
import pandas as pd
from datetime import timedelta
from time import perf_counter
from mpi4py import MPI

scriptPath = os.path.realpath(os.path.dirname(sys.argv[0]))
os.chdir(scriptPath)
# Put the path here
sys.path.append("..")

from lib.converter.Settings import Settings
from lib.converter.Settings import Verbosity
from lib.converter.Settings import ProjectionScenario
from lib.converter.Quincy_fluxnet22_parser import Quincy_Fluxnet22_Parser
from lib.converter.Quincy_fluxnet22_parser_parallel import Quincy_Fluxnet22_Parser_Parallel


set = Settings()
set.co2_concentration_file = '/Net/Groups/BSI/people/ppapastefanou/climate_aux/co2/GCP2023_co2_global.dat'
set.co2_dC13_file = '/Net/Groups/BSI/people/ppapastefanou/climate_aux/co2/delta13C_in_air_input4MIPs_GM_1850-2021_extrapolated.txt'
set.co2_DC14_file = '/Net/Groups/BSI/people/ppapastefanou/climate_aux/co2/Delta14C_in_air_input4MIPs_SHTRNH_1850-2021_extrapolated.txt'

set.root_ndep_path = "/Net/Groups/BSI/data/OCN/input/gridded/NDEP/CESM-CAM"
set.ndep_projection_scenario = ProjectionScenario.RCP585
set.root_pdep_path ="/Net/Groups/BSI/work/quincy/model/InputDataSources/P-DEP"

set.lithology_map_path = "/Net/Groups/BSI/data/datastructure_bgi_cpy/grid/Global/0d50_static/GLiM/v1_0/Data/GLim.720.360.nc"
set.soil_grid_database_path = "/Net/Groups/BSI/data/datastructure_bgi_cpy/grid/Global/0d10_static/soilgrids/v0_5_1/Data"
set.phosphorus_input_path = "/Net/Groups/BSI/data/datastructure_bgi_cpy/grid/Global/0d50_static/Phosphorous/v2014_06/Data"
set.qmax_file = "/Net/Groups/BSI/people/ppapastefanou/data/qmax_org_values_per_nwrb_category_20180515.csv"

set.verbosity = Verbosity.Info
set.root_output_path = "/Net/Groups/BSI/work_scratch/ppapastefanou/FLUXNET_QUINCY_test"
set.root_output_path = "/Net/Groups/BSI/scratch/ppapastefanou/FLUXNET_QUINCY_test_2big"
set.first_transient_forcing_year = 1901



root_flux_path = "/Net/Groups/BGI/work_1/scratch/fluxcom/sitecube_proc/model_files_20231129"
root_flux_path = "/Net/Groups/BSI/scratch/ppapastefanou/fluxnet/model_files_20231129"

sites = pd.read_csv("Sitenames_and_PFTs.csv")['Sitename']
#sites = sites[0:50]
sites = ["CA-ARB"]
#sites = ["AT-Neu", "DE-Hai", "BR-Sa3", "FR-Pue", "US-Var"]



# Initialize MPI
comm = MPI.COMM_WORLD
rank = comm.Get_rank()
size = comm.Get_size()

if rank == 0:
    t_begin = perf_counter()


quincy_fluxnet22_forcing =  Quincy_Fluxnet22_Parser_Parallel(settings = set,
                                          root_fluxnet_path = root_flux_path,
                                          sites= sites,
                                        comm=comm, rank=rank, size=size)

quincy_fluxnet22_forcing.send_parameter_indexes()

quincy_fluxnet22_forcing.parse()

if rank == 0:
    t_end = perf_counter()
    td = timedelta(seconds=t_end-t_begin)
    print(f"Whole conversion time: {td}")
