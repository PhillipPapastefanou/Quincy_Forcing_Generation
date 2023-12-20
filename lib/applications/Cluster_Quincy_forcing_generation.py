import sys
import os

import pandas as pd

scriptPath = os.path.realpath(os.path.dirname(sys.argv[0]))
os.chdir(scriptPath)
# Put the path here
sys.path.append("../../")

from lib.converter.Settings import Settings
from lib.converter.Settings import Verbosity
from lib.converter.Settings import ProjectionScenario
from lib.scripts.Quincy_Fluxnet22_Forcing import Quincy_Fluxnet22_Forcing

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
set.root_output_path = "/Net/Groups/BSI/work_scratch/ppapastefanou/forcing_generation/2023"
set.first_transient_forcing_year = 1901


root_flux_path = "/Net/Groups/BGI/work_1/scratch/fluxcom/sitecube_proc/model_files_20231129"

sites = pd.read_csv("Sitenames_and_PFTs.csv")['Sitename']
sites = sites[1:2]
#sites = ["AT-Neu", "DE-Hai", "BR-Sa3", "FR-Pue", "US-Var"]


static_forcing =  Quincy_Fluxnet22_Forcing(settings = set,
                                           root_fluxnet_path = root_flux_path,
                                           sites= sites)
static_forcing.parse()
