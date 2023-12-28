import sys
import os

scriptPath = os.path.realpath(os.path.dirname(sys.argv[0]))
os.chdir(scriptPath)
# Put the path here
sys.path.append("../lib")

from lib.converter.Settings import Settings
from lib.converter.Settings import Verbosity
from lib.converter.Settings import ProjectionScenario
from lib.converter.Quincy_fluxnet22_parser import Quincy_Fluxnet22_Parser

set = Settings()
set.co2_concentration_file = '/Users/pp/data/co2/GCP2023_co2_global.dat'
set.co2_dC13_file = '/Users/pp/data/co2/delta13C_in_air_input4MIPs_GM_1850-2021_extrapolated.txt'
set.co2_DC14_file = '/Users/pp/data/co2/Delta14C_in_air_input4MIPs_SHTRNH_1850-2021_extrapolated.txt'

set.root_ndep_path = "/Volumes/BSI/data/OCN/input/gridded/NDEP/CESM-CAM"
set.ndep_projection_scenario = ProjectionScenario.RCP585
set.root_pdep_path ="/Volumes/BSI/work/quincy/model/InputDataSources/P-DEP"

set.phosphorus_input_path = "/Volumes/BSI/data/datastructure_bgi_cpy/grid/Global/0d50_static/Phosphorous/v2014_06/Data"
set.soil_grid_database_path = "/Volumes/BSI/data/datastructure_bgi_cpy/grid/Global/0d10_static/soilgrids/v0_5_1/Data"
set.lithology_map_path = "/Volumes/BSI/data/datastructure_bgi_cpy/grid/Global/0d50_static/GLiM/v1_0/Data/GLim.720.360.nc"
set.verbosity = Verbosity.Info
set.root_output_path = "/Users/pp/data/temp"
set.qmax_file = "/Users/pp/data/co2/qmax_org_values_per_nwrb_category_20180515.csv"

root_flux_path = "/Users/pp/data/jake_quincy_forcing"
sites = ["AT-Neu", "DE-Hai", "BR-Sa3", "FR-Pue", "US-Var"]
sites = ["AT-Neu"]

quincy_fluxnet22_forcing =  Quincy_Fluxnet22_Parser(settings = set,
                                          root_fluxnet_path = root_flux_path,
                                          sites= sites)
quincy_fluxnet22_forcing.parse()