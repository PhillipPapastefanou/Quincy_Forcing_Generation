import sys
import os

scriptPath = os.path.realpath(os.path.dirname(sys.argv[0]))
os.chdir(scriptPath)
sys.path.append("../lib/converter")


from converter.Settings import Settings
from converter.Settings import Verbosity
from converter.Settings import ProjectionScenario

from converter.Quincy_fluxnet_2022_site_data_factory import Quincy_Site_Data_Factory
from converter.Quincy_fluxnet_2022_forcing import Quincy_Fluxnet_2022_Forcing
from converter.Quincy_fluxnet_2022_site_data import Quincy_Site_Data

from src.Fluxnet2022_Jake import Fluxnet2022_Jake

set = Settings()
set.co2_concentration_file = '/Users/pp/data/co2/GCP2023_co2_global.dat'
set.co2_dC13_file = '/Users/pp/data/co2/delta13C_in_air_input4MIPs_GM_1850-2021_extrapolated.txt'
set.co2_DC14_file = '/Users/pp/data/co2/Delta14C_in_air_input4MIPs_SHTRNH_1850-2021_extrapolated.txt'

set.root_ndep_path = "/Volumes/BSI/data/OCN/input/gridded/NDEP/CESM-CAM"
set.ndep_projection_scenario = ProjectionScenario.RCP585
set.root_pdep_path ="/Volumes/BSI/work/quincy/model/InputDataSources/P-DEP"

set.soil_grid_database_path = "/Volumes/BSI/data/datastructure_bgi_cpy/grid/Global/0d10_static/soilgrids/v0_5_1/Data"
set.verbosity = Verbosity.Info
set.root_output_path = "/Users/pp/data/temp"

root_flux_path = "/Users/pp/data/jake_quincy_forcing"

quincy_site_data_factory = Quincy_Site_Data_Factory(settings= set)

site_name = "At-Neu"

fnet = Fluxnet2022_Jake(rtpath = root_flux_path, sitename = site_name)
fnet.Read_And_Parse_Time()

qf = Quincy_Fluxnet_2022_Forcing(settings = set)
qf.Connect_to_fluxnet(fnet = fnet)
qf.Parse_forcing()
qf.Export()

qsd = Quincy_Site_Data(fluxnet_file= fnet, settings=set)
qsd.Parse_Environmental_Data()
qsd.Parse_PFT_Fractions(qf = qf)

quincy_site_data_factory.Add_site(qsd = qsd)
quincy_site_data_factory.Export()




