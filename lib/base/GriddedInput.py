import numpy as np
import pandas as pd
import netCDF4
import numbers

from lib.base.Fluxnet22_Jake import Fluxnet2022_Jake
from lib.converter.Settings import Verbosity
from lib.converter.Settings import ProjectionScenario


class GriddedInput:
    def __init__(self, root_path, verbosity_level : Verbosity):
        self.root_path = root_path
        self.verbosity_level = verbosity_level
        self.days_in_month = np.array([31,28,31,30,31,30,31,31,30,31,30,31])

    def get_index(self, coordinate_of_interest, available_coordinate_array):
        array = available_coordinate_array.copy()
        array -= coordinate_of_interest
        array = np.abs(array)
        min_index = np.argmin(array)
        if self.verbosity_level == Verbosity.Full:
            print(f"Selected {available_coordinate_array[min_index]} to represent the org coordinate {coordinate_of_interest}.")
        return min_index

    def extract(self, fluxnet_file: Fluxnet2022_Jake):
        raise NotImplementedError("Please implement the extract method for getting the mapped input back.")


class NdepositionForcing(GriddedInput):
    def __init__(self, root_path, projection_scenario: ProjectionScenario,  verbosity_level ):
        GriddedInput.__init__(self, root_path, verbosity_level)

        self.rcp_appendix  = ""
        if projection_scenario == ProjectionScenario.RCP585:
            self.rcp_appendix = "RCP8.5"
        elif projection_scenario == ProjectionScenario.RCP126:
            self.rcp_appendix = "RCP1.2"
        else:
            print("Invalid RCP scenario specified")
            exit(99)

    def extract(self, fluxnet_file: Fluxnet2022_Jake):

        lon = fluxnet_file.Lon
        lat = fluxnet_file.Lat

        year_min = fluxnet_file.Year_min
        year_max = fluxnet_file.Year_max

        # Loop through years bug make sure that year max is included!
        self.Data = pd.DataFrame(columns = ['year', 'doy', 'nhx', 'noy'])

        for year in range(year_min, year_max + 1):

            # Use historic N deposition before 1996 only
            if year < 1996:
                ds = netCDF4.Dataset(f"{self.root_path}/ndep_{year}.nc")
            else:
                ds = netCDF4.Dataset(f"{self.root_path}/ndep_{self.rcp_appendix}_{year}.nc")

            lon_array = ds['lon'][:]
            lat_array = ds['lat'][:]

            lon_index = self.get_index(lon, lon_array)
            lat_index = self.get_index(lat, lat_array)

            nhx_m = ds['NHx_deposition'][:, lat_index, lon_index] * 86400 * 1000 * 1000
            noy_m = ds['NOy_deposition'][:, lat_index, lon_index] * 86400 * 1000 * 1000

            nhx_d = np.zeros(365)
            noy_d = np.zeros(365)


            # SDitribute monthly means as daily values
            imin = 0
            for m in range(0, 12):
                imax  = imin + self.days_in_month[m]
                nhx_d[imin:imax] = nhx_m[m]
                noy_d[imin:imax] = noy_m[m]
                imin = imax

            new_rows = pd.DataFrame({
                                "year": (np.ones(365) * year).astype(int),
                                "doy": (np.arange(0, 365) + 1).astype(int),
                                "nhx": nhx_d,
                                "noy": noy_d})

            self.Data = pd.concat([self.Data, new_rows])

            ds.close()


class PdepositionForcing(GriddedInput):

    def __init__(self, root_path, verbosity_level):
        GriddedInput.__init__(self, root_path, verbosity_level)

    def extract(self, fluxnet_file: Fluxnet2022_Jake):
        lon = fluxnet_file.Lon
        lat = fluxnet_file.Lat

        ds = netCDF4.Dataset(f"{self.root_path}/nitrogenandphosphorus2x2annualdep.nc")

        lon_array = ds['lon'][:]

        # The lon array of the P deposition is in PM=360 which means it ranges from [0 to 360]
        # Transform to [-180 to +180]
        lon_array[lon_array > 180] -= 360

        lat_array = ds['lat'][:]

        lon_index = self.get_index(lon, lon_array)
        lat_index = self.get_index(lat, lat_array)

        self.p_dep = ds['pdep'][lat_index, lon_index] / 365.0

        ds.close()


class LithologyMap(GriddedInput):
    def __init__(self, root_path, verbosity_level):
        GriddedInput.__init__(self, root_path, verbosity_level)

    def extract(self, fluxnet_file: Fluxnet2022_Jake):
        lon = fluxnet_file.Lon
        lat = fluxnet_file.Lat

        ds = netCDF4.Dataset(f"{self.root_path}")

        lon_array = ds['lon'][:]
        # The lon array of the P deposition is in PM=360 which means it ranges from [0 to 360]
        # Transform to [-180 to +180]
        lon_array[lon_array > 180] -= 360
        lat_array = ds['lat'][:]

        lon_index = self.get_index(lon, lon_array)
        lat_index = self.get_index(lat, lat_array)

        self.Glim_class = ds['GLim'][lat_index, lon_index]

class SoilGridsDatabase(GriddedInput):

    def __init__(self, root_path, verbosity_level):
        GriddedInput.__init__(self, root_path, verbosity_level)

    def extract(self, fluxnet_file: Fluxnet2022_Jake):
        lon = fluxnet_file.Lon
        lat = fluxnet_file.Lat

        ds_bedrock = netCDF4.Dataset(f"{self.root_path}/BDRICM.soilgrid.3600.1800.nc")
        ds_taxousda = netCDF4.Dataset(f"{self.root_path}/TAXOUSDA_10km.soilgrid.3600.1800.nc")
        ds_taxnwrb = netCDF4.Dataset(f"{self.root_path}/TAXNWRB.soilgrid.3600.1800.nc")

        # Todo: Optimize lon checks and coordinate extraction
        lon_array = ds_bedrock['longitude'][:]
        # Check if lon array is in PM=360 projection
        lon_array[lon_array > 180] -= 360
        lat_array = ds_bedrock['latitude'][:]
        lon_index = self.get_index(lon, lon_array)
        lat_index = self.get_index(lat, lat_array)
        self.Depth_to_Bedrock = ds_bedrock['BDRICM'][lat_index, lon_index] / 100.0

        lon_array = ds_taxousda['longitude'][:]
        # Check if lon array is in PM=360 projection
        lon_array[lon_array > 180] -= 360
        lat_array = ds_taxousda['latitude'][:]
        lon_index = self.get_index(lon, lon_array)
        lat_index = self.get_index(lat, lat_array)
        self.Taxousda = int(ds_taxousda['TAXOUSDA_10km'][lat_index, lon_index])

        lon_array = ds_taxnwrb['longitude'][:]
        # Check if lon array is in PM=360 projection
        lon_array[lon_array > 180] -= 360
        lat_array = ds_taxnwrb['latitude'][:]
        lon_index = self.get_index(lon, lon_array)
        lat_index = self.get_index(lat, lat_array)
        self.Taxnwrb = int(ds_taxnwrb['TAXNWRB'][lat_index, lon_index])

        ds_bedrock.close()
        ds_taxousda.close()
        ds_taxnwrb.close()

class Phosphorus_Inputs(GriddedInput):

    def __init__(self, root_path, verbosity_level):
        GriddedInput.__init__(self, root_path, verbosity_level)
    def extract(self, fluxnet_file: Fluxnet2022_Jake):
        lon = fluxnet_file.Lon
        lat = fluxnet_file.Lat

        ds_labiles      = netCDF4.Dataset(f"{self.root_path}/Labile_Inorganic_P.nc")
        ds_sec_mineral  = netCDF4.Dataset(f"{self.root_path}/Seconday_Mineral__P.nc")
        ds_occluded     = netCDF4.Dataset(f"{self.root_path}/Occluded_P.nc")
        ds_apatite      = netCDF4.Dataset(f"{self.root_path}/Apatite_P.nc")

        lon_array = ds_labiles['longitude'][:]
        # The lon array of the P deposition is in PM=360 which means it ranges from [0 to 360]
        # Transform to [-180 to +180]
        lon_array[lon_array > 180] -= 360
        lat_array = ds_labiles['latitude'][:]

        self.lon_index = self.get_index(lon, lon_array)
        self.lat_index = self.get_index(lat, lat_array)

        self.P_labile_inorganic = ds_labiles['Labile_Inorganic_P'][self.lat_index, self.lon_index]
        self.P_slow = ds_sec_mineral['Seconday_Mineral__P'][self.lat_index, self.lon_index]
        self.P_occluded = ds_occluded['Occluded_P'][self.lat_index, self.lon_index]
        self.P_primary = ds_apatite['Apatite_P'][self.lat_index, self.lon_index]
        self.P_depth = 0.5


        if self.P_labile_inorganic.mask.all():
            self.P_labile_inorganic = self.extend_search(ds_labiles, 'Labile_Inorganic_P')

        if self.P_slow.mask.all():
            self.P_slow = self.extend_search(ds_sec_mineral, 'Seconday_Mineral__P')

        if self.P_occluded.mask.all():
            self.P_occluded = self.extend_search(ds_occluded, 'Occluded_P')

        if self.P_primary.mask.all():
            self.P_primary = self.extend_search(ds_apatite, 'Apatite_P')


        ds_labiles.close()
        ds_sec_mineral.close()
        ds_occluded.close()
        ds_apatite.close()


    def extend_search(self, ds : netCDF4.Dataset, var):

        success = False
        offset = 1

        while not success:

            lon_index_run = self.lon_index
            lat_index_run = self.lat_index - offset
            lat_index_run = lat_index_run % ds['latitude'].shape[0]
            val = ds[var][lat_index_run, lon_index_run]
            if not val.mask.all():
                success = True


            lon_index_run = self.lon_index
            lat_index_run = self.lat_index +  offset
            lat_index_run = lat_index_run % ds['latitude'].shape[0]
            val = ds[var][lat_index_run, lon_index_run]
            if not val.mask.all():
                success = True

            lon_index_run = self.lon_index - offset
            lat_index_run = self.lat_index
            lon_index_run = lon_index_run % ds['longitude'].shape[0]
            val = ds[var][lat_index_run, lon_index_run]
            if not val.mask.all():
                success = True

            lon_index_run = self.lon_index + offset
            lat_index_run = self.lat_index
            lon_index_run = lon_index_run % ds['longitude'].shape[0]
            val = ds[var][lat_index_run, lon_index_run]
            if not val.mask.all():
                success = True

            lon_index_run = self.lon_index - offset
            lat_index_run = self.lat_index - offset
            lat_index_run = lat_index_run % ds['latitude'].shape[0]
            lon_index_run = lon_index_run % ds['longitude'].shape[0]
            val = ds[var][lat_index_run, lon_index_run]
            if not val.mask.all():
                success = True

            lon_index_run = self.lon_index + offset
            lat_index_run = self.lat_index - offset
            lat_index_run = lat_index_run % ds['latitude'].shape[0]
            lon_index_run = lon_index_run % ds['longitude'].shape[0]
            val = ds[var][lat_index_run, lon_index_run]
            if not val.mask.all():
                success = True

            lon_index_run = self.lon_index - offset
            lat_index_run = self.lat_index + offset
            lat_index_run = lat_index_run % ds['latitude'].shape[0]
            lon_index_run = lon_index_run % ds['longitude'].shape[0]
            val = ds[var][lat_index_run, lon_index_run]
            if not val.mask.all():
                success = True

            lon_index_run = self.lon_index + offset
            lat_index_run = self.lat_index + offset
            lat_index_run = lat_index_run % ds['latitude'].shape[0]
            lon_index_run = lon_index_run % ds['longitude'].shape[0]
            val = ds[var][lat_index_run, lon_index_run]
            if not val.mask.all():
                success = True


            offset +=1

        if self.verbosity_level == Verbosity.Info:
            print(f"Using {lon_index_run} and {lat_index_run} with offset {offset} to estimate {var}. Obtained value: {val}")

        return val
