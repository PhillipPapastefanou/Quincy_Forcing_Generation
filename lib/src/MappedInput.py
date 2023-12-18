import numpy as np
from C13_NEE_PART.lib.src.Fluxnet2022_Jake import Fluxnet2022_Jake
from C13_NEE_PART.lib.converter.Settings import Verbosity
from C13_NEE_PART.lib.converter.Settings import ProjectionScenario
import pandas as pd
import netCDF4





class MappedInput:
    def __init__(self, root_path, verbosity_level : Verbosity):
        self.root_path = root_path
        self.verbosity_level = verbosity_level

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


class NdepositionForcing(MappedInput):
    def __init__(self, root_path, projection_scenario: ProjectionScenario,  verbosity_level ):
        MappedInput.__init__(self, root_path, verbosity_level)

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
        self.Data = pd.DataFrame(columns = ['year', 'nhx', 'noy'])

        index = 0
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

            nhx = np.sum(nhx_m)
            noy = np.sum(noy_m)

            new_row = pd.Series({"year": int(year),  "nhx": nhx, "noy": noy})

            self.Data.loc[index] = new_row
            index += 1

            ds.close()


class PdepositionForcing(MappedInput):

    def __init__(self, root_path, verbosity_level):
        MappedInput.__init__(self, root_path, verbosity_level)

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

        self.p_dep = ds['pdep'][lat_index, lon_index]/365.0

        ds.close()


class SoilGridsDatabase(MappedInput):

    def __init__(self, root_path, verbosity_level):
        MappedInput.__init__(self, root_path, verbosity_level)

    def extract(self, fluxnet_file: Fluxnet2022_Jake):
        lon = fluxnet_file.Lon
        lat = fluxnet_file.Lat

        ds = netCDF4.Dataset(f"{self.root_path}/BDRICM.soilgrid.3600.1800.nc")

        lon_array = ds['longitude'][:]

        # The lon array of the P deposition is in PM=360 which means it ranges from [0 to 360]
        # Transform to [-180 to +180]
        lon_array[lon_array > 180] -= 360

        lat_array = ds['latitude'][:]


        lon_index = self.get_index(lon, lon_array)
        lat_index = self.get_index(lat, lat_array)

        self.Depth_to_Bedrock = ds['BDRICM'][lat_index, lon_index]/100.0

        ds.close()


class SW_Distribution(MappedInput):


    def __init__(self, root_path):
        MappedInput.__init__(self, root_path)


    def extract(self, lon, lat):

        ds = netCDF4.Dataset(f"{self.root_path}")

        lon_array = ds['lon'][:]
        lat_array = ds['lat'][:]

        coi = np.array([lon,lat])

        coords_avail = np.array([lon_array, lat_array])


        coords_diff = np.abs(coords_avail - coi[:,np.newaxis])

        mse = coords_diff[0,:]**2 + coords_diff[1,:]**2

        min_index = np.argmin(mse)

        print( coords_avail[:,min_index])

        self.SW_Dataset = ds['insol'][min_index, :]

        ds.close()