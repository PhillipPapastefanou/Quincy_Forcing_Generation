import pandas as pd
import numpy as np

from lib.src.Fluxnet2022_Jake import Fluxnet2022_Jake
from lib.converter.Model_Forcing_Input import SW_Input_Parser
from lib.converter.Model_Forcing_Input import LW_Input_Parser
from lib.converter.Model_Forcing_Input import Tair_Input_Parser
from lib.converter.Model_Forcing_Input import Precipitation_Input_Parser
from lib.converter.Model_Forcing_Input import Qair_Input_Parser
from lib.converter.Model_Forcing_Input import Pressure_Input_Parser
from lib.converter.Model_Forcing_Input import Windspeed_Input_Parser

from lib.src.GriddedInput import NdepositionForcing
from lib.src.GriddedInput import PdepositionForcing

from lib.converter.Settings import Settings
from lib.converter.Base_Parsing import Base_Parsing

class Quincy_Fluxnet_2022_Forcing(Base_Parsing):
    def __init__(self, settings: Settings):

        # Set random number generator seed
        np.random.seed(42)

        Base_Parsing.__init__(self, settings = settings)

        # Fluxnet specific variable name according to QUINCY conventions
        self.quincy_fluxnet_columns = ['year', 'doy', 'hour', 'swvis_srf_down', 'lw_srf_down', 't_air', 'q_air', 'press_srf',
             'rain','snow','wind_air']

        self.quincy_full_forcing_columns = ['year','doy','hour','swvis_srf_down','lw_srf_down','t_air','q_air','press_srf',
             'rain','snow','wind_air','co2_mixing_ratio','co2_dC13','co2DC14','nhx_srf_down','noy_srf_down','p_srf_down']

        # Unit row according to QUINCY forcing
        self.quincy_unit_row = ['-', '-', '-', 'Wm-2', 'Wm-2', 'K', 'g/kg', 'hPa', 'mm/day', 'mm/day', 'm/s', 'ppm', 'per-mill', 'per-mill', 'mg/m2/day', 'mg/m2/day', 'mg/m2/day']

        # Fluxnet Variables according to jnelson naming conventions
        self.fluxnet_forcing_columns = ['SWdown', 'LWdown', 'Tair', 'Precip', 'Qair', 'Psurf', 'Wind']

    def Connect_to_fluxnet(self, fnet : Fluxnet2022_Jake):
        self.fnet = fnet

    def Parse_forcing(self):
        # Reading variables from fluxnet site
        self.dprint("Read fluxnet forcing..", lambda: self.fnet.Read_forcing_variables(self.fluxnet_forcing_columns))

        # Retrieve longitude and latitude
        self.Lat = self.fnet.Lat
        self.Lon = self.fnet.Lon

        # Create main QUINCY forcing dataframe
        self.DataFrame = pd.DataFrame(columns = self.quincy_fluxnet_columns)

        # Add year and day of year from the file
        self.DataFrame['year'] = self.fnet.df['date'].dt.year
        self.DataFrame['doy'] = self.fnet.df['date'].dt.day_of_year
        # Add hour based on input, but substract 15 min offset from files
        # Todo make this generic by checking whether there is an offset at the file
        hour_dec = self.fnet.df['date'].dt.hour + self.fnet.df['date'].dt.minute / 60.0 - 0.25
        self.DataFrame['hour'] = hour_dec

        # Parse fluxnet forcing
        self.dprint("Parsing fluxnet forcing..", lambda: self._parse_fluxnet_forcing(self.fnet))

        # Parse CO2 forcing
        self.dprint("Parsing CO2..", lambda: self._parse_co2_forcing())

        # Parse dC13 and DC14
        self.dprint("Parsing dCO2-13 and 14..", lambda:self._parse_dC13_and_DC14())

        # Parse phosphorus deposition
        self.dprint("Parsing P deposition..",lambda: self._parse_p_depositions())

        # Parse nitrogen deposition
        self.dprint("Parsing N deposition..", lambda:self._parse_n_deposition())

        # Testing for Nan
        self.dprint("Testing for missing values..", lambda: self._testing_for_nan())

    def Export_static_forcing(self):
        # Exporting QUINCY file
        self.dprint("Exporting static forcing data..", lambda:self._export_static())

    def Export_transient_forcing(self):
        # Exporting QUINCY file
        self.dprint("Exporting transient forcing data..", lambda: self._generate_and_export_transient_forcing())

    def _parse_co2_forcing(self):
        # Parse main CO2
        df_co2 = pd.read_csv(self.settings.co2_concentration_file, delim_whitespace=True, header=None)
        df_co2.columns = ['year', 'co2_mixing_ratio']
        self.DataFrame = pd.merge(self.DataFrame, df_co2, on='year')

    def _parse_fluxnet_forcing(self, fnet):
        # Set up unit parser
        sw_parser = SW_Input_Parser(fnet.units['SWdown'])
        lw_parser = LW_Input_Parser(fnet.units['LWdown'])
        temp_parser = Tair_Input_Parser(fnet.units['Tair'])
        precip_parser = Precipitation_Input_Parser(fnet.units['Precip'])
        qair_parser = Qair_Input_Parser(fnet.units['Qair'])
        psurv_parser = Pressure_Input_Parser(fnet.units['Psurf'])
        ws_parser = Windspeed_Input_Parser(fnet.units['Wind'])

        # parse fluxnet data by passing through the parsers
        self.DataFrame['swvis_srf_down'] = sw_parser.convert(fnet.df['SWdown'])
        self.DataFrame['lw_srf_down'] = lw_parser.convert(fnet.df['LWdown'])
        self.DataFrame['t_air'] = temp_parser.convert(fnet.df['Tair'])
        self.DataFrame['q_air'] = qair_parser.convert(fnet.df['Qair'])
        self.DataFrame['press_srf'] = psurv_parser.convert(fnet.df['Psurf'])
        self.DataFrame['rain'] = precip_parser.convert(fnet.df['Precip'])
        # Sofar we have no snow input
        self.DataFrame['snow'] = 0.0
        self.DataFrame['wind_air'] = ws_parser.convert(fnet.df['Wind'])

        # Removing too low wind values to avoid model instabilities
        self.DataFrame.loc[self.DataFrame['wind_air'] < 0.1, 'wind_air'] = 0.1


    def _parse_dC13_and_DC14(self):
        df_co2_dC13 = pd.read_csv(self.settings.co2_dC13_file,
                                  delim_whitespace=True, header=None)
        df_co2_dC13.columns = ['year', 'co2_dC13']
        df_co2_dC13['year'] = df_co2_dC13['year'] - 0.5
        df_co2_dC13['year'] = df_co2_dC13['year'].astype(int)
        self.DataFrame = pd.merge(self.DataFrame, df_co2_dC13, on='year')

        # Parse DC14
        df_co2_DC14 = pd.read_csv(self.settings.co2_DC14_file,
                                  delim_whitespace=True, header=None)
        df_co2_DC14.columns = ['year', '1', '2', '3']
        df_co2_DC14['year'] = df_co2_DC14['year'] - 0.5
        df_co2_DC14['year'] = df_co2_DC14['year'].astype(int)

        if self.Lat > 30.0:
            c14_index = 1
        elif (self.Lat > - 30.0) & (self.Lat <= 30.0):
            c14_index = 2
        elif self.Lat <= -30.0:
            c14_index = 3
        else:
            print("This should not happen")
            exit(99)

        df_c14_slice = df_co2_DC14[['year', str(c14_index)]]
        df_c14_slice = df_c14_slice.rename(columns={str(c14_index): 'co2DC14'})
        self.DataFrame = pd.merge(self.DataFrame, df_c14_slice, on='year')

    def _parse_p_depositions(self):
        rt_path_p = self.settings.root_pdep_path
        p_dep_forcing = PdepositionForcing(root_path=rt_path_p, verbosity_level= self.settings.verbosity)
        p_dep_forcing.extract(self.fnet)
        self.DataFrame["p_srf_down"] = p_dep_forcing.p_dep

    def _parse_n_deposition(self):
        rt_path_n = self.settings.root_ndep_path
        n_dep_forcing = NdepositionForcing(root_path=rt_path_n, projection_scenario=self.settings.ndep_projection_scenario, verbosity_level= self.settings.verbosity)
        n_dep_forcing.extract(self.fnet)
        self.DataFrame = pd.merge(self.DataFrame, n_dep_forcing.Data, on='year')
        self.DataFrame = self.DataFrame.rename(columns={'nhx': 'nhx_srf_down', 'noy': 'noy_srf_down'})

    def _export_static(self):
        df_export = self.DataFrame.copy()

        # Round values according to 4 significant figures
        for var in self.quincy_full_forcing_columns:
            df_export[var] = round(df_export[var], 4)
            df_export[var] = df_export[var].apply(pd.to_numeric, downcast='float').fillna(0)

        # Make sure that columns are sorted according to QUINCY's needs
        df_export = df_export[self.quincy_full_forcing_columns]

        # Insert first unit row
        df_export.loc[-1] = self.quincy_unit_row
        df_export.index = df_export.index + 1
        df_export = df_export.sort_index()

        # Export file acccording to QUINCY standards
        outSiteFile = f"{self.settings.root_output_path}/{self.fnet.sitename}_s_{self.fnet.Year_min}-{self.fnet.Year_max}.dat"
        df_export.to_csv(outSiteFile, header=True, sep=" ", index=None)

    def _generate_and_export_transient_forcing(self):

        df_fluxnet = self.DataFrame.copy()

        year_min = self.fnet.Year_min
        year_max = self.fnet.Year_max
        year_f0 = self.settings.first_transient_forcing_year

        years_to_be_sampled = np.arange(year_f0, year_min)

        years_available = np.arange(year_min, year_max + 1)

        # Create transient dataframe
        df_transient = pd.DataFrame()

        for year in years_to_be_sampled:

            year_sampled = np.random.choice(years_available)

            df_slice = df_fluxnet[df_fluxnet['year'] == year_sampled].copy()
            df_slice['year'] = year

            df_transient = pd.concat([df_transient, df_slice], ignore_index=True)
            print(f"{year} sampled from {year_sampled}.")


        # Add the org. fluxnet data add the end of the file
        df_transient = pd.concat([df_transient, df_fluxnet], ignore_index=True)

        # Round values according to 4 significant figures
        for var in self.quincy_full_forcing_columns:
            df_transient[var] = round(df_transient[var], 4)
            df_transient[var] = df_transient[var].apply(pd.to_numeric, downcast='float').fillna(0)

        # Make sure that columns are sorted according to QUINCY's needs
        df_transient = df_transient[self.quincy_full_forcing_columns]

        # Resetting index to avoid year scrable after sorting
        df_transient.reset_index()

        # Insert first unit row
        df_transient.loc[-1] = self.quincy_unit_row
        df_transient.index = df_transient.index + 1
        df_transient = df_transient.sort_index()

        # Export file acccording to QUINCY standards
        outSiteFile = f"{self.settings.root_output_path}/{self.fnet.sitename}_t_{self.fnet.Year_min}-{self.fnet.Year_max}.dat"
        df_transient.to_csv(outSiteFile, header=True, sep=" ", index=None)


    def _testing_for_nan(self):
        nan_rows = self.DataFrame[self.DataFrame.isna().any(axis=1)].shape[0]
        if nan_rows > 0:
            raise Exception(f"Error: Found {nan_rows} rows with nan values. Skipping location...")