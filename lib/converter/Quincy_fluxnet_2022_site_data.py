import numpy as np
import netCDF4
from C13_NEE_PART.lib.src import SoilGridsDatabase
from C13_NEE_PART.lib.src.Fluxnet2022_Jake import Fluxnet2022_Jake
from C13_NEE_PART.lib.converter import Settings
from C13_NEE_PART.lib.converter.Quincy_fluxnet_2022_forcing import Quincy_Fluxnet_2022_Forcing
from C13_NEE_PART.lib.src import Quincy_Orchidee_PFT
from C13_NEE_PART.lib.src import Quincy_Orchidee_PFT_List
from C13_NEE_PART.lib.converter.Settings import Verbosity


class Quincy_Site_Data:

    def __init__(self, fluxnet_file :Fluxnet2022_Jake, settings : Settings):
        self.fnet = fluxnet_file
        self.settings = settings

    def Parse_Environmental_Data(self):

        ds = netCDF4.Dataset(self.fnet.fname_path_meteo)

        # Longitude reference to account for solar inclination difference
        self.Gmt_ref = self.fnet.Lon

        # Get clay fraction and convert from % to fraction
        self.Clay_fraction = ds['CLYPPT'][0,0] / 100.0

        # Get silt fraction and convert from % to fraction
        self.Silt_fraction = ds['SLTPPT'][0,0] / 100.0

        # Get sand fraction and convert from % to fraction
        self.Sand_fraction = ds['SNDPPT'][0,0] / 100.0

        # Saturated water content (volumetric fraction) for tS  [fraction]
        Fc_vol_sg = ds['AWCtS'][0, 0] / 100.0

        # Available soil water capacity (volumetric fraction) until wilting point
        pwp_vol_sg = ds['WWP'][0, 0] / 100.0

        # Obtain depth to berock from the soil grids database
        soil_grid_database = SoilGridsDatabase(self.settings.soil_grid_database_path, self.settings.verbosity)
        soil_grid_database.extract(fluxnet_file = self.fnet)
        depth_to_bedrock = soil_grid_database.Depth_to_Bedrock

        if (Fc_vol_sg < pwp_vol_sg) & (self.settings.verbosity == Verbosity.Warning):
            print(f"Saturated water content ({Fc_vol_sg}) is less then available water capacity ({pwp_vol_sg}).")

        self.AWC = (Fc_vol_sg - pwp_vol_sg) * depth_to_bedrock * 1000.0;

        # Get soil PH from the data
        self.PH = ds['PHIHOX'][0,0] / 10.0

        # Set Nleaf to missing value
        self.Nleaf = -9999.0

        # Set SLA to missing value
        self.SLA = -9999.0

        # Set height to missing value
        self.Height = -9999.0

        # Set Age to missing value
        self.Age = -9999

        # Set BG ?? to missing value
        # Todo figure out what BG means
        self.BG = -9999

        # Copy IGBP str
        self.PFT_biome_str = ds.pft

        ds.close()

        ds_rs = netCDF4.Dataset(self.fnet.fname_path_rs)
        # Take the first LAI value
        self.LAI = ds_rs['LAI'][0,0,0]

        ds_rs.close()

    def Parse_PFT_Fractions(self, qf : Quincy_Fluxnet_2022_Forcing):

        KelvinToCelcius = 273.15

        df = qf.DataFrame.copy()
        df['t_air_C']  = df['t_air'] - KelvinToCelcius
        df['date'] = self.fnet.df['date']

        self.Temp_monthly_avg_min = df['t_air_C'].groupby([df['date'].dt.year, df['date'].dt.month]).mean().min()
        self.Temp_yearly_avg = df['t_air_C'].groupby([df['date'].dt.year]).mean().mean()
        # Mulitply times 365 because rainfall is per day
        self.Rain_yearly_sum_avg = (df['rain'].groupby([df['date'].dt.year]).mean() * 365.0).mean()

        self._parse_IGBP_string(IGBP_str        = self.PFT_biome_str,
                                T_monthly_min   = self.Temp_monthly_avg_min,
                                T_yearly_avg    = self.Temp_yearly_avg,
                                P_yearly_sum    = self.Rain_yearly_sum_avg,
                                )

    def _parse_IGBP_string(self, IGBP_str, T_monthly_min, T_yearly_avg, P_yearly_sum):

        # Parsing PFT constants according to Sönke
        min_tropical_precipitation = 1200
        min_tropical_temperature = 15
        min_temperate_temperature_broadleaved = 2
        min_temperate_temperature_needleleaved = -2

        self.PFT_list = Quincy_Orchidee_PFT_List()

        # Sönke's magic conversion formula for C3 or C4 grasses
        frac_C4 = np.round(0.8 * (1.0 - np.exp(-((np.max([0.0, T_yearly_avg - 7.5])) / 10.0) ** 2.0)))
        frac_C3 = 1.0 - frac_C4

        if (IGBP_str == 'CRO'):
            self.PFT_list.Fractions[Quincy_Orchidee_PFT.TeHC] = frac_C3
            self.PFT_list.Fractions[Quincy_Orchidee_PFT.TrHC] = frac_C4

        elif (IGBP_str == 'GRA'):
            self.PFT_list.Fractions[Quincy_Orchidee_PFT.TeH] = frac_C3
            self.PFT_list.Fractions[Quincy_Orchidee_PFT.TrH] = frac_C4

        elif (IGBP_str == 'EBF'):
            if (P_yearly_sum > min_tropical_precipitation) & (
                    T_monthly_min > min_tropical_temperature):
                self.PFT_list.Fractions[Quincy_Orchidee_PFT.TrBE] = 1.0
            elif (T_monthly_min > min_temperate_temperature_broadleaved):
                self.PFT_list.Fractions[Quincy_Orchidee_PFT.TeBE] = 1.0
            else:
                self.PFT_list.Fractions[Quincy_Orchidee_PFT.BBS] = 1.0  # ???

        elif (IGBP_str == 'DBF'):
            if (P_yearly_sum > min_tropical_precipitation) & (
                    T_monthly_min > min_tropical_temperature):
                self.PFT_list.Fractions[Quincy_Orchidee_PFT.TrBR] = 1.0
            elif (T_monthly_min > min_temperate_temperature_broadleaved):
                self.PFT_list.Fractions[Quincy_Orchidee_PFT.TeBS] = 1.0
            else:
                self.PFT_list.Fractions[Quincy_Orchidee_PFT.BBS] = 1.0  # ???

        elif (IGBP_str == 'ENF'):
            if (P_yearly_sum > min_tropical_precipitation) & (
                    T_monthly_min > min_tropical_temperature):
                self.PFT_list.Fractions[Quincy_Orchidee_PFT.TrBR] = 1.0  # Should not happen
            elif (T_monthly_min > min_temperate_temperature_needleleaved):
                self.PFT_list.Fractions[Quincy_Orchidee_PFT.TeNE] = 1.0
            else:
                self.PFT_list.Fractions[Quincy_Orchidee_PFT.BNE] = 1.0  # ???

        else:
            if (P_yearly_sum > min_tropical_precipitation) & (
                    T_monthly_min > min_tropical_temperature):
                self.PFT_list.Fractions[Quincy_Orchidee_PFT.TrBE] = 1.0
            elif (T_monthly_min > min_temperate_temperature_needleleaved):
                self.PFT_list.Fractions[Quincy_Orchidee_PFT.TeBE] = 1.0
            else:
                self.PFT_list.Fractions[Quincy_Orchidee_PFT.BBS] = 1.0  # ???



