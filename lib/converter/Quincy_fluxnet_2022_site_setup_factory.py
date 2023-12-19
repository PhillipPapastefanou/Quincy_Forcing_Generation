import pandas as pd
import numpy  as np

from lib.converter.Settings import Settings
from lib.converter.Base_Parsing import Base_Parsing
from lib.converter.Quincy_fluxnet_2022_site_data import Quincy_Site_Data
from lib.src.PFT import Quincy_Orchidee_PFT
from datetime import date



class Quincy_Site_Setup_Factory(Base_Parsing):

    def __init__(self, settings :Settings):

        Base_Parsing.__init__(self, settings)

        self.columns = ['Site-ID','lon','lat','PFT','start','end',
                      'lon_gmt','clay','silt','sand','awc','bd','ph',
                      'taxusda','taxnwrb','lith_glim',
                      'LAI','Nleaf','SLA','Height','PlantYear',
                      'soilP_depth','soilP_labile','soilP_slow','soilP_occluded','soilP_primary',
                      'Qmax_org_fp']

        self.df = pd.DataFrame(columns = self.columns)


    def Add_site(self, qsd : Quincy_Site_Data):

        new_row = {'Site-ID': qsd.fnet.sitename,
                   'lon' : qsd.fnet.Lon,
                   'lat' : qsd.fnet.Lat,
                   'pft' : qsd.PFT_Quincy_str,
                   'start': qsd.fnet.Year_min,
                   'end': qsd.fnet.Year_max,
                   'lon_gmt': qsd.Gmt_ref,
                   'clay': qsd.Clay_fraction,
                   'silt': qsd.Silt_fraction,
                   'sand': qsd.Sand_fraction,
                   'awc' : qsd.AWC,
                   'bd' : qsd.Bulk_density_sg,
                   'ph': qsd.PH,
                   'taxusda': qsd.Taxousda,
                   'taxwrb': qsd.Taxnwrb,
                   'lith_glim': qsd.Glim_class,
                   'LAI': qsd.LAI,
                   'Nleaf': qsd.Nleaf,
                   'SLA': qsd.SLA,
                   'Height': qsd.Height,
                   'PlantYear': qsd.Plant_year,
                   'soilP_depth': qsd.P_soil_depth,
                   'soilP_labile': qsd.P_soil_labile,
                   'soilP_slow': qsd.P_soil_slow,
                   'soilP_occluded': qsd.P_soil_occlud,
                   'soilP_primary': qsd.P_soil_primary,
                   'Qax_org_fp': qsd.Q_max_org
                   }

        self.df.loc[len(self.df)] = new_row


    def Export(self):
        # GEt todat date string
        today = date.today()

        # Pass reference
        df_export = self.df

        # Round values to 4 significant figures
        for var in ["clay", "silt", "sand", "awc", "ph", "LAI"]:
            df_export[var] = df_export[var].astype(np.float64)
            df_export[var] = self.round(df_export[var], 4)
            df_export[var] = df_export[var].apply(pd.to_numeric, downcast='float').fillna(0)

        # Export file acccording to QUINCY standards
        outSiteFile = f"{self.settings.root_output_path}/fluxnet_all_sites_1990-2022_list_{today}.dat"
        df_export.to_csv(outSiteFile, header=True, sep=" ", index=None)

        outSiteFile = f"{self.settings.root_output_path}/fluxnet_all_sites_1901-2022_list_{today}.dat"
        df_export.to_csv(outSiteFile, header=True, sep=" ", index=None)

