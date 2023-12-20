import pandas as pd

from lib.converter.Quincy_fluxnet_2022_site_data_factory import Quincy_Site_Data_Factory
from lib.converter.Quincy_fluxnet_2022_site_setup_factory import Quincy_Site_Setup_Factory
from lib.converter.Quincy_fluxnet_2022_forcing import Quincy_Fluxnet_2022_Forcing

from lib.converter.Quincy_fluxnet_2022_site_data import Quincy_Site_Data
from lib.converter.Base_Parsing import Base_Parsing
from lib.converter.Settings import Settings
from lib.src.Fluxnet2022_Jake import Fluxnet2022_Jake

import os


class Quincy_Fluxnet22_Forcing(Base_Parsing):

    def __init__(self, settings: Settings, root_fluxnet_path, sites):

        Base_Parsing.__init__(self, settings = settings)
        self.root_fluxnet_path = root_fluxnet_path
        self.sites = sites

        self.df_error = pd.DataFrame(columns=["site", "message"])
        # Check if directory exists and create otherwise
        if not os.path.exists(self.settings.root_output_path):
            os.mkdir(self.settings.root_output_path)

    def parse(self):
        quincy_site_data_factory = Quincy_Site_Data_Factory(settings=self.settings)
        quincy_site_setup_factory = Quincy_Site_Setup_Factory(settings=self.settings)

        n = len(self.sites)
        print(f"Parsing {n} fluxnet sites.")


        current_site = 1
        for site in self.sites:

            try:
                print(f"Parsing site: {site} ({current_site} out of {n}).")
                print(f"Opening fluxnet site")
                fnet = Fluxnet2022_Jake(rtpath=self.root_fluxnet_path, sitename=site)
                self.dprint("Parsing Fluxnet time variable.. ", fnet.Read_And_Parse_Time)

                print("Creating Quincy fluxnet22 file:")
                qf = Quincy_Fluxnet_2022_Forcing(settings=self.settings)
                qf.Connect_to_fluxnet(fnet=fnet)
                qf.Parse_forcing()

                # Generate transient forcing based on the input data
                qf.Export_transient_forcing()
                # Generate static forcing based on the input data
                qf.Export_static_forcing()


                print("Reading Quincy site data")
                qsd = Quincy_Site_Data(fluxnet_file=fnet, settings=self.settings)
                qsd.Parse_Environmental_Data()
                qsd.Parse_PFT(qf=qf)
                qsd.Perform_sanity_checks()

                quincy_site_data_factory.Add_site(qsd=qsd)
                quincy_site_setup_factory.Add_site(qsd=qsd)
                print(f"Site {site} sucessfully parsed! ")


            except Exception as e:
                print(f"ERROR parsing site {site}.")
                print(e)
                self.df_error.loc[-1] = pd.Series({"site": site,  "message": e})
                self.df_error.index = self.df_error.index + 1
                self.df_error = self.df_error.sort_index()

            print("----------------------------------")
            print("")
            print("")
            current_site += 1

        print("Exporting site information")
        quincy_site_data_factory.Export()
        quincy_site_setup_factory.Export()
        self.df_error.to_csv(f"{self.settings.root_output_path}/Errors.csv", index=False)
        print("Parsing complete")








