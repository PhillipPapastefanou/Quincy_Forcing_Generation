from lib.converter.Quincy_fluxnet_2022_site_data_factory import Quincy_Site_Data_Factory
from lib.converter.Quincy_fluxnet_2022_site_setup_factory import Quincy_Site_Setup_Factory
from lib.converter.Quincy_fluxnet_2022_forcing import Quincy_Fluxnet_2022_Forcing

from lib.converter.Quincy_fluxnet_2022_site_data import Quincy_Site_Data
from lib.converter.Base_Parsing import Base_Parsing
from lib.converter.Settings import Settings
from lib.src.Fluxnet2022_Jake import Fluxnet2022_Jake



class Quincy_Fluxnet22_Static_Forcing(Base_Parsing):

    def __init__(self, settings: Settings, root_fluxnet_path, sites):

        Base_Parsing.__init__(self, settings = settings)
        self.root_fluxnet_path = root_fluxnet_path
        self.sites = sites


    def parse(self):
        quincy_site_data_factory = Quincy_Site_Data_Factory(settings=self.settings)
        quincy_site_setup_factory = Quincy_Site_Setup_Factory(settings=self.settings)

        for site in self.sites:

            try:
                print(f"Parsing site: {site}...")
                print(f"Opening fluxnet site")
                fnet = Fluxnet2022_Jake(rtpath=self.root_fluxnet_path, sitename=site)
                self.dprint("Parsing Fluxnet time variable.. ", fnet.Read_And_Parse_Time)

                print("Creating Quincy fluxnet file:")
                qf = Quincy_Fluxnet_2022_Forcing(settings=self.settings)
                qf.Connect_to_fluxnet(fnet=fnet)
                qf.Parse_forcing()
                qf.Export()

                print("Reading Quincy site data")
                qsd = Quincy_Site_Data(fluxnet_file=fnet, settings=self.settings)
                qsd.Parse_Environmental_Data()
                qsd.Parse_PFT(qf=qf)
                qsd.Perform_sanity_checks()

                quincy_site_data_factory.Add_site(qsd=qsd)
                quincy_site_setup_factory.Add_site(qsd=qsd)
                print(f"Site {site} sucessfully parsed! ")
                print("----------------------------------")
                print("")
                print("")

            except:
                print(f"ERROR parsing site {site}.")


        quincy_site_data_factory.Export()
        quincy_site_setup_factory.Export()








