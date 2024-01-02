import glob
import netCDF4
import pandas as pd
import os

files = glob.glob("/Net/Groups/BSI/work_scratch/ppapastefanou/fluxnet2022/*_meteo.nc")
df = pd.DataFrame()
list_pft = []

sitenames = []

files.sort()

for full_filename in files:

    filename = os.path.basename(full_filename)
    ds = netCDF4.Dataset(full_filename)


    sitename = filename.split('_')[0]

    print(f"{sitename} {ds.pft}")
    sitenames.append(sitename)
    list_pft.append(ds.pft)
    ds.close()


df['PFT'] = list_pft
df['Sitename'] = sitenames
df.to_csv("Sitenames_and_PFTs.csv", index= False)