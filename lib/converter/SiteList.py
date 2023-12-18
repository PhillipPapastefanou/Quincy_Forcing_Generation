import glob
import netCDF4
import pandas as pd

files = glob.glob("/Volumes/BSI/work_scratch/ppapastefanou/fluxnet2022/*_meteo.nc")


df = pd.DataFrame()

list = []

for file in files:
    ds = netCDF4.Dataset(file)
    print(ds.pft)
    list.append(ds.pft)
    ds.close()

df['PFT'] = list
df.to_csv("PFT_list.csv", index= False)