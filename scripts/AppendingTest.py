import pandas as pd
import numpy as np



size = 2

i = 0
bpath = '/Net/Groups/BSI/work_scratch/ppapastefanou/FLUXNET_QUINCY_test/fluxnet_all_sites_1990-2021_list_2023-12-28.dat'
df = pd.read_csv(f"{bpath}{i}")
for i in range(1, size):
    df_r = pd.read_csv(f"{bpath}{i}")
    df = pd.concat([df, df_r])
df.to_csv(f"{bpath}", index =None)
