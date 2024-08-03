import vnquant.plot as pl
import vnquant.data as dt
import pandas as pd
import json
from icecream import ic

# tickers = pd.read_csv("E:\VS_Workspace\\test_VN_stosk\\vnquant\VN30.csv")
# tickers = list(tickers.values.reshape(1,-1)[0])
tickers = ['BCM', 'BID', 'BVH', 'CTG', 'FPT', 'GAS'
        , 'GVR', 'HDB', 'HPG', 'MBB', 'MSN', 'MWG'
        , 'PLX', 'POW', 'SAB', 'SHB', 'SSB', 'SSI'
        , 'STB', 'TCB', 'TPB', 'VCB', 'VHM', 'VIB'
        , 'VIC', 'VJC', 'VNM', 'VPB', 'VRE']
loader = dt.DataLoader_json(symbols=tickers,
           start="2024-07-25",
           end="2024-07-27",
           minimal=False,
           data_source="cafe",
           table_style="prefix")

data = loader.download()
# print(data)
# for k, v in data.items():
#     # print("Key", k)
#     print("Value", v)
#     print()
# print("Check")
