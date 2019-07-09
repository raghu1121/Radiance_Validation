import pandas as pd

#df=pd.read_csv('pred_kaiserslautern.csv',usecols=[5,11,13,14])
#df=pd.read_csv('pred_19_06_2018_kaiserslautern.csv',usecols=[11,13,14])
path='weather_stn/'
df=pd.read_csv(path+'pred_kaiserslautern_20_03_2019-01_04_2019.csv',usecols=[5,11,13,14])
# date_no=1
# tint=8905
dates=[20,21,22,26,28,29,1]
tints=[8905,8906,8905,8909,8909,8909,8905]

year=pd.DatetimeIndex(df['time']).year
month=pd.DatetimeIndex(df['time']).month
day=pd.DatetimeIndex(df['time']).day
hour=pd.DatetimeIndex(df['time']).hour+pd.DatetimeIndex(df['time']).minute / 60
df['month']=month
df['day']=day
df['hour']=hour
df['year']=year

#df=df[(df.Diffuse_Strahlung > 50) | (df.Direkt_Strahlung > 50)]
df=df[df.Globalstrahlung > 2]
temp=df
for date_no,tint in zip(dates,tints):
    df=df[df.day == date_no]
    df = df[df.hour < 12]
    df = df[df.hour > 9]
    df=df.round({'Direkt_Strahlung': 0, 'Diffuse_Strahlung': 0,'hour':2})
    df=df[['month','day','hour','Direkt_Strahlung','Diffuse_Strahlung','year']]
    df.to_csv(path+'kaiserslautern_'+str(date_no)+'_'+str(tint)+'.wea', sep=',', index=False)
    df=temp
#df.to_csv('kaiserslautern_19_06_2018.wea', sep=',', index=False)

