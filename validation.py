import pandas as pd
import glob
from datetime import datetime
import matplotlib.pyplot as plt
import numpy as np

radiance_df = pd.read_csv('id_dgp_ev.csv', usecols=[0, 2])
radiance_df[['view', 'year', 'month', 'day', 'time', 'T', 'M', 'D']] = pd.DataFrame(
    radiance_df['ID'].str.split('_').values.tolist())
radiance_df['time'] = pd.to_numeric(radiance_df['time'])
radiance_df['hour'] = radiance_df['time'].apply(int)
radiance_df['minute'] = ((radiance_df['time'] - radiance_df['hour']) * 60).round().apply(int)
radiance_df['hour'] = radiance_df['hour'].apply(str)
radiance_df['minute'] = radiance_df['minute'].apply(str)
radiance_df['Time'] = pd.to_datetime(
    radiance_df['year'] + '-' + radiance_df['month'] + '-' + radiance_df['day'] + ' ' + radiance_df['hour'] + ':' +
    radiance_df['minute'])
radiance_df = radiance_df[['Time', 'view', 'T', 'M', 'D', 'EV']]
radiance_df = radiance_df.sort_values(by=['Time'])
#radiance_df = radiance_df[~radiance_df['view'].str.contains('v6')]
radiance_df = radiance_df[~radiance_df['view'].str.contains('v8')]

dates = [20, 21, 22, 26, 28, 29, 1]
tints = [8905, 8906, 8905, 8909, 8909, 8909, 8905]
simulated_df = pd.DataFrame()
temp = []
for date, tint in zip(dates, tints):
    temp.append(radiance_df.loc[(radiance_df['Time'].dt.day == date) & (radiance_df['T'] == str(tint)) & (
                radiance_df['view'] != 'v5')])
simulated_df = pd.concat(temp, ignore_index=True)

sensor1_df= pd.read_csv('sensor1/20-03-2019_01-04-2019.csv',usecols=[1,2,3])
sensor1_df = sensor1_df.replace('Value', np.nan)
sensor1_df = sensor1_df.dropna()
sensor1_df['date']=pd.to_datetime(sensor1_df['Date']+' '+sensor1_df['Time'],format='%m/%d/%y %H:%M:%S')
sensor1_df=sensor1_df[['date','Value']]
#print(sensor1_df)

files_sensor2 = glob.glob('sensor2/*')
sensor2_dfs = [pd.read_csv(file, delimiter='\t', skiprows=[0, 1, 3], usecols=[1, 10], parse_dates={'date': [0]},
                           infer_datetime_format=True) for file in files_sensor2]
sensor2_df = pd.concat(sensor2_dfs, ignore_index=True)
sensor2_df = sensor2_df.sort_values(by=['date'])
sensor2_df['date'] = pd.to_datetime(sensor2_df['date'], format='%m/%d/%y %H:%M:%S')

# print(sensor2_df)

files_sensor3 = glob.glob('sensor3/*')
sensor3_dfs = [pd.read_csv(file, delimiter='\t', skiprows=[0, 1, 3], usecols=[1, 10], parse_dates={'date': [0]},
                           infer_datetime_format=True) for file in files_sensor3]
sensor3_df = pd.concat(sensor3_dfs, ignore_index=True)
sensor3_df = sensor3_df.sort_values(by=['date'])
sensor3_df['date'] = pd.to_datetime(sensor3_df['date'], format='%m/%d/%y %H:%M:%S')

# sensors=[sensor2_df,sensor3_df]
sensors = [sensor1_df,sensor2_df]
for [idx1, view], sensor in zip(simulated_df.groupby(simulated_df.view), sensors):
    for [sim_date, sim_df], (sens_date, sens_df), tint in zip(view.groupby(simulated_df.Time.dt.date),
                                                              sensor.groupby(sensor.date.dt.date), tints):
        sim_df = sim_df[['Time', 'EV']]

        sim_df = sim_df.set_index('Time')
        sens_df = sens_df.set_index('date')
        sim_df = sim_df.between_time('09:40:00','11:20:00')
        sens_df = sens_df.between_time('09:40:00', '11:20:00')
        sens_df=sens_df.astype(float)
        ax = sens_df.plot()
        sim_df.plot(legend=True, ax=ax)
        plt.ylabel('Illuminance (lux)')
        plt.xlabel(str(sim_date))
        plt.title(idx1 + ' with ' + str(tint) + ' on ' + str(sim_date))
        plt.savefig('output/' + idx1 + '_' + str(tint) + '_' + str(sim_date) + '.jpg', dpi=300)
        plt.show()
        plt.clf()
