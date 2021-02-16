import pandas as pd
import numpy as np
from shutil import move
import os
import glob
from os import path
from influxdb import DataFrameClient
from datetime import date

# Parameters:

timezone = 'CET'
measurements = 'all'
processed_logs = 'CTC_processed_logs'

# Influxdb parameters:

host = 'localhost'
port = 8086
user = 'root'
password = 'root'
db_main = 'ctc'
db_alarmbank = 'ctc_alarms'


def main(_tz=timezone, _measurements=measurements, _host=host, _port=port, _user=user, _pwd=password, _db=db_main,
         _dba=db_alarmbank):
    """
    Main function to be run
    :return: prints success message
    """
    
    files = glob.glob("*.CSV")

    df_main = pd.DataFrame()
    df_alarms = pd.DataFrame()

    for file in files:
        parse(file)
        if file.startswith("AD"):
            df_alarms = df_alarms.append(read_csv(file, _tz, rename_columns=False), sort=False)
        else:
            df_main = df_main.append(read_csv(file, _tz), sort=False)

    df_main.sort_index(inplace=True)
    df_alarms.sort_index(inplace=True)
    
    # Push main csv files to influxDB Database db_main
    if not df_main.empty:
        src = 'ctc_csv'
        influx(df_main,src,_measurements,_host, _port, _user, _pwd, _db)

    # Push alarm bank csv files to influxDB Database db_alarmbank
    if not df_alarms.empty:
        src = 'ctc_alarms'
        influx(df_alarms,src,_measurements,_host, _port, _user, _pwd, _dba)

    archive(files, processed_logs)

    return print(len(files), 'csv file(s) have been parsed and pushed to the influxDB database', _db)
        
def read_csv(file, tz, rename_columns=True):
    """
    Reads the file into a pandas dataframe, cleans data and rename columns
    :param file: file to be read
    :param tz: timezone
    :param rename_columns: rename some duplicate columns
    :return: pandas dataframe
    """
    df = pd.read_csv(file, parse_dates=True)
    
    df['DateTime'] = pd.to_datetime(df['DateTime'])
    df.set_index('DateTime', drop = False, inplace = True)
    df.index = df.index.tz_localize(tz, ambiguous='NaT')
    df = df.astype({'TankUpperTemp': 'int', 'TankLowerTemp': 'int', 'HeatWater1Temp': 'int', 'Return temp': 'int', 'Unnamed: 42': 'float'})
    
    df = df.loc[df.index.notnull()]
    df = df.loc[~df.index.duplicated(keep='first')]
    if rename_columns:
        df['CompState'] = np.where(df['CompState'] == 'ON', 1, 0)
        df.rename(columns={df.columns[25]: "Outdoor temp2" }, inplace = True)
    return df

def parse(file):
    """
    Removes first line of log if it's useless
    :param file: log file
    :return: log file
    """
    with open(file, 'r') as fin:
        lines = fin.read().splitlines(True)
        if not lines[0].startswith("DateTime"):    
            with open(file, 'w') as fout:
                fout.writelines(lines[1:])

def archive(files, directory):
    """
    Send list of files to a directory
    :param files: list of files to send
    :param directory: directory where to send files
    """
    if not os.path.exists(directory):
            os.mkdir(directory)
    for file in files:
        move(file, os.path.join(directory, file))

def influx(DataFrame, source, measurements, host, port, user, password, dbname):
    """
    Instantiates influxdb and writes the dataframe to the database
    :param measurements: "all" or "custom"
    :param DataFrame: DataFrame to be written to db
    :param host: optional if other than localhost
    :param port: optional if other than 8086
    :return: Name of database that has been written to
    """
    today = str(date.today())

    client = DataFrameClient(host, port, user, password, dbname)
    
    # Custom measurements
    measurements_custom = {'temperature': [
                                    'Outdoor temp',
                                    'Outdoor temp2',
                                    'TankUpperTemp',
                                    'TankLowerTemp',
                                    'RoomTemperature1',],
                            'electric power': [
                                        'ElBoilerUsedPwr'],
                            'electric current': [
                                        'CurrentL1',
                                        'CurrentL2',
                                        'CurrentL3',
                                        'SoftStartCurrent'],
                            'compressor info': [
                                       'Compressor Max rps',
                                       'Compressor SetpTemp',
                                       'Compressor SetpRps',
                                       'CompressorSpeed',
                                       'CompState',],
                            'alarms info': [
                                       'Alarm1',
                                       'Alarm2',
                                       'Alarm3',
                                       'Alarm4',
                                       'Alarms',],
                            'misc': [
                                       'ShuntSwitch',
                                       'ETime',
                                       'LoopCount',
                                       'WorkMode',
                                       'HeatPumpChargePump1']}
    
    # All measurements in csv file (excluding first column)
    measurements_all = {'measurements': DataFrame.columns.tolist()[1:]}
    
    if measurements == "all":
        for x,y in measurements_all.items():
            client.write_points(DataFrame.filter(y, axis=1), x, {'source': source, 'date_read': today}, protocol='line')
    elif measurements == "custom":
        for x,y in measurements_custom.items():
            client.write_points(DataFrame.filter(y, axis=1), x, {'source': source, 'date_read': today}, protocol='line')

if __name__ == '__main__':
    main()