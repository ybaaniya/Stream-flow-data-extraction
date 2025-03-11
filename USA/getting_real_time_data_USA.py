import io
import requests
import pandas as pd
import datetime as dt

import warnings
warnings.filterwarnings("ignore")

stations_USA = pd.read_csv('C:\\Users\\drojasle\\Box\\Post_Doc\\GEOGLOWS_Applications\\Parameter_Estimation\\Real_Time_Data\\USA_Total_Stations_Q.csv')
stations_USA = stations_USA.loc[stations_USA['Data_Source'] == 'USA']

codes = stations_USA['ID'].to_list()
names = stations_USA['Station'].to_list()

hoy = dt.date.today()

dia = hoy.day
if dia < 10:
    dia = '0{}'.format(str(dia))
else:
    dia = str(dia)

mes = hoy.month
if mes < 10:
    mes = '0{}'.format(str(mes))
else:
    mes = str(mes)

anio = hoy.year
anio = str(anio)

ini_date = hoy - dt.timedelta(days=120)

ini_dia = ini_date.day
if ini_dia < 10:
    ini_dia = '0{}'.format(str(ini_dia))
else:
    ini_dia = str(ini_dia)

ini_mes = ini_date.month
if ini_mes < 10:
    ini_mes = '0{}'.format(str(ini_mes))
else:
    ini_mes = str(ini_mes)

ini_anio = ini_date.year
ini_anio = str(ini_anio)

ini_dia = '31'
ini_mes = '12'
ini_anio = '2012'

stations_to_delete = []
url_to_review = []

for code, name in zip(codes, names):

    print(code, ' - ', name)

    if code < 10000000:
        site = '0' + str(code)
    else:
        site = str(code)

    #url = 'https://waterdata.usgs.gov/nwis/uv/?cb_00060=on&format=rdb&site_no={0}&legacy=1&period=&begin_date={1}-{2}-{3}&end_date={4}-{5}-{6}'.format(site, ini_anio, ini_mes, ini_dia, anio,mes,dia)
    url = 'https://nwis.waterdata.usgs.gov/usa/nwis/uv/?cb_00060=on&format=rdb&site_no={0}&legacy=1&period=&begin_date={1}-{2}-{3}&end_date={4}-{5}-{6}'.format(site, ini_anio, ini_mes, ini_dia, anio, mes, dia)
    print(url)

    response = requests.get(url, timeout=300)
    response.raise_for_status()
    data_content = response.text

    skip = 20

    while True:
        # print(skip)
        try:
            #data = pd.read_table(url, delimiter='\t', skiprows=skip)
            data = pd.read_table(io.StringIO(data_content), delimiter='\t', skiprows=skip)
            #print(data)
            data.drop([0], inplace=True)
            break
        except:
            skip = skip + 1
            if skip == 100:
                stations_to_delete.append(code)
                url_to_review.append(url)
                break
            continue

    try:
        data.set_index('datetime', inplace=True)
        data.index = pd.to_datetime(data.index)
        data.index = data.index.to_series().dt.strftime("%Y-%m-%d %H:%M:%S")
        data.index = pd.to_datetime(data.index)
        data = data[[col for col in data.columns if '_00060' in col]]
        data = data[[col for col in data.columns if '_cd' not in col]]
        data.rename(columns={data.columns[0]: 'Streamflow (m3/s)'}, inplace=True)
        data.rename(index={'datetime': 'Datetime'}, inplace=True)
        data['Streamflow (m3/s)'] = pd.to_numeric(data['Streamflow (m3/s)'], errors='coerce')
        data['Streamflow (m3/s)'] *= 0.0283168
        #print(data)
        data.to_csv('C:\\Users\\drojasle\\Box\\Post_Doc\\GEOGLOWS_Applications\\Parameter_Estimation\\Real_Time_Data\\USA\\{}_Q.csv'.format(code))
    except Exception as e:
        print(e)
        print(stations_to_delete)
        #print(url_to_review)

print("All the data has been downloaded!!")
print(stations_to_delete)
print(url_to_review)