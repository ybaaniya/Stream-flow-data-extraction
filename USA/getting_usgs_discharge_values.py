
import pandas as pd
import datetime as dt

stations = pd.read_csv('712_gauge_stations.csv')

codes = stations['SOURCE_FEA'].to_list()
names = stations['STATION_NM'].to_list()
sites = stations['FEATUREDET'].to_list()

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

stations_to_delete = []
url_to_review = []

for code, name, site in zip(codes, names, sites):

    print(code, ' - ', name)

    url = 'https://waterdata.usgs.gov/nwis/dv?cb_00060=on&format=rdb&{0}&legacy=&referred_module=sw&period=&begin_date=1838-01-01&end_date={1}-{2}-{3}'.format(site,anio,mes,dia)
    print(url)

    skip = 20

    while True:
        #print(skip)
        try:
            data = pd.read_table(url, delimiter='\t', skiprows=skip)
            data.drop([0], inplace=True)
            break
        except:
            skip = skip + 1
            if skip == 100:
                stations_to_delete.append(code)
                url_to_review.append(url)
                break
            continue
    #print(data)

    try:
        data.set_index('datetime', inplace=True)
        data.index = pd.to_datetime(data.index)
        data.index = data.index.to_series().dt.strftime("%Y-%m-%d")
        data.index = pd.to_datetime(data.index)
        data = data[[col for col in data.columns if '_00060_00003' in col]]
        data = data[[col for col in data.columns if '_cd' not in col]]
        data.rename(columns={data.columns[0]: 'Streamflow (m3/s)'}, inplace=True)
        data.rename(index={'datetime': 'Datetime'}, inplace=True)
        data['Streamflow (m3/s)'] = pd.to_numeric(data['Streamflow (m3/s)'], errors='coerce')
        data['Streamflow (m3/s)'] *= 0.0283168
        #print(data)

        data.to_csv('C:\\Users\\drojasle\\Documents\\Github\\National_Water_Model\\Observed_Data\\{}_Q.csv'.format(code))

    except Exception as e:
        print(e)
        print(stations_to_delete)
        print(url_to_review)

print("All the data has been downloaded!!")
print(stations_to_delete)
print(url_to_review)