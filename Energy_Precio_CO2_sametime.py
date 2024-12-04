#Consultas API
#Consulta precio
#comentario conflicto

import requests
import pandas as pd

url_precio = "https://api.esios.ree.es/indicators/1001"
url_co2 = "https://api.esios.ree.es/indicators/10355"
token_ree="c4d1043847fd7147001bc04669ae9e3b1b5552b536e17e647436f194a63bd67e"
#token_ree = "bb7299fe21b6c816db42a1d359f59a861854ac434d"

headers = {
    'Accept': 'application/json; application/vnd.esios-api-v1+json',
    'Content-Type': 'application/json',
    'x-api-key': token_ree
}

params = {
    "start_date": "2024-11-26T09:00:00+00:00",  # Asegúrate de que el formato sea correcto
    "end_date": "2024-11-26T13:00:00+00:00",    # Asegúrate de que el formato sea correcto
    "date_type": "datos"
}
response_precio = requests.get(url_precio,headers=headers,params=params)
response_co2= requests.get(url_co2, headers=headers,params=params)

if response_precio.status_code == 200:
    print("¡Éxito! La API del precio se ha leído correctamente")
    #print(response_precio.json())
    
else:
    print("Error en lectura Precio")
    print(f"Error en la solicitud. Código de estado: {response_precio.status_code}")
    print(f"Detalle del error: {response_precio.text}")
    print(type(response_precio))

if response_co2.status_code == 200:
    print("¡Éxito! La API del CO2 se ha leído correctamente")
    #print(response_co2.json())
    
else:
    print("Error en lectura CO2")
    print(f"Error en la solicitud. Código de estado: {response_co2.status_code}")
    print(f"Detalle del error: {response_co2.text}")
    print(type(response_co2))

#Influxdb

from influxdb_client import InfluxDBClient, Point, WriteOptions


url_influx = "http://localhost:8086" 
token_influx = "ZGnlb2d0DZbN6xgKH71nLDwrc3g-7z4y3CDS1d-gR6abZGhBl16xhz8dK0qcTG_0YNC6lHfk1UCLS4SbzjrtWw=="
org = "NOKIA_TECSS"
bucket = "Energy_Precio_CO2_TIME2"
client = InfluxDBClient (url=url_influx, token=token_influx, org=org)

#API

write_api = client.write_api()

data_precio= response_precio.json()
data_co2 = response_co2.json()
df = pd.read_csv('energy_filtered.csv')

influx_data_precio = []
influx_data_co2 = []
influx_data_energy = []

for value_data_precio in data_precio["indicator"]["values"]:
    value = value_data_precio["value"]
    datetime = value_data_precio["datetime"]
    datetime_utc = value_data_precio["datetime_utc"]  # Usar el timestamp en UTC
    tz_time = value_data_precio["tz_time"] 
    geo_id = value_data_precio["geo_id"]
    geo_name = value_data_precio["geo_name"]


    #timestamp = datetime.strptime(datetime_utc, "%Y-%m-%dT%H:%M:%SZ")
    #point = f"geodata,geo_id={geo_id},geo_name={geo_name} value={value} {datetime_utc}"
    point = Point("precio") \
        .tag("geo_id",geo_id) \
        .tag("geo_name",geo_name) \
        .field("value", value) \
        .time(datetime_utc)

    influx_data_precio.append(point)

for value_data_co2 in data_co2["indicator"]["values"]:
    value = value_data_co2["value"]
    datetime = value_data_co2["datetime"]
    datetime_utc = value_data_co2["datetime_utc"]  # Usar el timestamp en UTC
    tz_time = value_data_co2["tz_time"] 
    geo_id = value_data_co2["geo_id"]
    geo_name = value_data_co2["geo_name"]


    #timestamp = datetime.strptime(datetime_utc, "%Y-%m-%dT%H:%M:%SZ")
    #point = f"geodata,geo_id={geo_id},geo_name={geo_name} value={value} {datetime_utc}"
    point = Point("co2") \
        .tag("geo_id",geo_id) \
        .tag("geo_name",geo_name) \
        .field("value", value) \
        .time(datetime_utc)

    influx_data_co2.append(point)

energia = 0

for index, row in df.iterrows():
    device_id= row["device_id"]
    device_id_1= row["device_id_1"]
    location = row["location"]
    active_power= row["active_power"]
    apparent_power = row["apparent_power"]
    voltage = row["voltage"]
    current = row["current"]
    reactive_power = row["reactive_power"]
    freq = row["freq"]
    power_factor = row["power_factor"]
    rate1_active_energy = row["rate1_active_energy"]
    rate1_reactive_energy = row["rate1_reactive_energy"]
    rate2_active_energy = row["rate2_active_energy"]
    rate2_reactive_energy = row["rate2_reactive_energy"]
    total_active_energy = row["total_active_energy"]
    total_reactive_energy = row["total_reactive_energy"]

    
    time = row["time"]
   

    current_time = row["time"]
    if index < len(df)-1:
        next_time =df.iloc[index+1]["time"]
        delta_time= next_time - current_time
    
    energia = energia + (reactive_power)*delta_time

    point = Point("energy")\
        .tag("device_id",device_id)\
        .tag("device_id_1",device_id_1)\
        .tag("location", location)\
        .field("active_power",active_power)\
        .field("apparent_power", apparent_power)\
        .field("voltage",voltage)\
        .field("current",current)\
        .field("reactive_power",reactive_power)\
        .field("freq",freq)\
        .field("power_factor",power_factor)\
        .field("rate1_active_energy",rate1_active_energy)\
        .field("rate1_reactive_energy",rate1_reactive_energy)\
        .field("rate2_active_energy",rate2_active_energy)\
        .field("rate2_reactive_energy",rate2_reactive_energy)\
        .field("total_active_energy",total_active_energy)\
        .field("total_reactive_energy",total_reactive_energy)\
        .time(time)
    
    influx_data_energy.append(point)

print(energia)

write_api.write(bucket=bucket, org=org, record=influx_data_precio)
write_api.write(bucket=bucket, org=org, record=influx_data_co2)
write_api.write(bucket=bucket, org=org, record=influx_data_energy)
write_api.__del__()
client.close()