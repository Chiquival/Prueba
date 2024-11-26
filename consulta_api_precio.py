import requests

# curl -X GET "https://api.esios.ree.es/indicators/59526" -H "Authorization: Token token=bb7299fe21b6c816db42a1d359f59a861854ac434d"

url = "https://api.esios.ree.es/indicators/1001"
token = "bb7299fe21b6c816db42a1d359f59a861854ac434d"

headers = {
    "Authorization": f"Bearer {token}",
    "Content-Type": "application/json"
}

response = requests.get(url,headers=headers)

if response.status_code == 200:
    print("¡Éxito! Aquí está la respuesta:")
    print(response.json())
    print(type(response))
    print(type(response.json()))
    #print(response.text)
    print(response.status_code)
    #print(response)
else:
    #print("Error: {response.status.code}")
    #print(response.status_code)
    print(f"Error en la solicitud. Código de estado: {response.status_code}")
    print(f"Detalle del error: {response.text}")
    print(type(response))