# Proyecto AWS
## Requests API

Para realizar el proyecto necesitamos extraer los datos con la API de Open-Meteor.
Open-Meteo colabora con los servicios meteorológicos nacionales para ofrecer datos abiertos de alta resolución, de 1 a 11 kilómetros, 
contando con 80 años de datos históricos y es de código abierto.

Extraeremos información de la API de Open-Meteor con los siguientes parámetros diarios:
- **Temperatura máxima** (temperature_2m_max)
- **Temperatura mínima** (temperature_2m_min)
- **Precipitación total** (rain_sum)
- **Horas de precipitación** (precipitation_hours)
- **Velocidad máxima del viento** a 10m (wind_speed_10m_max)
- **Radiación solar acumulada** (shortwave_radiation_sum)
  
Este proceso lo realizamos en Google colab con el codigo que aparece en API_requests.ipynb.

***

## **AWS**

### S3

> [!NOTE]
> Crearemos dos buckets en S3. 
    
El primero **clima-proyecto* donde subiremos nuestro archivo **datos_clima.json* en la carpeta raw/,  y tendremos otra carpeta transform/, donde guardaremos el archivo que nos genere la funcion lambda.

![image](https://github.com/user-attachments/assets/dba901cb-ec5d-4f0a-98c5-18d2165f8b72)
![image](https://github.com/user-attachments/assets/ae789695-de50-4dd6-9ba5-e3d35a6ef8b6)

Y el otro bucket **transformados-clima-proyecto*.

![image](https://github.com/user-attachments/assets/c78968f8-bcab-4ad9-be43-f81393183734)


### Lambda

Con lambda procesaremos la información de nuestro archivo.
Primero crearemos nuestra funcion desde la consola. 

![image](https://github.com/user-attachments/assets/4d206c51-94d4-4642-9671-b24c40ee2a99)

> [!IMPORTANT]  
> Tenemos que asignarle los permisos necesarios a lambda y configurar el trigger, además de aumentar el tiempo de ejecucion.

Asignamos permisos de listado, obtener y poner objeto en la politica creada.

![image](https://github.com/user-attachments/assets/96941af3-34e3-4063-95bf-1aee5c741ab7)

![image](https://github.com/user-attachments/assets/5290223f-bec8-4233-8e73-b6f75af47c0c)

Configuramos el trigger para que el evento suceda solo cuando se suban archivos a esta carpeta.

![image](https://github.com/user-attachments/assets/212d4885-0491-4606-abc5-efb703476087)

El codigo de lambda fue el siguiente:

``` python
import json
import csv
import boto3
from io import StringIO

# Configurar el cliente de S3
s3 = boto3.client('s3')

# Definir nombres de bucket y archivos
BUCKET_NAME = "clima-proyecto"
RAW_PATH = "raw/datos_clima.json"
TRANSFORMED_PATH = "transform/datos_clima.csv"

def lambda_handler(event, context):
    try:
        # Descargar el archivo JSON desde S3
        response = s3.get_object(Bucket=BUCKET_NAME, Key=RAW_PATH)
        json_data = json.loads(response['Body'].read().decode('utf-8'))

        # Configurar encabezados con nuestors parametros
        headers = [
            "year", "date", 
            "precipitation_hours", "wind_speed_10m_max", "temperature_2m_max", 
            "temperature_2m_min", "rain_sum", "shortwave_radiation_sum"
        ]

        # Crear buffer para CSV
        csv_buffer = StringIO()
        csv_writer = csv.writer(csv_buffer)

        # Escribir encabezados
        csv_writer.writerow(headers)

        # Extraer datos del JSON y escribir filas en el CSV
        for year, data in json_data.items():
            for i, date in enumerate(data["daily"]["time"]):
                csv_writer.writerow([
                    year,
                    date,
                    data["daily"].get("precipitation_hours", [None])[i],
                    data["daily"].get("wind_speed_10m_max", [None])[i],
                    data["daily"].get("temperature_2m_max", [None])[i],
                    data["daily"].get("temperature_2m_min", [None])[i],
                    data["daily"].get("rain_sum", [None])[i],
                    data["daily"].get("shortwave_radiation_sum", [None])[i]
                ])

        #  Subir el CSV a S3 en la carpeta "transform/"
        s3.put_object(Bucket=BUCKET_NAME, Key=TRANSFORMED_PATH, Body=csv_buffer.getvalue())

        return {
            "statusCode": 200,
            "body": f"Archivo CSV guardado en s3://{BUCKET_NAME}/{TRANSFORMED_PATH}"
        }
    
    except Exception as e:
        return {
            "statusCode": 500,
            "body": f"Error: {str(e)}"
        }
```

Realice un test para asegurarme de que estuviera bien mi codigo.

![image](https://github.com/user-attachments/assets/88d68e21-01b4-4fa0-844e-46fd1b5f8816)

Agreggue el archivo a a carpeta raw/ para desencadenar el trigger.

En la carpteta transform/ ya obtenemos nuestro archivo en csv (el archivo **datos_clima.csv puede verse en los recursos).

![image](https://github.com/user-attachments/assets/f52220e8-5602-4625-ae81-33c94b3f8ce3)

### Glue Crawler
Con Glue Crawler catalogaremos los datos.

Crearemos nuestro crawler y le asignamos permisos.

![image](https://github.com/user-attachments/assets/c61362ef-7055-4855-8ca7-6c9f6781aa56)

Tambien crearemos una nueva base de datos.

![image](https://github.com/user-attachments/assets/4af7ae89-0a4d-4c3f-9548-cd36a0afcaa1)

Ya creado ejecutamos el crawler

![image](https://github.com/user-attachments/assets/9bf02663-76ce-4e82-92e4-63553f39bc99)

Revisamos el esquema de nuestra nueva tabla creada

![image](https://github.com/user-attachments/assets/c8e99aee-b33f-4672-a35f-92bf059b5f2e)

### Glue Job

Con Glue job transformaremos los datos en nuestro proceso ETL.







