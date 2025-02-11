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

Con un cambio de esquema

![image](https://github.com/user-attachments/assets/28f1d6c3-157b-49c6-b23c-d46fd1fce6bd)

También eliminamos valores nulos y duplicados

![image](https://github.com/user-attachments/assets/4657771a-b7c1-4286-8b52-7bf9f40fc03b)


Ejecutamos nuestro job

![image](https://github.com/user-attachments/assets/bf47c6ae-1c24-4126-9625-0d244523b0bc)

Finalmente obtenemos nuestro archivo

![image](https://github.com/user-attachments/assets/d0dc43bb-c21b-47fe-b401-9394d2f20b5c)

Este es el script del job
```python
import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
import gs_uuid
from awsglue.gluetypes import *
from awsgluedq.transforms import EvaluateDataQuality
from awsglue.dynamicframe import DynamicFrame
from awsglue import DynamicFrame
from pyspark.sql import functions as SqlFuncs

def _find_null_fields(ctx, schema, path, output, nullStringSet, nullIntegerSet, frame):
    if isinstance(schema, StructType):
        for field in schema:
            new_path = path + "." if path != "" else path
            output = _find_null_fields(ctx, field.dataType, new_path + field.name, output, nullStringSet, nullIntegerSet, frame)
    elif isinstance(schema, ArrayType):
        if isinstance(schema.elementType, StructType):
            output = _find_null_fields(ctx, schema.elementType, path, output, nullStringSet, nullIntegerSet, frame)
    elif isinstance(schema, NullType):
        output.append(path)
    else:
        x, distinct_set = frame.toDF(), set()
        for i in x.select(path).distinct().collect():
            distinct_ = i[path.split('.')[-1]]
            if isinstance(distinct_, list):
                distinct_set |= set([item.strip() if isinstance(item, str) else item for item in distinct_])
            elif isinstance(distinct_, str) :
                distinct_set.add(distinct_.strip())
            else:
                distinct_set.add(distinct_)
        if isinstance(schema, StringType):
            if distinct_set.issubset(nullStringSet):
                output.append(path)
        elif isinstance(schema, IntegerType) or isinstance(schema, LongType) or isinstance(schema, DoubleType):
            if distinct_set.issubset(nullIntegerSet):
                output.append(path)
    return output

def drop_nulls(glueContext, frame, nullStringSet, nullIntegerSet, transformation_ctx) -> DynamicFrame:
    nullColumns = _find_null_fields(frame.glue_ctx, frame.schema(), "", [], nullStringSet, nullIntegerSet, frame)
    return DropFields.apply(frame=frame, paths=nullColumns, transformation_ctx=transformation_ctx)

args = getResolvedOptions(sys.argv, ['JOB_NAME'])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# Default ruleset used by all target nodes with data quality enabled
DEFAULT_DATA_QUALITY_RULESET = """
    Rules = [
        ColumnCount > 0
    ]
"""

# Script generated for node Amazon S3
AmazonS3_node1738957950633 = glueContext.create_dynamic_frame.from_options(
    format_options={"quoteChar": "\"", "withHeader": True, "separator": ",", "optimizePerformance": False}, 
    connection_type="s3", 
    format="csv", 
    connection_options={"paths": ["s3://clima-proyecto/transform/datos_clima.csv"], "recurse": True}, 
    transformation_ctx="AmazonS3_node1738957950633"
)

# Script generated for node UUID
UUID_node1738958152033 = AmazonS3_node1738957950633.gs_uuid(colName="id")

# Script generated for node Change Schema
ChangeSchema_node1738958059224 = ApplyMapping.apply(
    frame=UUID_node1738958152033, 
    mappings=[
        ("year", "string", "year", "string"), 
        ("date", "string", "date", "date"), 
        ("precipitation_hours", "string", "precipitacion_hora", "float"), 
        ("wind_speed_10m_max", "string", "velocidad_viento", "float"), 
        ("temperature_2m_max", "string", "temperatura_max", "float"), 
        ("temperature_2m_min", "string", "temperatura_min", "float"), 
        ("rain_sum", "string", "lluvia_total", "float"), 
        ("shortwave_radiation_sum", "string", "radiacion_solar", "float")
    ], 
    transformation_ctx="ChangeSchema_node1738958059224"
)

# Script generated for node Drop Duplicates
DropDuplicates_node1738957989425 =  DynamicFrame.fromDF(
    ChangeSchema_node1738958059224.toDF().dropDuplicates(), 
    glueContext, 
    "DropDuplicates_node1738957989425"
)

# Script generated for node Drop Null Fields
DropNullFields_node1738958107033 = drop_nulls(
    glueContext, 
    frame=DropDuplicates_node1738957989425, 
    nullStringSet={}, 
    nullIntegerSet={}, 
    transformation_ctx="DropNullFields_node1738958107033"
)

# Evaluación de calidad de datos
EvaluateDataQuality().process_rows(
    frame=DropNullFields_node1738958107033, 
    ruleset=DEFAULT_DATA_QUALITY_RULESET, 
    publishing_options={
        "dataQualityEvaluationContext": "EvaluateDataQuality_node1738957943844", 
        "enableDataQualityResultsPublishing": True
    }, 
    additional_options={"dataQualityResultsPublishing.strategy": "BEST_EFFORT", "observations.scope": "ALL"}
)

# Convertir a DataFrame, aplicar coalesce(1) y volver a DynamicFrame
df = DropNullFields_node1738958107033.toDF().coalesce(1)
DropNullFields_node1738958107033 = DynamicFrame.fromDF(df, glueContext, "DropNullFields_node1738958107033")

# Escritura en S3 con un solo archivo
AmazonS3_node1738960071504 = glueContext.write_dynamic_frame.from_options(
    frame=DropNullFields_node1738958107033, 
    connection_type="s3", 
    format="csv", 
    connection_options={"path": "s3://transformados-clima-proyecto", "partitionKeys": []}, 
    transformation_ctx="AmazonS3_node1738960071504"
)

job.commit()
```
***
## Análisis de datos e implementacion de modelos.

Realizaremos el análisis de los datos, a continuacion las distribuciones de las variables seleccionadas.

![image](https://github.com/user-attachments/assets/1cc9afe6-2e5c-4e02-aa10-a364d9cbd06e)

Serie de tiempo de temperatura máxima

![image](https://github.com/user-attachments/assets/593d7750-ec34-41da-a722-05f6dc774297)


Serie de tiempo de temperatura miníma

![image](https://github.com/user-attachments/assets/3dd9c941-89e9-47ea-9a5d-25fadb7f952e)


Matriz de correlación
Podemos observar que la correalción mas alta se da entre temperatura máxima y radiacion solar, seguido de lluvia por hora y precipitación total. 

![image](https://github.com/user-attachments/assets/883c60aa-f6a7-4881-b5a2-81eb7f5ce339)


Para los modelos utilizamos tres siendo: Random Forest, Regresión Logistica y Random Forest Regressor.
Utilizando el selector de las 3 características mas relevantes con f_regresion:
```python
# Definir el número de características que queremos retener
k = 3  # Por ejemplo, seleccionaremos las 3 características más relevantes

# Crear el selector de características usando f_regression
selector = SelectKBest(score_func=f_regression, k=k)

# Ajustar el selector a los datos
X_new = selector.fit_transform(X, y)

# Obtener los nombres de las características seleccionadas
selected_features = X.columns[selector.get_support()]
print("Características seleccionadas por SelectKBest:")
print(selected_features.tolist())
````
Obtenemos:

```python
Características seleccionadas por SelectKBest:
['precipitacion_hora', 'temperatura_max', 'velocidad_viento']
```
Con mutual_info obtenemos los mismos resultados

Para los primeros dos modelos, nuestra variable objetivo tiene que ser una variable categorica, en este caso **"radiacion_solar"*.
En la carpeta de recursos esta el archivo mas detallado con los pasos para cada modelo aqui solo pondre los resultados.

### Random Forest
Con las siguientes variables relizamos el modelo.
```python
predictoras_rf = ["precipitacion_hora", "velocidad_viento", "temperatura_max"]
objetivo_rf = "radiacion_solar_cat"
```
Teniendo las siguientes metricas:

```python
Accuracy: 0.6788154897494305
Precision (macro): 0.6742429544606195
Recall (macro): 0.5377115455982756
F1-score (macro): 0.5807609098944749
```
No tenemos un buen rendimiento del modelo nuestras metricas estan en 67%.
Podemos observar la matriz de confusión para ver en que variables uvo un mejor rendimiento:

![image](https://github.com/user-attachments/assets/6b5b58e0-dfa9-4d98-a4ac-97554f66df90)


## Regresión Logistica

Metricas:

```python
=== Regresión Logística ===
Accuracy:  0.6651
Precision: 0.6360
Recall:    0.6651
F1 Score:  0.6204

Classification Report:
              precision    recall  f1-score   support

        Alta       0.65      0.89      0.75       238
        Baja       0.00      0.00      0.00         9
       Media       0.50      0.13      0.21        76
    Muy Alta       0.74      0.60      0.66       116

    accuracy                           0.67       439
   macro avg       0.47      0.41      0.41       439
weighted avg       0.64      0.67      0.62       439
```

Al igual que el anterior su rendimiento se queda en 66%

![image](https://github.com/user-attachments/assets/329d974a-c647-4f4b-bf04-881240eaef25)


> [!IMPORTANT]
> Nuestros dos primeros modelos no tuvieron el rendimiento que esperabamos podemos cambiar la variable objetivo esperando mejores resultados.

## Random Forest Reggressor

Las metricas para este modelo son distintas, ya que en este tipo de modelos no estamos haciemndo una clasificacion ni tenemos la necesidad de designar una variable categorica podemos trabajar las variables numericas como tal.

```python
Mean Squared Error (MSE): 0.010401825034790643
Mean Absolute Error (MAE): 0.07920955407195589
R² Score: 0.6188635350761986
```
Este modelo tiene un rendimiento del 61.88% una leve mejora en comparacion de los modelos ateriores.

![image](https://github.com/user-attachments/assets/bbc559b4-ad7d-48c7-879d-5190cfb12675)



