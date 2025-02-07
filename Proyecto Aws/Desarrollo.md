# Proyecto AWS
Para realizar el proyecto necesitamos extraer los datos con la API de Open-Meteor.
Open-Meteo colabora con los servicios meteorológicos nacionales para ofrecer datos abiertos de alta resolución, de 1 a 11 kilómetros, 
contando con 80 años de datos históricos y es de código abierto.

Extraeremos información de la API de Open-Meteor con los siguientes parámetros diarios:
- **Temperatura máxima** (temperature_2m_max)
- **Temperatura mínima** (temperature_2m_min)
- **Temperatura media** (temperature_2m_mean)
- **Precipitación total** (rain_sum)
- **Horas de precipitación** (precipitation_hours)
- **Velocidad máxima del viento** a 10m (wind_speed_10m_max)
- **Radiación solar acumulada** (shortwave_radiation_sum)
  
Este proceso lo realizamos en Google colab con el siguiente codigo (link)
Ahora procederemos a utilizar **AWS**
