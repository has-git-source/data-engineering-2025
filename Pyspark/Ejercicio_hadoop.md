# Ejercicio Hadoop

```bash
sudo ./start-container.sh
```
**Obteniendo**
```bash
start hadoop-master container...
start hadoop-slave1 container...
start hadoop-slave2 container...
```
**Iniciamos hadoop**

```bash
./start-hadoop.sh
```
**Agrego un archivo txt de Gutenberg**

Este link nos lleva al texto correspondiente a 20,000 leguas de viaje submarino de Julio Verne

```bash
wget https://www.gutenberg.org/cache/epub/164/pg164.txt
```
**Creamos el directorio y el archivo tipo tar.gz**

```bash
mkdir input
tar -czvf input/pg164.tar.gz pg164.txt
```
**Revisamos el tamaño de los archivos**

```bash
ls -flarts input
```
Obteniendo
```bash
total 248
  8 drwx------ 1 root root   4096 Feb  7 17:19 ..
  4 drwxr-xr-x 2 root root   4096 Feb  7 17:20 .
236 -rw-r--r-- 1 root root 240575 Feb  7 17:20 pg164.tar.gz
```
**Creamos y movemos nuestro directorio**

```bash
hdfs dfs -mkdir -p test
hdfs dfs -put input
```
**Revisamos el directorio**

```bash
hdfs dfs –ls  input
```
**Leemos las primeras lineas de nuestro archivo**

```bash
hdfs dfs -cat  input/pg164.tar.gz | zcat | tail -n 20
```
Esto es lo que obtenemos en la consola
![image](https://github.com/user-attachments/assets/18a65518-0137-4cef-abda-131a1ca0c435)

**Ejecutamos el trabajo**
```bash
hadoop jar $HADOOP_HOME/share/hadoop/mapreduce/sources/hadoop-mapreduce-examples-2.7.2-sources.jar org.apache.hadoop.examples.WordCount input output
```
Este comando: 
-Lee archivos de texto desde el directorio input en HDFS` 
- Divide el texto en palabras (Tokenization).
- Cuenta cuántas veces aparece cada palabra (MapReduce).
- Guarda el resultado en el directorio output en HDFS`.

**Observamos el resultado**

```bash
hdfs dfs -cat output/part-r-00000
```
Con el conteo de palabras

![image](https://github.com/user-attachments/assets/4481936d-cd6d-45bf-9c3e-348f35c210d1)

**Eliminamos el archivo**

```bash
hdfs dfs –rm –f /user/rawdata/example/pg164.tar.gz
```



