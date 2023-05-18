"""
Análisis de las 5 estaciones más usadas y las 5 menos frecuentes en un mes, en
función de los días (de los días laborales o fin de semana). Sólo accederemos
a un fichero.

Apellidos1 : Ballester Navarro
Nombre1 : Teresa
    
Apellidos2 : González Morala
Nombre2 : David
"""
from pyspark.sql import SparkSession #Para crear una sesión de Spark
from collections import Counter #Para contar la frecuencia de los elementos de una lista
import json # Datos en formato JSON, pues los datos de BICIMAD vienen en ese formato
#from pprint import pprint # Para imprimir de manera legible los resultados
from datetime import datetime # Para poder trabajar con fechas 
import sys

spark = SparkSession.builder.getOrCreate() #Creamos la sesión
sc = spark.sparkContext # Entorno spark


def datos(linea): 
    """
    Función que recibe una línea de datos en formato JSON.

    """
    data = json.loads(linea) # Cargamos la línea como un diccionario en Python, para poder acceder sin problema a los datos
    estacion = data["idnunplug_station"]
    
    #Convertimos la cadena de fecha y hora en un objeto datetime
    fecha = datetime.strptime(data['unplug_hourTime']['$date'], "%Y-%m-%dT%H:%M:%S.%f%z") 
    dia = fecha.weekday() # Obtenemos el número de días de la semana: 0 - Lunes, 1 - Martes, etc.
    
    return (dia, estacion)
    

def no_fin_de(dia):
    
    if dia <= 4: # Dias laborables
        semana = True
    else: # Fin de semana
        semana = False
        
    return semana

def main(sc,filename):
    
    # Obtenemos del archivo dado la información en tuplas de: (dia de la semana, estacion) = x
    texto = sc.textFile(filename).map(datos) 
    
    # Calculamos la frecuencia de uso de las estaciones de bicicletas en función 
    # de los días laborales o los fines de semana. 
   # estaciones = texto.map(lambda x: ((x[0], x[1]), no_fin_de(x[0]))).groupByKey().mapValues(Counter)
    estaciones = Counter(texto.map(lambda x : ((x[0], x[1]), no_fin_de(x[0])))).collect()

    # Nos quedamos con las 5 estaciones más frecuentes
    # takeOrdered por defecto ordena de menor a mayor frecuencia (por eso -x[1])
    estaciones_frecuentes_semana = estaciones.map(lambda x: (x[0], x[1][True])).takeOrdered(5, lambda x: -x[1])
    estaciones_frecuentes_finde = estaciones.map(lambda x: (x[0], x[1][False])).takeOrdered(5, lambda x: -x[1])
    
    print(" 5 ESTACIONES MÁS FRECUENTES ENTRE SEMANA: ")
    print("~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~")
    for i in range(5):
        print(estaciones_frecuentes_semana[i][0])
        print("\n")
    print("\n")
    
    print(" 5 ESTACIONES MÁS FRECUENTES FIN DE SEMANAS:")
    print("~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~")
    for i in range(5):
        print(estaciones_frecuentes_finde[i][0]) #"veces: ", estaciones_frecuentes_finde[i][1])
        print("\n")
        
    # Nos quedamos con las estaciones menos frecuentes
    estaciones_NOfrecuentes_semana = estaciones.map(lambda x: (x[0], x[1][False])).takeOrdered(5, lambda x: x[1])
    estaciones_NOfrecuentes_finde  = estaciones.map(lambda x: (x[0], x[1][True])).takeOrdered(5, lambda x: x[1])

    print(" 5 ESTACIONES MENOS FRECUENTES ENTRE SEMANA:")
    print("~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~")
    for i in range(5):
        print(estaciones_NOfrecuentes_semana[i][0])
        print("")
    print("\n")
    
    print(" 5 ESTACIONES MENOS FRECUENTES FIN DE SEMANAS:")
    print("~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~")
    for i in range(5):
        print(estaciones_NOfrecuentes_finde[i][0])
        print("\n")

if __name__ == "__main__":
    
    if len(sys.argv) != 2:
        print("Uso: python3 {0} <file>".format(sys.argv[0]))
    else:
        filename = sys.argv[1].split(",")
        main(sc, filename)
