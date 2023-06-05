"""
ANÁLISIS BICIMAD
Teresa Ballester Navarro y David González Morala
"""

#from pyspark.sql import SparkSession #Para crear una sesión de Spark

from pyspark import SparkContext, SparkConf
from collections import Counter #Para contar la frecuencia de los elementos de una lista
import json # Datos en formato JSON, pues los datos de BICIMAD vienen en ese formato
#from pprint import pprint # Para imprimir de manera legible los resultados
import datetime # Para poder trabajar con fechas 
import sys
import statistics
#import pandas as pd
#import matplotlib.pyplot as plt
edades = ['0 a 11','12 a 17','18 a 24','25 a 44','45 a 64','+65']
nombres = ['niños', 'adolescentes', 'jovenes', 'jovenes_adultos', 'adultos', 'mayores']

def init():
    conf = SparkConf()\
        .setAppName('PracticaSpark')
    sc = SparkContext(conf = conf)
    return sc


def FiletoDic(line):
  data = json.loads(line)
  id_bici = data['_id']
  usuario = data['user_day_code']
  salida = data['idunplug_station']
  llegada = data['idplug_station']
  tiempo = data['travel_time']
  edad = data['ageRange']
  tipo = data['user_type']
  
  return usuario,id_bici,salida,llegada,tiempo,edad,tipo

def ciclos(data):
    usuarios_ciclo = data.filter(lambda x: x[2] == x[3])
    media_ciclo = usuarios_ciclo.map(lambda x : x[4]).mean()/60
    num_bicis_ciclo = usuarios_ciclo.map(lambda x: (x[0],x[1]))\
                                    .groupByKey()\
                                    .mapValues(lambda x: list(x)).count()
                                    
    total_usuarios_ciclo = usuarios_ciclo.count()
    return usuarios_ciclo, media_ciclo, num_bicis_ciclo, total_usuarios_ciclo
    
def es_camino(lista_bicis):
    return all(x == lista_bicis[0] for x in lista_bicis)

def caminos(data):
    usuarios_ciclo = data.filter(lambda x: x[2] == x[3])

    usuarios_camino = usuarios_ciclo.map(lambda x: (x[0],x[1]))\
                                    .groupByKey()\
                                    .filter(lambda x: es_camino(list(x[1])))\
                                    .mapValues(lambda x: list(x))
                                    
    id_usuarios_camino = usuarios_camino.map(lambda x: x[0]).collect()
    bici_usuarios_camino = usuarios_camino.map(lambda x: x[1][0]).collect()
    usuario_camino = data.filter(lambda x: (x[0] in id_usuarios_camino and x[1] in bici_usuarios_camino))
    num_usuario_camino = usuario_camino.count()
    media_camino = usuario_camino.map(lambda x : x[4]).mean()/60

    return media_camino, num_usuario_camino

def edades(data):

    usuarios = []
    for i in range(6):
        usuarios.append((data.filter(lambda x: x[5] == i)))
        
    medias = []
    for u in usuarios:
        medias.append(u.map(lambda x: (x[4])).mean()/60)

    usos = []
    for u in usuarios:
        usos.append(u.map(lambda x: (x[5],1)).reduceByKey(lambda x,y: x + y).take(1)[0][1])
        
    print(medias, usos)
    return medias, usos
"""
def estaciones(data):
    
    
    # Mapeo y conteo de estaciones
    estaciones_origen = data.map(lambda x: (x[2], 1))\
                               .reduceByKey(lambda x, y: x + y)
    
    # Ordenar por frecuencia
    estaciones_frecuentes = estaciones_origen.sortBy(lambda x: x[1], ascending = False)
    estaciones_NOfrecuentes = estaciones_origen.sortBy(lambda x: x[1], ascending = True)
    
    
    # Mapeo y conteo de estaciones
    estaciones_destino = rdd1.map(lambda x: (x[3], 1)) \
                               .reduceByKey(lambda x, y: x + y)
    
    # Ordenar por frecuencia
    estaciones_frecuentes_d = estaciones_destino.sortBy(lambda x: x[1], ascending = False)
    estaciones_NOfrecuentes_d = estaciones_destino.sortBy(lambda x: x[1], ascending = True)
    
    estaciones_ciclos = usuarios_ciclo.map(lambda x: (x[3], 1)) \
                           .reduceByKey(lambda x, y: x + y)
    
    estaciones_frecuentes_c = estaciones_ciclos.sortBy(lambda x: x[1], ascending = False)
    estaciones_NOfrecuentes_c = estaciones_ciclos.sortBy(lambda x: x[1], ascending = True)
    
        
    
    estaciones_caminos = usuario_camino.map(lambda x: (x[3], 1)) \
                               .reduceByKey(lambda x, y: x + y)
    
    estaciones_frecuentes_ca = estaciones_caminos.sortBy(lambda x: x[1], ascending = False)
    estaciones_NOfrecuentes_ca = estaciones_caminos.sortBy(lambda x: x[1], ascending = True)
    
    
    
    lista_rdds = [usuarios_niños, usuarios_adolescentes, usuarios_jovenes, usuarios_jovenes_adultos, usuarios_adultos, usuarios_mayores]
    
    for i in range(len(lista_rdds)):
        estaciones_niños = lista_rdds[i].map(lambda x: (x[3], 1)) \
                            .reduceByKey(lambda x, y: x + y)
    
        estaciones_frecuentes_n = estaciones_niños.sortBy(lambda x: x[1], ascending = False)
        estaciones_NOfrecuentes_n = estaciones_niños.sortBy(lambda x: x[1], ascending = True)
    
        
    
       
        return estaciones_frecuentes,estaciones_NOfrecuentes, estaciones_frecuentes_d,\
            estaciones_NOfrecuentes_d

"""
def main(sc, filename):
    rdd = sc.textFile(filename).map(FiletoDic)
    """
    data_ciclo = ciclos(rdd)
    print('--------ANÁLISIS DE CICLOS--------')
    print(f'El tiempo medio de los usuarios que realizan un ciclo es de {(f"{data_ciclo[1]:.2f}")} minutos')
    print(f'El número total de usuarios que realizan un ciclo es de {data_ciclo[3]} usuarios')                                                                        
    print(f'El número total de bicicletas usadas para realizar ciclos es {data_ciclo[2]}')
    
    
    data_camino = caminos(rdd)
    print('--------ANÁLISIS DE CAMINOS--------')
    print(f'El tiempo medio de los usuarios que realizan un camino es de {(f"{data_camino[0]:.2f}")} minutos')
    print(f'Dentro de los usuarios que realizan un ciclo hay {data_camino[1]} usuarios que realizan un camino')
    """
    data_edades = edades(rdd)
    print('--------ANÁLISIS DE EDADES--------')

    for i in range(len(data_edades)):
        print(f'El tiempo medio de uso en los {nombres[i]} es {(f"{(data_edades[0][i]):.2f}")} minutos')
        print(f'Los {nombres[i]}  han usado {data_edades[1][i]}  veces BICIMAD')
    """
    # Obtener las estaciones más frecuentes
    print("--------------- 5 ESTACIONES de SALIDA MÁS FRECUENTES ---------------")
    estaciones_top = estaciones_frecuentes.take(n) # Estaciones más frecuentes
    print(estaciones_top)
    print("--------------- 5 ESTACIONES de SALIDA MENOS FRECUENTES ---------------")
    estaciones_bottom = estaciones_NOfrecuentes.take(n) # Estaciones menos frecuentes 
    print(estaciones_bottom)
    
    # Obtener las estaciones más frecuentes 
    print("--------------- 5 ESTACIONES de LLEGADA MÁS FRECUENTES ---------------")
    estaciones_top_d = estaciones_frecuentes_d.take(n) # Estaciones más frecuentes
    print(estaciones_top_d)
    print("--------------- 5 ESTACIONES de LLEGADA MENOS FRECUENTES ---------------")
    estaciones_bottom_d = estaciones_NOfrecuentes_d.take(n) # Estaciones menos frecuentes 
    print(estaciones_bottom_d)
     
    print("--------------- 5 ESTACIONES para los CICLOS MÁS FRECUENTES ---------------")
    estaciones_top_c = estaciones_frecuentes_c.take(n) # Estaciones más frecuentes
    print(estaciones_top_c)
    print("--------------- 5 ESTACIONES para los CICLOS MENOS FRECUENTES ---------------")
    estaciones_bottom_c = estaciones_NOfrecuentes_c.take(n) # Estaciones menos frecuentes 
    print(estaciones_bottom_c)
    
    print("--------------- 5 ESTACIONES para los CAMINOS MÁS FRECUENTES ---------------")
    estaciones_top_ca = estaciones_frecuentes_ca.take(n) # Estaciones más frecuentes
    print(estaciones_top_ca)
    print("--------------- 5 ESTACIONES para los CAMINOS MENOS FRECUENTES ---------------")
    estaciones_bottom_ca = estaciones_NOfrecuentes_ca.take(n) # Estaciones menos frecuentes 
    print(estaciones_bottom_ca)
    
    print(f"--------------- 5 ESTACIONES MÁS FRECUENTES para {nombres[i]} ---------------")
    estaciones_top_n = estaciones_frecuentes_n.take(n) # Estaciones más frecuentes
    print(estaciones_top_n)
    print(f"--------------- 5 ESTACIONES MENOS FRECUENTES para {nombres[i]} ---------------")
    estaciones_bottom_n = estaciones_NOfrecuentes_n.take(n) # Estaciones menos frecuentes 
    print(estaciones_bottom_n)
    print('\n\n')
    """
   
     
"""     
usuarios_niños = data.filter(lambda x: x[5] == 0)
usuarios_adolescentes = data.filter(lambda x: x[5] == 1)
usuarios_jovenes = data.filter(lambda x: x[5] == 2)
usuarios_jovenes_adultos = data.filter(lambda x: x[5] == 3)
usuarios_adultos = data.filter(lambda x: x[5] == 4)
usuarios_mayores = data.filter(lambda x: x[5] == 5)
 
media_niños = usuarios_niños.map(lambda x: (x[4])).mean()/60
media_adolescentes = usuarios_adolescentes.map(lambda x: (x[4])).mean()/60
media_jovenes = usuarios_jovenes.map(lambda x: (x[4])).mean()/60
media_jovenes_adultos = usuarios_jovenes_adultos.map(lambda x: (x[4])).mean()/60
media_adultos = usuarios_adultos.map(lambda x: (x[4])).mean()/60
media_mayores = usuarios_mayores.map(lambda x: (x[4])).mean()/60
 
usos_niños = usuarios_niños.map(lambda x: (x[5],1)).reduceByKey(lambda x,y: x + y).take(1)[0][1]
usos_adolescentes = usuarios_adolescentes.map(lambda x: (x[5],1)).reduceByKey(lambda x,y: x + y).take(1)[0][1]
usos_jovenes = usuarios_jovenes.map(lambda x: (x[5],1)).reduceByKey(lambda x,y: x + y).take(1)[0][1]
usos_jovenes_adultos = usuarios_jovenes_adultos.map(lambda x: (x[5],1)).reduceByKey(lambda x,y: x + y).take(1)[0][1]
usos_adultos = usuarios_adultos.map(lambda x: (x[5],1)).reduceByKey(lambda x,y: x + y).take(1)[0][1]
usos_mayores = usuarios_mayores.map(lambda x: (x[5],1)).reduceByKey(lambda x,y: x + y).take(1)[0][1]
 
 print(f'El tiempo medio de uso en los niños es {(f"{(media_niños):.2f}")} minutos')
 print(f'El tiempo medio de uso en los adolescentes es {(f"{(media_adolescentes):.2f}")} minutos')
 print(f'El tiempo medio de uso en los jovenes es {f"{(media_jovenes):.2f}"} minutos')
 print(f'El tiempo medio de uso en los jovenes/adultos es {f"{(media_jovenes_adultos):.2f}"} minutos')
 print(f'El tiempo medio de uso en los adultos es {f"{(media_adultos):.2f}"} minutos')
 print(f'El tiempo medio de uso en los mayores es {f"{(media_mayores):.2f}"} minutos')
 
 
 print(f'Los niños han usado {usos_niños} veces BICIMAD')
 print(f'Los adolescentes han usado {usos_adolescentes} veces BICIMAD')
 print(f'Los jovenes han usado {usos_jovenes} veces BICIMAD')
 print(f'Los jovenes/adultos han usado {usos_jovenes_adultos} veces BICIMAD')
 print(f'Los adultos han usado {usos_adultos} veces BICIMAD')
 print(f'Los mayores han usado {usos_mayores} veces BICIMAD')
"""

if __name__ == "__main__":
    filename = sys.argv[1]
    with init() as sc:
        main(sc, filename)