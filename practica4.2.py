"""
ANÁLISIS BICIMAD.2

Teresa Ballester Navarro y David González Morala
"""

from pyspark import SparkContext, SparkConf
from collections import Counter #Para contar la frecuencia de los elementos de una lista
import json # Datos en formato JSON, pues los datos de BICIMAD vienen en ese formato
import datetime # Para poder trabajar con fechas 
import sys
import statistics

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

    return usuario_camino, media_camino, num_usuario_camino


def edades(data):

    usuarios_niños = data.filter(lambda x: x[5] == 0)
    usuarios_adolescentes = data.filter(lambda x: x[5] == 1)
    usuarios_jovenes = data.filter(lambda x: x[5] == 2)
    usuarios_jovenes_adultos = data.filter(lambda x: x[5] == 3)
    usuarios_adultos = data.filter(lambda x: x[5] == 4)
    usuarios_mayores = data.filter(lambda x: x[5] == 5)
    
    usuarios = [usuarios_niños, usuarios_adolescentes, usuarios_jovenes, usuarios_jovenes_adultos, usuarios_adultos, usuarios_mayores]
    medias = []
    for u in usuarios:
        medias.append(u.map(lambda x: (x[4])).mean()/60)
    
    usos = []
    for u in usuarios:
        usos.append(u.map(lambda x: (x[5],1)).reduceByKey(lambda x,y: x + y).take(1)[0][1])

    return usuarios, medias, usos

def estaciones(data):
    
    
    # Mapeo y conteo de estaciones
    estaciones_origen = data.map(lambda x: (x[2], 1))\
                               .reduceByKey(lambda x, y: x + y)
    
    # Ordenar por frecuencia
    estaciones_frecuentes = estaciones_origen.sortBy(lambda x: x[1], ascending = False)
    estaciones_NOfrecuentes = estaciones_origen.sortBy(lambda x: x[1], ascending = True)
    
    
    # Mapeo y conteo de estaciones
    estaciones_destino = data.map(lambda x: (x[3], 1)) \
                               .reduceByKey(lambda x, y: x + y)
    
    # Ordenar por frecuencia
    estaciones_frecuentes_d = estaciones_destino.sortBy(lambda x: x[1], ascending = False)
    estaciones_NOfrecuentes_d = estaciones_destino.sortBy(lambda x: x[1], ascending = True)
    
    usuarios_ciclo = ciclos(data)
    estaciones_ciclos = usuarios_ciclo[0].map(lambda x: (x[3], 1)) \
                           .reduceByKey(lambda x, y: x + y)
    
    estaciones_frecuentes_c = estaciones_ciclos.sortBy(lambda x: x[1], ascending = False)
    estaciones_NOfrecuentes_c = estaciones_ciclos.sortBy(lambda x: x[1], ascending = True)
    
        
    usuarios_camino = caminos(data)
    estaciones_caminos = usuarios_camino[0].map(lambda x: (x[3], 1)) \
                               .reduceByKey(lambda x, y: x + y)
   
    
    estaciones_frecuentes_ca = estaciones_caminos.sortBy(lambda x: x[1], ascending = False)
    estaciones_NOfrecuentes_ca = estaciones_caminos.sortBy(lambda x: x[1], ascending = True)
    
    

    lista_rdds = edades(data)[0]
    
    
    estaciones_niños = lista_rdds[0].map(lambda x: (x[3], 1)) \
                            .reduceByKey(lambda x, y: x + y)
    estaciones_frecuentes_n = estaciones_niños.sortBy(lambda x: x[1], ascending = False)
    estaciones_NOfrecuentes_n = estaciones_niños.sortBy(lambda x: x[1], ascending = True)


    estaciones_adolescentes = lista_rdds[1].map(lambda x: (x[3], 1)) \
                         .reduceByKey(lambda x, y: x + y)
    estaciones_frecuentes_a = estaciones_adolescentes.sortBy(lambda x: x[1], ascending = False)
    estaciones_NOfrecuentes_a = estaciones_adolescentes.sortBy(lambda x: x[1], ascending = True)
    
    
    estaciones_jovenes = lista_rdds[2].map(lambda x: (x[3], 1)) \
                         .reduceByKey(lambda x, y: x + y)
    estaciones_frecuentes_j = estaciones_jovenes.sortBy(lambda x: x[1], ascending = False)
    estaciones_NOfrecuentes_j = estaciones_jovenes.sortBy(lambda x: x[1], ascending = True)                         
                         
                         
    estaciones_jovenes_adultos = lista_rdds[3].map(lambda x: (x[3], 1)) \
                       .reduceByKey(lambda x, y: x + y)
    estaciones_frecuentes_j_a = estaciones_jovenes_adultos.sortBy(lambda x: x[1], ascending = False)
    estaciones_NOfrecuentes_j_a = estaciones_jovenes_adultos.sortBy(lambda x: x[1], ascending = True)                       
                       
                       
    estaciones_adultos = lista_rdds[4].map(lambda x: (x[3], 1)) \
                         .reduceByKey(lambda x, y: x + y)
    estaciones_frecuentes_ad = estaciones_adultos.sortBy(lambda x: x[1], ascending = False)
    estaciones_NOfrecuentes_ad = estaciones_adultos.sortBy(lambda x: x[1], ascending = True)                         
                         
                         
    estaciones_mayores = lista_rdds[5].map(lambda x: (x[3], 1)) \
                         .reduceByKey(lambda x, y: x + y)
    estaciones_frecuentes_m = estaciones_mayores.sortBy(lambda x: x[1], ascending = False)
    estaciones_NOfrecuentes_m = estaciones_mayores.sortBy(lambda x: x[1], ascending = True)                     

    estaciones_frecuentes_edades = [estaciones_frecuentes_n,estaciones_frecuentes_a,estaciones_frecuentes_j,
                                    estaciones_frecuentes_j_a,estaciones_frecuentes_ad,estaciones_frecuentes_m]
    
    estaciones_NOfrecuentes_edades = [estaciones_NOfrecuentes_n,estaciones_NOfrecuentes_a,estaciones_NOfrecuentes_j,
                                    estaciones_NOfrecuentes_j_a,estaciones_NOfrecuentes_ad,estaciones_NOfrecuentes_m]
     
       
    return estaciones_frecuentes,estaciones_NOfrecuentes, estaciones_frecuentes_d,\
            estaciones_NOfrecuentes_d,estaciones_frecuentes_c,estaciones_NOfrecuentes_c,\
            estaciones_frecuentes_ca,estaciones_NOfrecuentes_ca,\
           estaciones_frecuentes_edades, estaciones_NOfrecuentes_edades


def main(sc, filenames):
    rddlist = []
    for filename in filenames:
        rddlist.append(sc.textFile(filename).map(FiletoDic))
    
    rdd = sc.union(rddlist)
    data_ciclo = ciclos(rdd)
    print('--------ANÁLISIS DE CICLOS--------')
    print(f'El tiempo medio de los usuarios que realizan un ciclo es de {(f"{data_ciclo[1]:.2f}")} minutos')
    print(f'El número total de usuarios que realizan un ciclo es de {data_ciclo[3]} usuarios')                                                                        
    print(f'El número total de bicicletas usadas para realizar ciclos es {data_ciclo[2]}')
    print('\n\n')
    
    
    
    data_camino = caminos(rdd)
    print('--------ANÁLISIS DE CAMINOS--------')
    print(f'El tiempo medio de los usuarios que realizan un camino es de {(f"{data_camino[1]:.2f}")} minutos')
    print(f'Dentro de los usuarios que realizan un ciclo hay {data_camino[2]} usuarios que realizan un camino')
    print('\n\n')
    
    
    
    data_edades = edades(rdd)
    print('--------ANÁLISIS DE EDADES--------')

    for i in range(len(data_edades)):
        print(f'El tiempo medio de uso en los {nombres[i]} es {(f"{(data_edades[1][i]):.2f}")} minutos')
        print(f'Los {nombres[i]}  han usado {data_edades[2][i]}  veces BICIMAD')
        print('\n')
        
    data_estaciones = estaciones(rdd)
    
   
    # Obtener las estaciones más frecuentes
    print("--------------- 5 ESTACIONES de SALIDA MÁS FRECUENTES ---------------")
    estaciones_top = data_estaciones[0] # Estaciones más frecuentes
    print(estaciones_top.take(5))
    print('\n')
    
    print("--------------- 5 ESTACIONES de SALIDA MENOS FRECUENTES ---------------")
    estaciones_bottom =  data_estaciones[1]# Estaciones menos frecuentes 
    print(estaciones_bottom.take(5))
    print('\n')
        
    # Obtener las estaciones más frecuentes 
    print("--------------- 5 ESTACIONES de LLEGADA MÁS FRECUENTES ---------------")
    estaciones_top_d = data_estaciones[2] # Estaciones más frecuentes
    print(estaciones_top_d.take(5))
    print('\n')
    
    print("--------------- 5 ESTACIONES de LLEGADA MENOS FRECUENTES ---------------")
    estaciones_bottom_d = data_estaciones[3] # Estaciones menos frecuentes 
    print(estaciones_bottom_d.take(5))
    print('\n')
          
    print("--------------- 5 ESTACIONES para los CICLOS MÁS FRECUENTES ---------------")
    estaciones_top_c = data_estaciones[4] # Estaciones más frecuentes
    print(estaciones_top_c.take(5))
    print('\n')
    
    print("--------------- 5 ESTACIONES para los CICLOS MENOS FRECUENTES ---------------")
    estaciones_bottom_c = data_estaciones[5] # Estaciones menos frecuentes 
    print(estaciones_bottom_c.take(5))
    print('\n')
    
    print("--------------- 5 ESTACIONES para los CAMINOS MÁS FRECUENTES ---------------")
    estaciones_top_ca = data_estaciones[6] # Estaciones más frecuentes
    print(estaciones_top_ca.take(5))
    print('\n')
    
    print("--------------- 5 ESTACIONES para los CAMINOS MENOS FRECUENTES ---------------")
    estaciones_bottom_ca = data_estaciones[7] # Estaciones menos frecuentes 
    print(estaciones_bottom_ca.take(5))
    print('\n')
        
    for i in range(len(data_estaciones[8])): 
        print(f"--------------- 5 ESTACIONES MÁS FRECUENTES para {nombres[i]} ---------------")
        estaciones_top_n = data_estaciones[8][i] # Estaciones más frecuentes
        print(estaciones_top_n.take(5))
        
        
        print(f"--------------- 5 ESTACIONES MENOS FRECUENTES para {nombres[i]} ---------------")
        estaciones_bottom_n = data_estaciones[9][i] # Estaciones menos frecuentes 
        print(estaciones_bottom_n.take(5))
        print('\n')
        

if __name__ == "__main__":
    filename = sys.argv[1]
    with init() as sc:
        main(sc, filename)