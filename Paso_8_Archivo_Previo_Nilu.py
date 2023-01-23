import pandas as pd
import numpy as np
from glob import glob
import os
import matplotlib.pyplot as plt
from sqlalchemy import create_engine
import BaseDeDatos_Lib_v02 as BD
import O3_Nilu_Programas_v0_02 as NL


engine = create_engine('postgresql://postgres:vag@10.30.19.227:5432/GAWUSH_PROCESADOS')
engine_Flag_Manual = create_engine('postgresql://postgres:vag@10.30.19.227:5432/GAWUSH_PROCESADOS')
Tabla_Flag_Manual = 'O3_Minutal_Flag_Manual'
Tabla_Data = 'O3_SN_0330102717_Minutal'
Tabla_Flag_Wind = 'O3_SN_0330102717_Minutal_Flag_Wind'
Tabla_EBAS_NILU = 'o3_ush_ebas_nilu'

inicio = pd.to_datetime('2022-01-01 00:00')
fin = pd.to_datetime('2023-01-01 00:00')


if __name__ == '__main__':
    O3_Minutal = BD.buscarEnBaseDeDatos(engine, Tabla_Data, inicio, fin)
    Flag_Wind = BD.buscarEnBaseDeDatos(engine, Tabla_Flag_Wind, inicio, fin)
    Flag_Manual = BD.buscarEnBaseDeDatos(engine_Flag_Manual, Tabla_Flag_Manual, inicio, fin)

    O3_Minutal['Flag_Manual']=np.nan
    O3_Minutal.Flag_Manual.update(Flag_Manual.Flag_Manual)

    #Recorrer Hora a Hora Buscando Condiciones Base
    #Horas = pd.date_range(inicio.floor('H'), fin.ceil('H'), freq='H', closed='left')
    #Quitar horas ya calculadas.
    O3_Hr = BD.buscarEnBaseDeDatos(engine,Tabla_EBAS_NILU, inicio, fin).drop(['DateTime'], axis=1)
    try:
        O3_Minutal = O3_Minutal.loc[~O3_Minutal.index.floor('H').isin(O3_Hr.index)]
    except:
        index_old = []

    Flag_Wind = Flag_Wind.reindex(O3_Minutal.index)
    #Recorrer y calcular
    O3_Minutal = pd.concat([O3_Minutal, Flag_Wind], axis=1)
    O3_EBAS_NILU = pd.concat([O3_Hr, NL.Generar_EBAS_NILU_MANUAL(O3_Minutal)], sort=False)
    
    BD.Consulta_de_Existencia_Y_Envio_DIAxDIA(O3_EBAS_NILU, engine, Tabla_EBAS_NILU)

    #O3_Dy = NL.Generar_Da_para_SMN(O3_EBAS_NILU)
    #O3_Dy.to_csv('O3_Dy.csv')
    #O3_Mo = NL.Generar_Mo_para_SMN(O3_EBAS_NILU)
    #O3_Mo.to_csv('O3_Mo.csv')
    axes = plt.subplot(311)
    axes.set_xlim([inicio, fin])
    axes.set_ylim([0, 40])
    plt.xlabel('Tiempo')
    plt.ylabel('O3 [ppb]')
    lst2 = [item[0] for item in O3_EBAS_NILU.flag.values]
    FLAG_VALID_FULL = np.array(lst2) == np.array([0])
    FLAG_VALID_BAJO = np.array(lst2) == np.array([188])
    plt.scatter(O3_Minutal.index, O3_Minutal.O3, c='grey', s=100)
    plt.scatter(O3_EBAS_NILU.loc[FLAG_VALID_FULL].index,
                O3_EBAS_NILU.o3.loc[FLAG_VALID_FULL], c='blue', s=10, alpha=0.5)

    axes = plt.subplot(312)
    plt.scatter(O3_Minutal.index, O3_Minutal.O3, c='grey', s=100)
    plt.scatter(O3_EBAS_NILU.loc[FLAG_VALID_BAJO].index,
                O3_EBAS_NILU.o3.loc[FLAG_VALID_BAJO], c='blue', s=10, alpha=0.5)
    axes = plt.subplot(313)
 
    plt.show()