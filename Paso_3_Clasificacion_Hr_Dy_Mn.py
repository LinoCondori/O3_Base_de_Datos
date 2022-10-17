import pandas as pd
import numpy as np
from glob import glob
import os
import matplotlib.pyplot as plt
from sqlalchemy import create_engine
import BaseDeDatos_Lib_v02 as BD
import O3_Nilu_Programas_v0_02 as NL

engine_Flag_Manual = create_engine('postgresql://postgres:vag@10.30.19.227:5432/GAWUSH_PROCESADOS')
Tabla_Flag_Manual = 'O3_Minutal_Flag_Manual'
engine = create_engine('postgresql://postgres:vag@10.30.19.227:5432/GAWUSH_PROCESADOS')
Tabla_Data = 'O3_SN_0330102717_Minutal'
Tabla_Flag_Wind = 'O3_SN_0330102717_Minutal_Flag_Wind'
Tabla_SMN_Hr = 'O3_SMN_BaseLine_Hr'

inicio = pd.to_datetime('2022-01-01 00:00')
fin = pd.to_datetime('2022-10-01 00:00')


if __name__ == '__main__':
    O3_Minutal = BD.buscarEnBaseDeDatos(engine, Tabla_Data, inicio, fin)
    Flag_Wind = BD.buscarEnBaseDeDatos(engine, Tabla_Flag_Wind, inicio, fin)
    Flag_Manual = BD.buscarEnBaseDeDatos(engine_Flag_Manual, Tabla_Flag_Manual, inicio, fin)

    O3_Minutal['Flag_Manual']=np.nan
    O3_Minutal.Flag_Manual.update(Flag_Manual.Flag_Manual)

    #Recorrer Hora a Hora Buscando Condiciones Base
    Horas = pd.date_range(inicio.floor('H'), fin.ceil('H'), freq='H', closed='left')
    #Quitar horas ya calculadas.
    O3_Hr = BD.buscarEnBaseDeDatos(engine,Tabla_SMN_Hr, inicio, fin).drop(['DateTime'], axis=1)
    try:
        O3_Minutal = O3_Minutal.loc[~O3_Minutal.index.floor('H').isin(O3_Hr.index)]
    except:
        index_old = []

    #O3_Minutal = O3_Minutal.reindex(O3_Minutal.index.difference(index_old))
    Flag_Wind = Flag_Wind.reindex(O3_Minutal.index)
    #Recorrer y calcular
    O3_Minutal = pd.concat([O3_Minutal, Flag_Wind], axis=1)
    O3_Paso_3 = pd.concat([O3_Hr, NL.Generar_Hr_para_SMN_Tres_Banderas(O3_Minutal)], sort=False)
    O3_Paso_3.to_csv('O3_Hr.csv')
    BD.Consulta_de_Existencia_Y_Envio_DIAxDIA(O3_Paso_3, engine, Tabla_SMN_Hr)

    O3_Dy = NL.Generar_Da_para_SMN(O3_Paso_3)
    O3_Dy.to_csv('O3_Dy.csv')
    O3_Mo = NL.Generar_Mo_para_SMN(O3_Paso_3)
    O3_Mo.to_csv('O3_Mo.csv')
    axes = plt.subplot(311)
    axes.set_xlim([inicio, fin])
    axes.set_ylim([0, 40])
    plt.xlabel('Tiempo')
    plt.ylabel('O3 [ppb]')
    plt.scatter(O3_Minutal.index, O3_Minutal.O3, c='grey', s=100)
    plt.scatter(O3_Paso_3.loc[(O3_Paso_3.F == 0)].index,
                O3_Paso_3.O3.loc[(O3_Paso_3.F == 0)], c='blue', s=10, alpha=0.5)
    plt.plot(O3_Mo.index, O3_Mo.O3, linestyle='-', marker='.', ms=20, c='tab:green')
    #plt.scatter(O3_Paso_3.loc[(O3_Paso_3.F == 1)].index,
    #            O3_Paso_3.O3.loc[(O3_Paso_3.F == 1)], c='green', s=10, alpha=0.5)
    #plt.plot(O3_Mo.index, O3_Mo.O3, linestyle='', marker='.', ms=20)

    axes = plt.subplot(312)
    axes.set_title('Viento (Rapidez) [m/s]')
    axes.set_xlim([inicio, fin])
    axes.get_xaxis().set_visible(False)
    # plt.plot(O3_Meteo.DateTime, O3_Meteo.ViMS, linestyle='', marker='.', )

    axes = plt.subplot(313)
    axes.set_title('O3 [ppb] Mensual')
    axes.set_xlim([inicio, fin])
    axes.set_ylim([0, 40])
    plt.plot(O3_Mo.index, O3_Mo.O3, linestyle='-', marker='.', ms=20)

    plt.show()



