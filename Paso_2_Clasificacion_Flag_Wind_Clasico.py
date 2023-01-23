import pandas as pd
import numpy as np
from glob import glob
import os
import matplotlib.pyplot as plt
from sqlalchemy import create_engine
import BaseDeDatos_Lib_v02 as BD
import O3_Nilu_Programas_v0_02 as NL


engine = create_engine('postgresql://postgres:vag@10.30.19.227:5432/GAWUSH_PROCESADOS')
Tabla_Data = 'O3_SN_0330102717_Minutal'
engine_wind = create_engine('postgresql://postgres:vag@10.30.19.227:5432/GAWUSH_DATABASE')
Tabla_Wind = 'SIAP'
Tabla_Wind_Flag = 'O3_SN_0330102717_Minutal_Flag_Wind'

inicio = pd.to_datetime('2022-01-01 00:00')
fin = pd.to_datetime('2023-01-01 00:00')

engine_Final = create_engine('postgresql://postgres:vag@10.30.19.227:5432/GAWUSH_PROCESADOS')
Tabla_Data_Final = 'O3_SN_0330102717_Flag_Wind'


O3_min = -2
O3_max = 40



def Agrupamiento_vecinos(df, n_neighbors, weights):
    from sklearn import neighbors, datasets
    df['DateTime_J'] = df.index.to_julian_date()
    df_simplificado = df[['DateTime_J', 'Flag_Wind', 'O3']]
    clf = neighbors.KNeighborsClassifier(n_neighbors, weights=weights)
    clf.fit(df_simplificado['DateTime_J', 'Flag_Wind'], df_simplificado.O3)
    Z = clf.predict(df_simplificado.DateTime_J)
    print(max(Z))


def Agrupamiento(df, epsilon, grupos):
    # SCIKIT
    from sklearn.decomposition import PCA
    import sklearn.neighbors
    from sklearn.neighbors import kneighbors_graph
    from sklearn import preprocessing
    from sklearn.cluster import DBSCAN

    #Dia Juliano
    df['DateTime_J'] = df.index.to_julian_date()
    # Se normalizan los datos con MinMax()
    min_max_scaler = preprocessing.MinMaxScaler()
    #df_corto = pd.concat([df[['DateTime_J', 'O3']], df_second.CO_sync.reindex(df.index)], axis=1)
    df_corto = df[['DateTime_J', 'Flag_Wind', 'O3']]
    df_corto = df_corto.dropna()
    df_escalado = min_max_scaler.fit_transform(df_corto)
    df_escalado = pd.DataFrame(df_escalado)
    df_escalado = df_escalado.rename(columns={0: 'DateTime_J', 1: 'Flag_Wind', 2: 'O3'})

    # Ejecutamos DBSCAN
    dbscan = DBSCAN(eps=epsilon*1400/len(df_corto), min_samples=grupos, metric="euclidean").fit(df_escalado)
    clusters = dbscan.fit_predict(df_escalado)
    df_corto['Flag_Stat'] = clusters
    df['Flag_Stat'] = np.nan
    df.Flag_Stat.update(df_corto.Flag_Stat)
    return df

def Determinacion_Parametros(df, df_second, df_meteo, eps, grupos):
    n_neighbors = 15
    O3_Raw = Agrupamiento(df,  eps, grupos)
    #O3_Raw = Agrupamiento_vecinos(df, n_neighbors, 'uniform')
    axes = plt.subplot(411)
    axes.set_title('Ozono [ppb]')
    axes.set_ylim(0, 35)
    axes.set_xlim([O3_Raw.index[0], O3_Raw.index[-1]])
    axes.get_xaxis().set_visible(False)
    plt.scatter(O3_Raw.DateTime, O3_Raw.O3, c='grey', s=100)
    plt.scatter(O3_Raw.DateTime.loc[O3_Raw.Flag_Stat >= 0], O3_Raw.O3.loc[O3_Raw.Flag_Stat >= 0],
                c=O3_Raw.Flag_Stat.loc[O3_Raw.Flag_Stat >= 0], cmap="plasma", s=10)
    axes = plt.subplot(412)
    axes.set_title('Ozono [ppb]')
    axes.set_ylim(0, 0.01)
    axes.set_xlim([O3_Raw.index[0], O3_Raw.index[-1]])
    axes.get_xaxis().set_visible(False)
    #plt.scatter(O3_Raw.DateTime, O3_Raw.O3, c='grey', s=100)
    plt.scatter(df_second.index, df_second.STD)
    #plt.scatter(O3_Raw.DateTime, O3_Raw.O3, c=df_meteo.Flag_Wind, cmap="plasma", s=10)

    axes = plt.subplot(413)
    axes.set_title('Viento (Rapidez) [m/s]')
    axes.set_xlim([O3_Raw.index[0], O3_Raw.index[-1]])
    axes.get_xaxis().set_visible(False)
    plt.plot(df_meteo.index, df_meteo.ViMS, linestyle='', marker='.', )

    axes = plt.subplot(414)
    axes.set_title('Viendo (Direccion) [Grados]')
    axes.set_xlim([O3_Raw.index[0], O3_Raw.index[-1]])
    plt.plot(df_meteo.DateTime, df_meteo.VdGrad, linestyle='', marker='.', )
    #plt.scatter(df_meteo.DateTime, df_meteo.VdGrad, c=df_meteo.Flag_8, cmap="plasma", s=10)


    plt.show()


def Flag_Wind_Cal(df, flag, Vmin, Vmax, AngMin, AngMax):
    if (AngMin < AngMax):
        df_aux = ((df.ViMS >= Vmin) & (df.ViMS < Vmax) & (df.VdGrad >= AngMin) & (df.VdGrad < AngMax)) * (flag + 1000)
        if df_aux > 0:
            return df_aux
        else:
            return np.nan
    if (AngMin > AngMax):
        df_aux = ((df.ViMS >= Vmin) & (df.ViMS < Vmax) & ((df.VdGrad > AngMin) | (df.VdGrad <= AngMax))) * (flag + 1000)
        if df_aux > 0:
            return df_aux
        else:
            return np.nan




def Wind_8(df, flag, AngMin, AngMax):
    if AngMin < AngMax:
        df_aux = ((df.VdGrad > AngMin) & (df.VdGrad <= AngMax))*flag
    if AngMin > AngMax:
        df_aux = ((df.VdGrad > AngMin) | (df.VdGrad < AngMax)) * flag
    df_aux = df_aux.to_frame('Flag_8')
    df_aux.replace(0, np.nan, inplace=True)
    return df_aux.dropna()

velocMin = 2.5
angMin = 100
angMAx = 300

if __name__ == '__main__':
    O3_Minutal = BD.buscarEnBaseDeDatos(engine,Tabla_Data, inicio, fin)
    O3_Meteo = BD.buscarEnBaseDeDatos(engine_wind, Tabla_Wind, inicio, fin)

    #Clasificar de acuerdo al viento

    O3_Meteo['Flag_Wind'] = np.nan
    Flag188 = O3_Meteo.apply(lambda x: Flag_Wind_Cal(x, 188, 0, 2.5, 0, 360), axis=1)
    Flag189 = O3_Meteo.apply(lambda x: Flag_Wind_Cal(x, 189, 2.5, 99, 300, 200), axis=1)
    Flag000 = O3_Meteo.apply(lambda x: Flag_Wind_Cal(x, 000, 2.5, 99, 200, 300), axis=1)
    O3_Meteo.Flag_Wind.update(Flag188)
    O3_Meteo.Flag_Wind.update(Flag189)
    O3_Meteo.Flag_Wind.update(Flag000)
    O3_Meteo.Flag_Wind.fillna(1999, inplace=True) # Mejorar con un aproximado

    #A cada agrupamiento verificar su bandera

    O3_Minutal['Flag_Wind'] = np.nan
    O3_Minutal.Flag_Wind.update(O3_Meteo.Flag_Wind)
    O3_Paso_2 =  O3_Minutal

    #O3_Paso_2 = pd.concat([O3_Paso_2.O3, O3_Cal.Flag_Zero], axis=1)
    axes = plt.subplot(311)
    axes.set_xlim([inicio, fin])
    axes.set_ylim([0, 40])

    plt.scatter(O3_Paso_2.DateTime, O3_Paso_2.O3, c='grey', s=100)
    plt.scatter(O3_Paso_2.DateTime.loc[(O3_Paso_2.Flag_Wind == 1000) & (O3_Paso_2.Flag_Zero == 0)],
                O3_Paso_2.O3.loc[(O3_Paso_2.Flag_Wind == 1000) & (O3_Paso_2.Flag_Zero == 0)], c='blue', s=10, alpha=0.1)
    plt.scatter(O3_Paso_2.DateTime.loc[(O3_Paso_2.Flag_Wind == 1189) & (O3_Paso_2.Flag_Zero == 0)],
                O3_Paso_2.O3.loc[(O3_Paso_2.Flag_Wind == 1189) & (O3_Paso_2.Flag_Zero == 0)], c='green', s=10, alpha=0.1)
    plt.scatter(O3_Paso_2.DateTime.loc[(O3_Paso_2.Flag_Wind == 1188) & (O3_Paso_2.Flag_Zero == 0)],
                O3_Paso_2.O3.loc[(O3_Paso_2.Flag_Wind == 1188) & (O3_Paso_2.Flag_Zero == 0)], c='red', s=2, alpha=0.1)
    axes = plt.subplot(312)
    axes.set_title('Viento (Rapidez) [m/s]')
    axes.set_xlim([inicio, fin])
    axes.get_xaxis().set_visible(False)
    plt.plot(O3_Meteo.DateTime, O3_Meteo.ViMS, linestyle='', marker='.', )

    axes = plt.subplot(313)
    axes.set_title('Viendo (Direccion) [Grados]')





    axes.set_xlim([inicio, fin])
    plt.plot(O3_Meteo.DateTime, O3_Meteo.VdGrad, linestyle='', marker='.', )

    plt.show()
    O3_Paso_2.Flag_Wind = (O3_Paso_2.Flag_Wind - 1000)/1000
    BD.Consulta_de_Existencia_Y_Envio_DIAxDIA(O3_Paso_2.Flag_Wind, engine_Final, Tabla_Wind_Flag)



