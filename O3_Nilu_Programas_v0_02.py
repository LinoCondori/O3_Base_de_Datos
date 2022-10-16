from __future__ import print_function
import pyodbc
import numpy as np
import pandas as pd
import os
import matplotlib.pyplot as plt
from glob import glob


menErrorLong = '#'*45 + '\n' +'#'*14  + 'ERROR DE LONGITUD' + '#'*14 + '\n' +'#'*45 + '\n'
#direcMeteo = os.path.join(os.path.dirname(os.getcwd()), 'Meteo')
direcMeteo = os.path.normpath( '/media/linoc/Datos/Meteo/')
from sqlalchemy import create_engine
credenciales = 'postgresql://postgres:vag@10.30.19.227:5432/GAWUSH_DATABASE'
engine = create_engine(credenciales)
velocMin = 2.5
angMin = 100
angMAx = 300


# 0.000 - dato valido
# 0.999 - dato invalido y perdido.
# 0.980 - dato invalido por calibración o span

# Nosotros para otras estaciones utilizamos:

# 0.559 - dato valido por contaminación local no especificada  (Ej:  un  evento de humo, etc)
# 0.189 - dato valido posible contaminación indicada por  dirección   del viento. ()
# 0.188 - dato valido por viento bajo ( Cuando el viento es  menor a 3 m/s)
# 0.680 - dato valido por sector de viento indefinido (  cuando  existe  el dato de ozono pero no existe el dato met)
#  0.392 - dato valido pero cuando el promedio se realizo con  menos   del 3/4 partes de los datos.

flag_V_0 = 0      # dato valido
flag_V_B = 188    # dato valido por viento bajo ( Cuando el viento es  menor a 3 m/s)
flag_V_NB = 189   # dato valido Sector no Limpio

flag_Cal = 980    # 0.980 - dato invalido por calibración o span
flag_NV = 999     # dato invalido y perdido.
flag_N_MET = 680  # 0.680 - cuando  existe  el dato de ozono pero no existe el dato met

###########################################3
#consultar
flag_V_75 = 392 # Data completeness less than 75%


def Buscar_CSV_from_VAG_Script(dir):
    df = pd.DataFrame()
    listaDeArchivos = glob(os.path.join(dir, 'O3_SN_49C*'))
    for archivo in listaDeArchivos:
        df = df.append(pd.read_csv(archivo), sort=False)
    if df.empty:
        return df
    df.reset_index(drop=True, inplace=True)
    df.rename({'read_dt': 'DateTime'}, axis=1, inplace=True)
    df.DateTime = pd.to_datetime(df.DateTime)
    df.set_index('DateTime')
    df.set_index('DateTime', inplace=True)
    return df.loc[~np.isnan(df.O3)]

def Promedio_Minutal(df):
    df_MinNuevo = df.resample('T').mean()  # Falta las banderas
    df.Flags = df.Flags.apply(lambda x: int(x, 16))
    df_MinNuevo['Flags'] = df.Flags.resample('T').median().dropna().astype('int').apply(
        lambda x: format(x, 'x')).reindex(df_MinNuevo.index)
    return df_MinNuevo.dropna()

def Validador(df):
    # Validar que DateTime Sea Tipo DateTime
    # print('Comienza Validacion')
    if df.index.dtype.name != 'datetime64[ns]':
        print('Problemas en validacion')
        print('Problemas de Indice de Tiempo')
    # self.col
    df.O3 = pd.to_numeric(df.O3, errors='coerce').round(4)
    df.CellA = pd.to_numeric(df.CellA, errors='coerce').round(1)
    df.CellB = pd.to_numeric(df.CellB, errors='coerce').round(1)
    df.BenchTemp = pd.to_numeric(df.BenchTemp, errors='coerce').round(1)
    df.LampTemp = pd.to_numeric(df.LampTemp, errors='coerce').round(1)
    df.FlowA = pd.to_numeric(df.FlowA, errors='coerce').round(3)
    df.FlowB = pd.to_numeric(df.FlowB, errors='coerce').round(3)
    df.Pres = pd.to_numeric(df.Pres, errors='coerce').round(1)
    # print('Finaliza Validacion')
    return df


def Buscar_Archivos_Unificados(dir, cond, ini, fin):
    df = pd.DataFrame()
    dict = {'Alarms': 'Flag', 'IntensityA': 'CellA', 'IntensityB': 'CellB', 'flags': 'Flag', 'lampA': 'CellA',
            'lampB': 'CellB', 'temp': 'BenchTemp', 'A': 'LampTemp', 'C': 'FlowA', 'D': 'FlowB', 'press': 'Pres',
            'cell A int': 'CellA', 'cell B int': 'CellB', 'bench temp': 'BenchTemp', 'lamp temp': 'LampTemp',
            'flow A': 'FlowA', 'flow B': 'FlowB', 'pres': 'Pres'}
    col = ['DateTime', 'O3', 'Flag', 'CellA', 'CellB', 'BenchTemp', 'LampTemp', 'FlowA', 'FlowB', 'Pres']

    listaDeArchivos = glob(os.path.join(dir, cond))
    for archivo in listaDeArchivos:
        try:
            df = df.append(pd.read_csv(archivo).rename(dict, axis=1)[col], sort=False)
        except:
            print('ddddd')
    df.reset_index(drop=True, inplace=True)
    df.DateTime = pd.to_datetime(df.DateTime)
    df.set_index('DateTime')
    df.set_index('DateTime', inplace=True)
    df.sort_index(inplace=True)
    df = df.loc[(df.index >= pd.to_datetime(ini)) & (df.index < pd.to_datetime(fin))]
    return df.loc[~np.isnan(df.O3)]
    #return df.dropna(subset=['O3'])

def Buscar_Archivos(dir, cond):
    df = pd.DataFrame()
    listaDeArchivos = glob(os.path.join(dir, cond))
    for archivo in listaDeArchivos:
        try:
            df = df.append(pd.read_csv(archivo), sort=False)
        except:
            print('ddddd')
    df.reset_index(drop=True, inplace=True)
    df.DateTime = pd.to_datetime(df.DateTime)
    df.set_index('DateTime')
    df.set_index('DateTime', inplace=True)
    df.sort_index(inplace=True)
    return df.loc[~np.isnan(df.O3)]

def buscarMeteo(fecha_ini, fecha_fin):
    meteo = pd.DataFrame()
    for dia in pd.date_range(fecha_ini, fecha_fin, freq='d', closed='left'):
        meteo = meteo.append(buscarMeteoDia(dia))
    return meteo

def buscarMeteoDia(fecha):
    columnas = ['velocid',	'dirVect', 'Temperatura']
    owd = os.getcwd()
    df_aux = meteoArchivo(fecha)
    os.chdir(owd)
   # Arreglar la hora
    if df_aux.empty:
        return df_aux
    df_aux['DateTime'] = pd.to_datetime(df_aux['DateTime'])
    df_aux.set_index('DateTime', inplace=True)  # se selecciona la hora como indice
    df_aux = df_aux.loc[(df_aux.index >= fecha) & (df_aux.index <fecha+pd.DateOffset(1)) ]# Verificar que solo tenga datos de ese dia.
    if len(df_aux) != 1440:
        print ('DIA CON DATOS DE MAS')
    return df_aux[columnas]

def meteoArchivo(fecha):
    df_aux = pd.DataFrame()  # Crea un DataFrame Vacio
    # Busca la carpeta de la direccion pedida
    #Campbell
    direcTemp = os.path.join(direcMeteo, 'Campbell', fecha.strftime('%Y'), 'meteo_' + fecha.strftime('%Y-%m-%d') + '.txt')
    #print (direcTemp)
    try:
        df_aux = pd.read_csv(direcTemp, sep=',')
    except:
        return df_aux
    return df_aux

def buscarMeteo_SIAP(fecha_ini, fecha_fin):
    meteo = pd.DataFrame()
    for dia in pd.date_range(fecha_ini, fecha_fin, freq='d'):
        #meteo = meteo.append(buscarMeteo_SIAP_Dia(dia))
        meteo = meteo.append(buscarEnBaseDeDatos('SIAP', dia, dia + pd.to_timedelta(1, unit='D'))[['ViMS', 'VdGrad', 'TambC']])
    return meteo

def buscarMeteo_SIAP_Dia(fecha):
    columnas = ['ViMS', 'VdGrad', 'TambC']

    #Campbell
    direcTemp = os.path.join(direcMeteo, 'SIAP', fecha.strftime('%Y'), 'USH' + fecha.strftime('%Y-%m-%d') + '.csv')
    try:
        df_aux = pd.read_csv(direcTemp, sep=',')
        df_aux['DateTime'] = pd.to_datetime(df_aux['DateTime'])
        df_aux.set_index('DateTime', inplace=True)  # se selecciona la hora como indice
        df_aux = df_aux.loc[(df_aux.index >= fecha) & (
                    df_aux.index < fecha + pd.DateOffset(1))]  # Verificar que solo tenga datos de ese dia.
    except:
        return pd.DataFrame()

    if len(df_aux) > 1440:
        print ('DIA CON DATOS DE MAS')
    return df_aux[columnas]

def buscarEnBaseDeDatos( Tabla, inicio, fin):
    #columnas = ['ViMS', 'VdGrad', 'TambC']

    try:
        consulta = 'Select * from "' + Tabla + '" where "DateTime" >= \'' + inicio._repr_base + '\' and "DateTime" < \'' + fin._repr_base + '\' order by "DateTime"'
        print(consulta)
        df_aux = pd.read_sql_query(consulta, con=engine)
        df_aux.DateTime = pd.to_datetime(df_aux.DateTime)
        df_aux.set_index(['DateTime'], inplace=True)
        df_aux['DateTime'] = df_aux.index
    except:
        return pd.DataFrame()

    if len(df_aux) > 1440:
        print ('DIA CON DATOS DE MAS')
    return df_aux


def correcionZero(O3Crudo, umbral):
    #O3desplazado = O3crudo.copy()#Copia del Marco de datos Original
    O3desplazado = pd.DataFrame()
    Zeros = pd.read_csv('./Paso_2/Zeros_' + str(O3Crudo.index.year[0]) + '.csv')
    Zeros.DateTime = pd.to_datetime(Zeros.DateTime)
    Zeros.set_index('DateTime', inplace=True)
    Zeros.Cal_Zero = Zeros.Cal_Zero * 1
    #Las Correcciones se realizan por cada mes.
    for mes in pd.date_range(start=O3Crudo.index[0], end=O3Crudo.index[-1], freq='MS'):
        Mes_Corregido = correccionMensual(O3Crudo.loc[(O3Crudo.index >= mes) & (O3Crudo.index < mes + pd.DateOffset(months=1))].copy(), Zeros.loc[(Zeros.index >= mes) & (Zeros.index < mes + pd.DateOffset(months=1))].copy(), umbral)
        O3desplazado = O3desplazado.append(Mes_Corregido)
        print(mes)


    return O3desplazado

def correccionMensual(df_o3, df_ze, ub):
    calibracion = pd.DataFrame()  # donde guardo todas las horas de calibracion.
    df_o3.loc[:, 'Cal_Zero'] = df_ze.Cal_Zero.reindex(df_o3.index)
    Cal_Zero = df_o3.O3.loc[(df_o3.O3 <= ub) & (df_o3.O3 >= -ub) & (df_o3.Flag == '1c000000') & (df_o3.Cal_Zero == 1)]
    print('Desplazamiento del mes ' + str(df_o3.index[0]) + 'es : ' + str(Cal_Zero.mean()))
    print('Desviacion del mes ' + str(df_o3.index[0]) + 'es : ' + str(Cal_Zero.STD()))
    Offset = Cal_Zero.mean()
    df_o3.O3 = df_o3.O3 - Offset
    return df_o3

def Condicion_Base(df, fname):
    fname = 'Flag_' + fname
    df[fname] = ((df.Dir < angMAx) & (df.Dir > angMin) & (df.Vel > velocMin)) * 1000 + (
                (df.Dir < angMAx) & (df.Dir > angMin) & (df.Vel > 0) & (df.Vel <= velocMin)) * 1188 + (
                (df.Dir >= angMAx) | (df.Dir <= angMin) | (df.Vel == 0)) * 1189 + 0
    print(df.loc[(df[fname] != 1000) & (df[fname] != 1188) & (df[fname] != 1189)])
    print(df.loc[(df[fname] != 1000) & (df[fname] != 1188) & (df[fname] != 1189) & (df[fname] != 0)])
    df[fname].fillna(0, inplace=True)
    return (df[fname]-1000)/1000

def CargarDatosMeteo(dir, principio):
    anio = pd.to_datetime(principio)
    df = pd.read_csv(os.path.join(dir, 'Flag_Wind_' + str(anio.year) + '.csv'))
    df['DateTime'] = pd.to_datetime(df['DateTime'])
    df.set_index('DateTime', inplace=True)
    df = df.round(3)
    #df.drop(['Flag_SIAP', 'Flag_CAMP'], axis=1, inplace=True)
    return df

def Generar_para_Nilu(df):
    df_aux = pd.DataFrame(columns=['O3', 'ND', 'SD', 'Flag'])
    df_aux.index.name = 'DateTime'
    for hora in pd.date_range(start=df.index[0].floor('D'), end=df.index[-1].ceil('D'), freq='H', closed='left'):
        df_aux.loc[hora] = niluRow(df.loc[df.index.floor('H') == hora].copy())
    df_aux.O3 = df_aux.O3.round(2)
    df_aux.SD = df_aux.SD.round(2)
    return df_aux

def niluRow(datosHora):
    minDatos = 45

    if datosHora.empty:
        O3prom = np.nan
        ND = 0
        SD = np.nan
        Flag = [999]
        return [O3prom, ND, SD, Flag]
    #primero chequear la longitud 60
    if len(datosHora) != 60:
        print(datosHora.index[0])
        print(len(datosHora))

    datosHora = datosHora.loc[datosHora.Flag_Inst]
    # Bandera 0.980 Calibracion
    if len(datosHora.loc[datosHora['Cal_Zero'] == 1]) >= 20:
        O3prom = np.nan
        ND = 0
        SD = np.nan
        Flag = [flag_Cal]
        return [O3prom, ND, SD, Flag]

    #Verifico Tener al menos el 50% de los datos (30 datos)
    if len(datosHora.O3.loc[~np.isnan(datosHora['O3'])]) >= 30:
        Datos = datosHora.O3.loc[(datosHora['FlagWind'] == 0)]


        # Flag 0.000 con mas del 75%
        if len(Datos) >= minDatos:
            O3prom = Datos.mean() #
            ND = len(Datos)
            SD = Datos.std()
            Flag = [flag_V_0]
            return [O3prom, ND, SD, Flag]
            #Falta Bandera para no ingresar en dos condiciones seguidas

        #Bandera Flag 0.000 y 0.392 - dato valido pero con datos mayores al 50% y menores al 75%
        if len(Datos) >= 30:
            Datos = datosHora['O3'].loc[datosHora['FlagWind'] == 0]
            O3prom = Datos.mean()  #
            ND = len(Datos)
            SD = Datos.std()
            Flag = [flag_V_0, flag_V_75]
            return [O3prom, ND, SD, Flag]
            # Falta Bandera para no ingresar en dos condiciones seguidas

        # Bandera 0.188, Dato valido pero de viento Bajo
        Datos = datosHora.O3.loc[(datosHora['FlagWind'] == 0) | (datosHora['FlagWind'] == 0.188)]
        if len(Datos) >= minDatos:
            O3prom = Datos.mean()  #
            ND = len(Datos)
            SD = Datos.std()
            Flag = [flag_V_B]
            return [O3prom, ND, SD, Flag]

        # Bandera 0.188 pero con menos del 75%, Dato valido pero de viento Bajo
        if len(Datos) >= 30:
            O3prom = Datos.mean()  #
            ND = len(Datos)
            SD = Datos.std()
            Flag = [flag_V_B, flag_V_75]
            return [O3prom, ND, SD, Flag]
            # Falta Bandera para no ingresar en dos condiciones seguidas

        #Bandera 0.189, Dato valido pero de viento NO limpio
        Datos = datosHora.O3
        if len(Datos) >= minDatos:
            O3prom = Datos.mean() #
            Flag = [flag_V_NB]
            ND = len(Datos)
            SD = Datos.std()
            return [O3prom, ND, SD, Flag]
            # Falta Bandera para no ingresar en dos condiciones seguidas



    #Bandera 0.999 Cuando no entra en ninguna de las anteriorse condiciones
    O3prom = np.nan
    ND = 0
    SD = np.nan
    Flag = [999]
    return [O3prom, ND, SD, Flag]


def filtroFlag(x, filtro):
    if x == filtro:
        return True
    return False

def Generar_Mi_para_SMN(df):
    df_aux = df.copy()
    df_aux['Date'] = df.index.date
    df_aux['Hour'] = df.index.strftime('%H:%M')
    df_aux.O3 = df.O3.round(2)
    df_aux['Flag'] = (~df.Flag_Inst | (df.Cal_Zero != 0))*2 + 1*((df.FlagWind != 0) & (df.Flag_Inst & (df.Cal_Zero == 0)))
    df_aux.Flag = df_aux.Flag.astype('int')
    return df_aux[['Date', 'Hour', 'O3', 'Flag']]

def Generar_Hr_para_SMN_Tres_Banderas(df):
    df_aux = pd.DataFrame(columns=['O3', 'ND', 'SD', 'F'])
    df_aux.index.name = 'DateTime'
    if df.empty:
        return pd.DataFrame()
    for hora in pd.date_range(start=df.index[0].floor('D'), end=df.index[-1].ceil('D'), freq='H', closed='left'):
        df_aux.loc[hora] = Hr_Row_v2(df.loc[df.index.floor('H') == hora].copy())
    df_aux.index.name = 'DateTime'
    df_aux['DATE'] = df_aux.index.date
    df_aux['TIME'] = df_aux.index.strftime('%H:%M')
    df_aux.O3 = df_aux.O3.round(2)
    df_aux.SD = df_aux.SD.round(2)
    df_aux.ND = df_aux.ND.astype('int')
    df_aux.F = df_aux.F.astype('int')
    return df_aux[['DATE', 'TIME', 'O3', 'ND', 'SD', 'F']]

def Generar_EBAS_NILU_MANUAL(df):
    df_aux = pd.DataFrame(columns=['o3', 'std', 'flag'])
    df_aux.index.name = 'DateTime'
    if df.empty:
        return pd.DataFrame()
    for hora in pd.date_range(start=df.index[0].floor('D'), end=df.index[-1].ceil('D'), freq='H', closed='left'):
        df_aux.loc[hora] = Hr_EBAS_NILU(df.loc[df.index.floor('H') == hora].copy())
    df_aux.index.name = 'DateTime'
    df_aux.o3 = df_aux.o3.round(2)
    df_aux[['std']] = df_aux[['std']].round(2)
    df_aux.flag #= df_aux.flag.round(3)
    return df_aux[['o3', 'std', 'flag']]

def Generar_Hr_para_SMN(df):
    df_aux = pd.DataFrame(columns=['O3', 'ND', 'SD', 'F'])
    df_aux.index.name = 'DateTime'
    for hora in pd.date_range(start=df.index[0].floor('D'), end=df.index[-1].ceil('D'), freq='H', closed='left'):
        df_aux.loc[hora] = Hr_Row(df.loc[df.index.floor('H') == hora].copy())
    df_aux['DATE'] = df_aux.index.date
    df_aux['TIME'] = df_aux.index.strftime('%H:%M')
    df_aux.O3 = df_aux.O3.round(2)
    df_aux.SD = df_aux.SD.round(2)
    df_aux.ND = df_aux.ND.astype('int')
    df_aux.F = df_aux.F.astype('int')
    return df_aux[['DATE', 'TIME', 'O3', 'ND', 'SD', 'F']]



def Generar_Da_para_SMN(df):
    df_aux = pd.DataFrame(columns=['O3', 'ND', 'SD', 'F'])
    df_aux.index.name = 'DateTime'
    for Dia in pd.date_range(start=df.index[0].floor('D'), end=df.index[-1].ceil('D'), freq='D', closed='left'):
        df_aux.loc[Dia] = Da_Row(df.loc[df.index.floor('D') == Dia].copy())
    df_aux['DATE'] = df_aux.index.date
    df_aux['TIME'] = df_aux.index.strftime('%H:%M')
    df_aux.O3 = df_aux.O3.round(1)
    df_aux.SD = df_aux.SD.round(2)
    df_aux.ND = df_aux.ND.astype('int')
    df_aux.F = df_aux.F.astype('int')
    return df_aux[['DATE', 'TIME', 'O3', 'ND', 'SD', 'F']]

def Generar_Mo_para_SMN(df):
    df_aux = pd.DataFrame(columns=['O3', 'ND', 'SD', 'F'])
    df_aux.index.name = 'DateTime'
    for Mes in pd.date_range(start=df.index[0].floor('D'), end=df.index[-1].ceil('D'), freq='MS', closed='left'):
        df_aux.loc[Mes + pd.to_timedelta(Mes.daysinmonth/2, unit='D')] = Mo_Row_v2(df.loc[df.index.month == Mes.month].copy())
    df_aux['DATE'] = df_aux.index.date
    df_aux['TIME'] = df_aux.index.strftime('%H:%M')
    df_aux.O3 = df_aux.O3.round(1)
    df_aux.SD = df_aux.SD.round(2)
    df_aux.ND = df_aux.ND.astype('int')
    df_aux.F = df_aux.F.astype('int')
    return df_aux[['DATE', 'TIME', 'O3', 'ND', 'SD', 'F']]

def Hr_EBAS_NILU(datosHora):
    minDatos = 45

    if datosHora.empty:
        O3prom = np.nan
        SD = np.nan
        Flag = [flag_NV]
        return [O3prom, SD, Flag]

    datosHora = datosHora.loc[datosHora.Flag_Manual == 0]
    # Bandera 0.980 Calibracion
    if len(datosHora.loc[datosHora.Flag_Zero != 0]) >= 20:
        O3prom = np.nan
        SD = np.nan
        Flag = [flag_Cal]
        return [O3prom, SD, Flag]

    #Verifico Tener al menos el 50% de los datos (30 datos)
    if len(datosHora.O3.loc[~np.isnan(datosHora['O3'])]) >= 30:
        Datos = datosHora.O3.loc[(datosHora['Flag_Wind'] == 0)]

        # Flag 0.000 con mas del 75%
        if len(Datos) >= minDatos:
            O3prom = Datos.mean() #
            SD = Datos.std()
            Flag = [flag_V_0]
            return [O3prom, SD, Flag]


        #Bandera Flag 0.000 y 0.392 - dato valido pero con datos mayores al 50% y menores al 75%
        if len(Datos) >= 30:
            Datos = datosHora['O3'].loc[datosHora['Flag_Wind'] == 0]
            O3prom = Datos.mean()  #
            SD = Datos.std()
            Flag = [flag_V_0, flag_V_75]
            return [O3prom, SD, Flag]
            # Falta Bandera para no ingresar en dos condiciones seguidas

        # Bandera 0.188, Dato valido pero de viento Bajo
        Datos = datosHora.O3.loc[(datosHora['Flag_Wind'] == 0.188)]
        if len(Datos) >= minDatos:
            O3prom = Datos.mean()  #
            SD = Datos.std()
            Flag = [flag_V_B]
            return [O3prom, SD, Flag]

        # Bandera 0.188 pero con menos del 75%, Dato valido pero de viento Bajo
        if len(Datos) >= 30:
            O3prom = Datos.mean()  #
            SD = Datos.std()
            Flag = [flag_V_B, flag_V_75]
            return [O3prom, SD, Flag]
            # Falta Bandera para no ingresar en dos condiciones seguidas

        #Bandera 0.189, Dato valido pero de viento NO limpio
        Datos = datosHora.O3
        if len(Datos) >= minDatos:
            O3prom = Datos.mean() #
            Flag = [flag_V_NB]
            SD = Datos.std()
            return [O3prom, SD, Flag]
            # Falta Bandera para no ingresar en dos condiciones seguidas



    #Bandera 0.999 Cuando no entra en ninguna de las anteriorse condiciones
    O3prom = np.nan
    SD = np.nan
    Flag = [999]
    return [O3prom, SD, Flag]


def Hr_Row_v2(datosHora):
    minDatos = 41
    Nulo = [-9999999.9, 0, -999.99, 2]

    if datosHora.empty:

        return Nulo
    #primero chequear la longitud 60
    if len(datosHora) != 60:
        print(datosHora.index[0])
        print(menErrorLong)

    datosHora = datosHora.loc[datosHora.Flag_Manual == 0]

    # Bandera 0.980 Calibracion
    if len(datosHora.loc[datosHora.Flag_Zero != 0]) >= 20:
        return Nulo

    #Verifico Tener al menos el 50% de los datos (30 datos)
    if len(datosHora.O3.loc[~np.isnan(datosHora['O3'])]) >= 30:


        Datos = datosHora.O3.loc[(datosHora.Flag_Wind == 0)]

        # Flag 0 con mas del 66%
        if len(Datos) >= minDatos:
            O3prom = Datos.mean() #
            ND = len(Datos)
            SD = Datos.std()
            Flag = 0
            return [O3prom, ND, SD, Flag]
            #Falta Bandera para no ingresar en dos condiciones seguidas

        #Bandera 1, Dato valido pero de viento NO limpio
        Datos = datosHora.O3
        if len(Datos) >= minDatos:
            O3prom = Datos.mean() #
            Flag = 1
            ND = len(Datos)
            SD = Datos.std()
            return [O3prom, ND, SD, Flag]
            # Falta Bandera para no ingresar en dos condiciones seguidas



    #Bandera 0.999 Cuando no entra en ninguna de las anteriorse condiciones

    return Nulo

def Mo_Row_v2(datosHora):
    minDatos = 100 #150 #100
    Nulo = [-9999999.9, 0, -999.99, 2]

    if datosHora.empty:

        return Nulo
    #primero chequear la longitud 60
    #if len(datosHora) != 60:
    #    print(datosHora.index[0])
    #    print(menErrorLong)

    datosHora = datosHora.loc[datosHora.F != 2]

    # Bandera 0.980 Calibracion
    #if len(datosHora.loc[datosHora.Flag_Zero != 0]) >= 20:
    #    return Nulo

    #Verifico Tener al menos el 50% de los datos (30 datos)
    if len(datosHora.O3.loc[~np.isnan(datosHora['O3'])]) >= 300:


        Datos = datosHora.O3.loc[(datosHora.F == 0)]

        # Flag 0 con mas del 66%
        if len(Datos) >= minDatos:
            O3prom = Datos.mean() #
            ND = len(Datos)
            SD = Datos.std()
            Flag = 0
            return [O3prom, ND, SD, Flag]
            #Falta Bandera para no ingresar en dos condiciones seguidas

        #Bandera 1, Dato valido pero de viento NO limpio
        Datos = datosHora.O3
        if len(Datos) >= minDatos:
            O3prom = Datos.mean() #
            Flag = 1
            ND = len(Datos)
            SD = Datos.std()
            return [O3prom, ND, SD, Flag]
            # Falta Bandera para no ingresar en dos condiciones seguidas



    #Bandera 0.999 Cuando no entra en ninguna de las anteriorse condiciones

    return Nulo

def Hr_Row(datosHora):
    minDatos = 41


    if datosHora.empty:
        O3prom = -9999999.9
        ND = 0
        SD = -999.99
        Flag = 2
        return [O3prom, ND, SD, Flag]
    #primero chequear la longitud 60
    if len(datosHora) != 60:
        print(datosHora.index[0])
        print(menErrorLong)

    datosHora = datosHora.loc[datosHora.Flag_Inst]

    # Bandera 0.980 Calibracion
    if len(datosHora.loc[datosHora['Cal_Zero'] == 1]) >= 20:
        O3prom = -9999999.9
        ND = 0
        SD = -999.99
        Flag = 2
        return [O3prom, ND, SD, Flag]

    #Verifico Tener al menos el 50% de los datos (30 datos)
    if len(datosHora.O3.loc[~np.isnan(datosHora['O3'])]) >= 30:
        Datos = datosHora.O3.loc[(datosHora['FlagWind'] == 0)]


        # Flag 0 con mas del 66%
        if len(Datos) >= minDatos:
            O3prom = Datos.mean() #
            ND = len(Datos)
            SD = Datos.std()
            Flag = 0
            return [O3prom, ND, SD, Flag]
            #Falta Bandera para no ingresar en dos condiciones seguidas

        #Bandera 1, Dato valido pero de viento NO limpio
        Datos = datosHora.O3
        if len(Datos) >= minDatos:
            O3prom = Datos.mean() #
            Flag = 1
            ND = len(Datos)
            SD = Datos.std()
            return [O3prom, ND, SD, Flag]
            # Falta Bandera para no ingresar en dos condiciones seguidas



    #Bandera 0.999 Cuando no entra en ninguna de las anteriorse condiciones
    O3prom = -9999999.9
    ND = 0
    SD = -999.99
    Flag = 2
    return [O3prom, ND, SD, Flag]

def Da_Row(datosDia):
    # primero chequear la longitud 24
    if len(datosDia) != 24:
        print(datosDia.index[0])
        print(menErrorLong)
        return

    # Bandera 0 Dato Valido y limpio
    if len(datosDia.loc[datosDia['F'] == 0]) >= 15:
        Datos = datosDia['O3'].loc[(datosDia['F'] == 0)]
        O3prom = Datos.mean()
        ND = len(Datos)
        SD = Datos.std()
        Flag = 0
        return [O3prom, ND, SD, Flag]

    # Bandera 1
    if len(datosDia.loc[(datosDia['F'] == 0) | (datosDia['F'] == 1)]) >= 15:
        Datos = datosDia['O3'].loc[(datosDia['F'] == 0) | (datosDia['F'] == 1)]
        O3prom = Datos.mean()
        ND = len(Datos)
        SD = Datos.std()
        Flag = 1
        return [O3prom, ND, SD, Flag]

    # Bandera 2 Cuando no entra en ninguna de las anteriorse condiciones
    O3prom = -9999999.9
    ND = 0
    SD = -999.99
    Flag = 2
    return [O3prom, ND, SD, Flag]

def Mo_row(datosMes):

    # primero chequear la longitud 60
    if len(datosMes) != datosMes.index.days_in_month._data[0] * 24:
        print(datosMes.index[0])
        print(menErrorLong)
        return

    # Bandera 0 Dato Valido y limpio
    if len(datosMes.loc[datosMes['F'] == 0]) >= datosMes.index.days_in_month._data[0] * 16 : #(16 es 2/3 de 24 horas)
        Datos = datosMes['O3'].loc[(datosMes['F'] == 0)]
        O3prom = Datos.mean()
        ND = len(Datos)
        SD = Datos.std()
        Flag = 0
        return [O3prom, ND, SD, Flag]

    # Bandera 1
    if len(datosMes.loc[(datosMes['F'] == 0) | (datosMes['F'] == 1)]) >= datosMes.index.days_in_month._data[0] * 16:
        Datos = datosMes['O3'].loc[(datosMes['F'] == 0) | (datosMes['F'] == 1)]
        O3prom = Datos.mean()
        ND = len(Datos)
        SD = Datos.std()
        Flag = 1
        return [O3prom, ND, SD, Flag]

    # Bandera 2 Cuando no entra en ninguna de las anteriorse condiciones
    O3prom = -9999999.9
    ND = 0
    SD = -999.99
    Flag = 2
    return [O3prom, ND, SD, Flag]
