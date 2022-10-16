import pandas as pd
import numpy as np
from sqlalchemy import create_engine
#Este Script solo realiza un archivo con los horarios de calibracion de cada dia
import BaseDeDatos_Lib_v02 as BD
import matplotlib.pyplot as plt


engine = create_engine('postgresql://postgres:vag@10.30.19.227:5432/GAWUSH_DATABASE')
Tabla_Zero = 'O3_Calibraciones'
Tabla_Data = 'O3_SN_49C-58546-318'

inicio = pd.to_datetime('2022-01-01 00:00')
fin = pd.to_datetime('2022-08-01 00:00')

def Generar_Excel_Zero(ini, fin, file):
    Cal = pd.read_excel(file)
    Cal.Fecha[Cal.Hora.isna()]  # Filtro para Buscar Horas sin datos.

    # Cargar lecturas
    # recorrer por dia#
    for dia in Cal.Fecha[Cal.Hora.isna()]:
        O3_Raw = BD.buscarEnBaseDeDatos(engine, Tabla_Data, dia, dia + pd.to_timedelta(1, unit='d'))
        if len(O3_Raw.Flags.loc[O3_Raw.Flags == '1d000000']) > 30:
            Cal.Hora.loc[Cal.Fecha == dia] = O3_Raw.loc[O3_Raw.Flags == '1d000000'].index.floor('H')[
                0]._time_repr  # See the caveats in the documentation: https://pandas.pydata.org/pandas-docs/stable/user_guide/indexing.html#returning-a-view-versus-a-copy
            print(Cal[Cal.Fecha == dia])
    # encontrar en la bandera el zero
    # Buscar los dias que no tengan bandera

    Cal.to_excel(file)
def RangoTransitorio(datetime):
    range1 = pd.date_range(start=datetime, periods=5, freq='min')
    range2 = pd.date_range(start=datetime + pd.to_timedelta(55, 'min'), periods=5, freq='min')
    return np.append(range1, range2)

def RangoCalibracion(datetime):
    range = pd.date_range(start=datetime + pd.to_timedelta(5, 'min'), periods=50, freq='min')
    return range

def Cargar_Banderas_Calibracion(file, eng, tab):
    PDfile = pd.read_excel(file)
    DFCalibraciones = PDfile.loc[~PDfile.Hora.isna()]
    BDCal = pd.DataFrame()

    BDCal['DateTime'] = pd.date_range(start=pd.to_datetime(PDfile.Fecha.iloc[0] + pd.to_timedelta(00)), end=PDfile.Fecha.iloc[-1] + pd.to_timedelta(1439, 'min'), freq='T')
    #https://www.geeksforgeeks.org/convert-series-of-lists-to-one-series-in-pandas/
    RT = DFCalibraciones.apply(lambda x: RangoTransitorio(pd.to_datetime(x.Fecha + pd.to_timedelta(x.Hora))), axis=1)
    RT = RT.apply(pd.Series).stack().reset_index(drop=True)
    RC = DFCalibraciones.apply(lambda x: RangoCalibracion(pd.to_datetime(x.Fecha + pd.to_timedelta(x.Hora))), axis=1)
    RC = RC.apply(pd.Series).stack().reset_index(drop=True)

    BDCal['Flag_Zero'] = np.nan
    BDCal.loc[BDCal.DateTime.isin(RT), ('Flag_Zero')] = 2
    BDCal.loc[BDCal.DateTime.isin(RC), ('Flag_Zero')] = 1
    BDCal.Flag_Zero.fillna(0, inplace=True)
    BDCal.set_index('DateTime', inplace=True)
    BD.Consulta_de_Existencia_Y_Envio_General(BDCal, eng, tab)






if __name__ == '__main__':
    #Generar_Excel_Zero(inicio, fin, 'Zero_2022.xls')
    Cargar_Banderas_Calibracion('Zero_2022.xls', engine, Tabla_Zero)

