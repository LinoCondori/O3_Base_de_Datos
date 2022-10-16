import pandas as pd
import numpy as np
from glob import glob
import os
import matplotlib.pyplot as plt
from sqlalchemy import create_engine
import BaseDeDatos_Lib_v02 as BD

engine = create_engine('postgresql://postgres:vag@10.30.19.227:5432/GAWUSH_DATABASE')
Tabla_Data = 'O3_SN_49C-58546-318'
Tabla_Coef = 'O3_SN_49C-58546-318_Coeficientes'
Tabla_Zero = 'O3_Calibraciones'
inicio = pd.to_datetime('2022-01-01 00:00')
fin = pd.to_datetime('2022-10-01 00:00')

engine_Flag_Manual = create_engine('postgresql://postgres:vag@10.30.19.227:5432/GAWUSH_PROCESADOS')
Tabla_Flag_Manual = 'O3_SN_49C-58546-318_Minutal_Flag_Manual'

engine_Final = create_engine('postgresql://postgres:vag@10.30.19.227:5432/GAWUSH_PROCESADOS')
Tabla_Data_Final = 'O3_SN_49C-58546-318_Minutal'

def CalculoCoeficientesDiarios(data, cal):
    cal = cal.reindex(data.index)
    df_coef = pd.DataFrame(columns=['O3_Coef_m','O3_Coef_b'])
    for dia in pd.date_range(data.index[0].floor('D'), data.index[-1].ceil('D'), freq='D', closed='left'):
        dia_cal = data.reindex(cal.DateTime.loc[(cal.index.floor('D') == dia) & (cal.Flag_Zero == 2) ])
        #Tener en cuenta que no se hayan filtrado los datos de Zero, es decir
        #que no se haya disernido entre estable y transitorio
        dia_cal = dia_cal.loc[np.abs(dia_cal.O3) < 1.5]
        try:
            df_coef.loc[dia_cal.DateTime.iat[0].floor('H')] = [1, -dia_cal.O3.mean()]
        except:
            print(dia)

    return df_coef


if __name__ == '__main__':
    O3_Raw = BD.buscarEnBaseDeDatos(engine,Tabla_Data, inicio, fin)
    O3_FlM = BD.buscarEnBaseDeDatos(engine_Flag_Manual,Tabla_Flag_Manual, inicio, fin)
    O3_Cal = BD.buscarEnBaseDeDatos(engine,Tabla_Zero, inicio, fin)
    #Filtro por flag manual
    O3_FlM.Flag_Manual.fillna(0, inplace=True)
    O3_Raw = O3_Raw.loc[O3_FlM.Flag_Manual==0]
    #igualar tamaños de DF
    O3_Cal = O3_Cal.reindex(O3_Raw.index)
    #Calcular dia por dia los coeficientes. en este caso solo sera el desplazamiento vertical.
    O3_Coeficientes = CalculoCoeficientesDiarios(O3_Raw, O3_Cal)

    #Guardar los coeficientes
    O3_Coeficientes.index.name = 'DateTime'
    #BD.Consulta_de_Existencia_Y_Envio_General(O3_Coeficientes.rename(columns={'O3_Coef_m':'m' ,'O3_Coef_b':'b'}),
    #                                         engine, Tabla_Coef)
    O3_Coeficientes.to_csv('Coeficientes_' + Tabla_Data + '_' + O3_Raw.DateTime[0]._date_repr + '_to_' +
                           O3_Raw.DateTime[-1]._date_repr + '.csv', index_label='DateTime')
    #MEJORAR LA TOMA DEL AÑO ANTERIOR
    #Calcular de acuerdo a la frecuencia determinada.
    # Genero para cada minuto el coeficiente que le corresponde, teniendo en cuenta que los valores se corrigen con los
    # coeficientes de una calibracion anterior.
    O3_Coeficientes = O3_Coeficientes.reindex(O3_Raw.index, method='ffill')
    # Las mediciones antes de la primera calibracion las corrijo con los coeficientes de la primera calibracion
    O3_Coeficientes = O3_Coeficientes.fillna(O3_Coeficientes.loc[~np.isnan(O3_Coeficientes.O3_Coef_b)].iloc[0])

    #Correccion por coeficientes.
    O3_Paso_1 = pd.concat([O3_Raw, O3_Cal.Flag_Zero], axis=1)
    O3_Paso_1.O3 = O3_Paso_1.O3 * O3_Coeficientes.O3_Coef_m + O3_Coeficientes.O3_Coef_b
    O3_Paso_1.O3 = O3_Paso_1.O3.round(4)
    #O3_Paso_1 = pd.concat([O3_Paso_1.O3, O3_Cal.Flag_Zero], axis=1)
    #O3_Paso_1['Flag_Manual'] = 0
    O3_FlM = O3_FlM.reindex(O3_Paso_1.index)


    axes = plt.subplot(311)
    axes.set_xlim([inicio, fin])
    plt.scatter(O3_Paso_1.DateTime.loc[(O3_FlM.Flag_Manual == 0) & (O3_Cal.Flag_Zero == 0)],
                O3_Paso_1.O3.loc[(O3_FlM.Flag_Manual == 0) & (O3_Cal.Flag_Zero == 0)], c='grey', s=100)
    plt.scatter(O3_Paso_1.DateTime.loc[(O3_Paso_1.Flags != '1c000000')],
                O3_Paso_1.O3.loc[(O3_Paso_1.Flags != '1c000000')], c='red', s=50)

    axes = plt.subplot(312)
    axes.set_xlim([inicio, fin])
    plt.scatter(O3_Paso_1.DateTime.loc[(O3_Paso_1.Flag_Zero != 0) & (np.abs(O3_Paso_1.O3) <1.5)], O3_Paso_1.O3.loc[(O3_Paso_1.Flag_Zero != 0) & (np.abs(O3_Paso_1.O3)<1.5)], s=100, alpha=0.1)

    axes = plt.subplot(313)
    axes.set_xlim([inicio, fin])
    axes.set_ylim([0, 1])
    plt.scatter(O3_Coeficientes.index, -O3_Coeficientes.O3_Coef_b,)

    plt.show()
    #BD.Consulta_de_Existencia_Y_Envio_DIAxDIA(O3_Paso_1.drop(['DateTime'], axis=1), engine_Final, Tabla_Data_Final)
