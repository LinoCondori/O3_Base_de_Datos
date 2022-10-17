import pandas as pd
import numpy as np
from glob import glob
import os
import matplotlib.pyplot as plt
from sqlalchemy import create_engine
import BaseDeDatos_Lib_v02 as BD


import psycopg2
import sys


sql = """ UPDATE "O3_SN_49C-58546-318_Minutal_Flag_Manual"
            SET "Flag_Manual" = %s
            WHERE "DateTime" =  %s"""
sql2 = 'UPDATE "O3_SN_49C-58546-318_Minutal_Flag_Manual" SET "Flag_Manual" = %s WHERE "DateTime">= %s and "DateTime"<= %s' % (3, "'2020-01-01 00:00'", "'2020-01-01 00:05'")


def quitar_segun_index(df, engine, tabla):
    if df.empty:
        return 0
    conn = None
    updated_rows = 0
    for dia in pd.date_range(min(df.index.floor('D')), max(df.index.floor('D')), freq='D'):
        df.loc[dia.floor('D') == df.index.floor('D')].to_sql('temp_table', engine, if_exists='append')

    sql = """
         UPDATE "O3_SN_49C-58546-318_Minutal_Flag_Manual" AS f
         SET "Flag_Manual" = h."Flag_Manual"
         FROM temp_table AS h
         WHERE f."DateTime" = h."DateTime";
            """


    try:
        with engine.begin() as conn:  # TRANSACTION
            conn.execute(sql)
            conn.execute("DROP TABLE temp_table")
            #updated_rows = cur.rowcount
        # read database configuration
        #params = config(engine)
        #engine = psycopg2.connect(**params)
        #params = config()
        # connect to the PostgreSQL database postgresql://postgres:vag@10.30.19.227:5432/GAWUSH_PROCESADOS
        #conn = engine.connect()
        #conn = psycopg2.connect(**params)
        # create a new cursor
        #cur = conn.cursor()
        # execute the UPDATE  statement
        #conn.execute(drop)




        # get the number of updated rows
        #updated_rows = cur.rowcount
        # Commit the changes to the database
        #conn.commit()
        # Close communication with the PostgreSQL database
        #cur.close()
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
    finally:
        if conn is not None:
            conn.close()

    return updated_rows



inicio = pd.to_datetime('2022-01-01 00:00')
fin = pd.to_datetime('2022-10-01 00:00')

engine_Flag_Manual = create_engine('postgresql://postgres:vag@10.30.19.227:5432/GAWUSH_PROCESADOS') #create_engine('postgresql://:procesado@10.30.19.227:5432/GAWUSH_PROCESADOS')
Tabla_Flag_Manual = 'O3_Minutal_Flag_Manual'
engine = create_engine('postgresql://postgres:vag@10.30.19.227:5432/GAWUSH_PROCESADOS')
Tabla_Data = 'O3_SN_0330102717_Minutal'


if __name__ == '__main__':
    O3_Flag_Manual = BD.buscarEnBaseDeDatos(engine_Flag_Manual, Tabla_Flag_Manual, inicio, fin)
    O3_Minutal = BD.buscarEnBaseDeDatos(engine, Tabla_Data, inicio, fin)
    index_ = pd.date_range(inicio, fin, freq='T', closed='left')
    O3_Flag_Manual = O3_Flag_Manual.reindex(index_)
    O3_Flag_Manual.drop('DateTime', axis=1, inplace=True)
    #O3_Flag_Manual.DateTime = O3_Flag_Manual.index
    O3_Flag_Manual.index.name = 'DateTime'
    O3_Flag_Manual_Original = O3_Flag_Manual.copy()

    #Llenar lo nuevo (que esta en NAN) con CERO

    O3_Flag_Manual.Flag_Manual = 0
    #Leer csv Flag_MANUAL
    O3_Log_Book = pd.read_csv('O3_Flag_Manual.csv')
    O3_Log_Book.DateTimeInicio = pd.to_datetime(O3_Log_Book.DateTimeInicio)
    O3_Log_Book.DateTimeFin_Inclusive = pd.to_datetime(O3_Log_Book.DateTimeFin_Inclusive)

    for index, row in O3_Log_Book.iterrows():
        O3_Flag_Manual.Flag_Manual.loc[row.DateTimeInicio:row.DateTimeFin_Inclusive] = row.Flag_Manual

    O3_Minutal['Flag_Manual'] = np.nan
    O3_Minutal.Flag_Manual.update(O3_Flag_Manual.Flag_Manual)
    NO_NORMAL = O3_Minutal.Flags != '1c100000'
    NO_REGISTRADOS = NO_NORMAL & (O3_Minutal.Flag_Manual == 0)
    REGISTRADOS = NO_NORMAL & (O3_Minutal.Flag_Manual != 0)
    axes = plt.subplot(311)
    axes.set_xlim([inicio, fin])
    axes.set_ylim([0, 40])

    plt.scatter(O3_Minutal.DateTime, O3_Minutal.O3, c='grey', s=100)
    plt.scatter(O3_Minutal.DateTime.loc[NO_NORMAL],
                O3_Minutal.O3.loc[NO_NORMAL], c='brown', s=10, alpha=0.1)
    """
    plt.scatter(O3_Paso_2.DateTime.loc[(O3_Paso_2.Flag_Wind == 1189) & (O3_Paso_2.Flag_Zero == 0)],
                O3_Paso_2.O3.loc[(O3_Paso_2.Flag_Wind == 1189) & (O3_Paso_2.Flag_Zero == 0)], c='green', s=10,
                alpha=0.1)
    plt.scatter(O3_Paso_2.DateTime.loc[(O3_Paso_2.Flag_Wind == 1188) & (O3_Paso_2.Flag_Zero == 0)],
                O3_Paso_2.O3.loc[(O3_Paso_2.Flag_Wind == 1188) & (O3_Paso_2.Flag_Zero == 0)], c='red', s=2, alpha=0.1)"""

    axes = plt.subplot(312)
    axes.set_title('Viento (Rapidez) [m/s]')
    axes.set_xlim([inicio, fin])
    #axes.get_xaxis().set_visible(False)
    plt.plot(O3_Minutal.DateTime.loc[NO_REGISTRADOS], O3_Minutal.O3.loc[NO_REGISTRADOS], c='red', linestyle='', marker='.', )

    axes = plt.subplot(313)
    axes.set_title('Viendo (Direccion) [Grados]')
    axes.set_xlim([inicio, fin])
    plt.plot(O3_Minutal.DateTime.loc[REGISTRADOS], O3_Minutal.O3.loc[REGISTRADOS], c='green', linestyle='', marker='.', )

    plt.show()
    BD.Consulta_de_Existencia_Y_Envio_DIAxDIA(O3_Flag_Manual, engine_Flag_Manual, Tabla_Flag_Manual)
    #O3_UPDATE = O3_Flag_Manual.loc[O3_Flag_Manual.Flag_Manual != O3_Flag_Manual_Original.Flag_Manual]
    #quitar_segun_index(O3_UPDATE, engine_Flag_Manual, Tabla_Flag_Manual)
    #BD.Consulta_de_Existencia_Y_Envio_DIAxDIA(O3_UPDATE, engine_Flag_Manual, Tabla_Flag_Manual)