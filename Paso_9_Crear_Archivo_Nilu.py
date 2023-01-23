import pandas as pd
import numpy as np
from glob import glob
import os
import matplotlib.pyplot as plt
from sqlalchemy import create_engine
import BaseDeDatos_Lib_v02 as BD
import O3_Nilu_Programas_v0_02 as NL
from ast import literal_eval

engine = create_engine('postgresql://postgres:vag@10.30.19.227:5432/GAWUSH_PROCESADOS')
engine_Flag_Manual = create_engine('postgresql://postgres:vag@10.30.19.227:5432/GAWUSH_PROCESADOS')
Tabla_EBAS_NILU = 'o3_ush_ebas_nilu'

inicio = pd.to_datetime('2022-01-01 00:00')
fin = pd.to_datetime('2023-01-01 00:00')

from ebas.io.file import nasa_ames
from nilutility.datatypes import DataObject
from ebas.domain.basic_domain_logic.time_period import estimate_period_code, \
    estimate_resolution_code, estimate_sample_duration_code
import datetime
import pandas as pd

__version__ = '1.00.00'


def set_fileglobal_metadata(nas):
    """
    Colocar los medatos Globales

    Parameters:
        nas    EbasNasaAmes file object
    Returns:
        None
    """
    # Siempre en UTC
    nas.metadata.timezone = 'UTC'

    # Revision information
    nas.metadata.revdate = datetime.datetime(2023, 1, 18, 12, 00, 00)
    nas.metadata.revision = '1.0'
    nas.metadata.revdesc = 'Initial revision'

    # Data Originator Organisation
    nas.metadata.org = DataObject(
        OR_CODE='AR01L',
        OR_NAME='Servicio Meteorologico Nacional',
        OR_ACRONYM='SMNA', OR_UNIT='Research and development department',
        OR_ADDR_LINE1='Dorrego 4019', OR_ADDR_LINE2=None,
        OR_ADDR_ZIP='1425', OR_ADDR_CITY='CABA', OR_ADDR_COUNTRY='Argentina')

    # Projects the data are associated to
    nas.metadata.projects = ['GAW-WDCGG-node']

    # Data Originators (PIs)
    nas.metadata.originator = []
    nas.metadata.originator.append(
        DataObject(
            PS_LAST_NAME='Condori', PS_FIRST_NAME='Lino', PS_EMAIL='lcondori@smn.gob.ar',
            PS_ORG_NAME='Servicio Meteorologico Nacional',
            PS_ORG_ACR='SMNA', PS_ORG_UNIT='Research and development department',
            PS_ADDR_LINE1='Dorrego 4019', PS_ADDR_LINE2=None,
            PS_ADDR_ZIP='1425', PS_ADDR_CITY='CABA',
            PS_ADDR_COUNTRY='Argentina',
            PS_ORCID=None,
        ))
    nas.metadata.originator.append(
        DataObject(
            PS_LAST_NAME='Barlasina', PS_FIRST_NAME='Maria', PS_EMAIL='barlasina@smn.gob.ar',
            PS_ORG_NAME='Servicio Meteorologico Nacional',
            PS_ORG_ACR='SMNA', PS_ORG_UNIT='Research and development department',
            PS_ADDR_LINE1='Dorrego 4019', PS_ADDR_LINE2=None,
            PS_ADDR_ZIP='1425', PS_ADDR_CITY='CABA',
            PS_ADDR_COUNTRY='Argentina',
            PS_ORCID=None,
        ))

    # Data Submitters (contact for data technical issues)
    nas.metadata.submitter = []
    nas.metadata.submitter.append(
        DataObject(
            PS_LAST_NAME='Condori', PS_FIRST_NAME='Lino', PS_EMAIL='lcondori@smn.gob.ar',
            PS_ORG_NAME='Servicio Meteorologico Nacional',
            PS_ORG_ACR='SMNA', PS_ORG_UNIT='Research and development department',
            PS_ADDR_LINE1='Dorrego 4019', PS_ADDR_LINE2=None,
            PS_ADDR_ZIP='1425', PS_ADDR_CITY='CABA',
            PS_ADDR_COUNTRY='Argentina',
            PS_ORCID=None,
        ))

    # Station metadata
    nas.metadata.station_code = 'AR0002G'
    nas.metadata.platform_code = 'AR0002S'
    nas.metadata.station_name = 'Ushuaia'

    nas.metadata.station_wdca_id = None  # 'GAWANO__ZEP'
    nas.metadata.station_gaw_id = 'USH'
    nas.metadata.station_gaw_name = 'Ushuaia'
    # nas.metadata.station_airs_id =    # N/A
    nas.metadata.station_other_ids = None  # '721 (NILUDB)'
    # nas.metadata.station_state_code =  # N/A
    nas.metadata.station_landuse = 'Airport'
    nas.metadata.station_setting = 'Coastal'
    nas.metadata.station_gaw_type = 'G'
    nas.metadata.station_wmo_region = 3
    nas.metadata.station_latitude = -54.848465
    nas.metadata.station_longitude = -68.310692
    nas.metadata.station_altitude = 18.0

    # More file global metadata, but those can be overridden per variable
    # See set_variables for examples
    nas.metadata.instr_type = 'uv_abs'
    nas.metadata.lab_code = 'AR01L'
    nas.metadata.instr_name = 'uv_abs_USH3'
    nas.metadata.method = 'AR01L_uv_abs'
    nas.metadata.regime = 'IMG'
    nas.metadata.matrix = 'air'
    nas.metadata.instr_manufacturer = 'Thermo'
    nas.metadata.instr_model = '49C'
    nas.metadata.instr_serialno = '58546-318'
    nas.metadata.std_method = 'SOP=GAW_209(2013)'
    nas.metadata.inlet_type = 'Downward-facing tube'
    nas.metadata.inlet_desc = '7 m above ground on top of the station building, 2 m above the roof * 19 mm PFA  tubing - length=6 m up to the instrument - aditional pump to increase flow rate - residence time < 5 s'
    nas.metadata.inlet_tube_material = 'Teflon'
    nas.metadata.inlet_tube_outerD = 19
    nas.metadata.inlet_tube_innerD = 15
    nas.metadata.inlet_tube_length = 6
    nas.metadata.maintenance_desc = 'Monthly leak test and UV lamp test, inlet filter checked every 1 months,  Manual Zero and Spam check every 3 months'
    nas.metadata.flow_rate = 12
    nas.metadata.zero_span_type = 'automatic'
    nas.metadata.zero_span_interval = '1d'
    nas.metadata.vol_std_temp = 'ambient'
    nas.metadata.vol_std_pressure = 'ambient'
    nas.metadata.detection_limit = [1, 'nmol/mol']
    nas.metadata.abs_cross_section = 'Hearn, 1961' #https://ebas-submit.nilu.no/templates/comments/absorption_cross_section
    # nas.metadata.comp_name   will be set on variable level
    # nas.metadata.unit        will be set on variable level
    # nas.metadata.statistics = 'arithmetic mean'
    nas.metadata.datalevel = '2'
    nas.metadata.mea_altitude = 19.0
    nas.metadata.mea_height = 7

    nas.metadata.qa = []
    nas.metadata.qa.append(
        DataObject({
            'qa_number': 6,
            'qm_id': 'WCC Empa Site Audit',
            'qa_date': datetime.datetime(2019, 11, 13),
            'qa_desc': 'on site comparison with EMPA-WCC standard instrument',
            'qa_outcome': 'forthcoming'
        }))


def set_time_axes(nas):
    """
    Set the time axes and related metadata for the EbasNasaAmes file object.

    Parameters:
        nas    EbasNasaAmes file object
    Returns:
        None
    """
    # define start and end times for all samples
    # nas.sample_times = \
    #    [(datetime.datetime(2014, 1, 1, 11, 0), datetime.datetime(2014, 3, 7, 13, 22)),
    #     (datetime.datetime(2014, 3, 7, 13, 57), datetime.datetime(2014, 6, 12, 11, 32)),
    #     (datetime.datetime(2014, 6, 12, 11, 47), datetime.datetime(2014, 9, 15, 17, 3)),
    #     (datetime.datetime(2014, 9, 15, 17, 3), datetime.datetime(2014, 12, 15, 14, 42))]
    nas.sample_times = DateTimeLista
    #
    # Generate metadata that are related to the time axes:
    #

    # period code is an estimate of the current submissions period, so it should
    # always be calculated from the actual time axes, like this:
    nas.metadata.period = estimate_period_code(nas.sample_times[0][0],
                                               nas.sample_times[-1][1])

    # Sample duration can be set automatically
    nas.metadata.duration = estimate_sample_duration_code(nas.sample_times)
    # or set it hardcoded:
    # nas.metadata.duration = '3mo'

    # Resolution code can be set automatically
    # But be aware that resolution code is an identifying metadata element.
    # That means, several submissions of data (multiple years) will
    # only be stored as the same dataset if the resolution code is the same
    # for all submissions!
    # That might be a problem for time series with varying resolution code
    # (sometimes 2 months, sometimes 3 months, sometimes 9 weeks, ...). You
    # might consider using a fixed resolution code for those time series.
    # Automatic calculation (will work from ebas.io V.3.0.7):
    nas.metadata.resolution = estimate_resolution_code(nas.sample_times)
    # or set it hardcoded:
    # nas.metadata.resolution = '3mo'

    # It's a good practice to use Jan 1st of the year of the first sample
    # endtime as the file reference date (zero point of time axes).
    nas.metadata.reference_date = \
        datetime.datetime(nas.sample_times[0][1].year, 1, 1)
    nas.metadata.rescode_sample = '1mn'


def set_variables(nas):
    """
    Set metadata and data for all variables for the EbasNasaAmes file object.

    Parameters:
        nas    EbasNasaAmes file object
    Returns:
        None

    # variable 1: ejemplo con valores faltantes y banderas
    values = [1.22, 2.33, None, 4.55]   # el valor faltante es None
    flags = [[], [632, 665], [999], []]
    # [] Significa que no hay bandaeras para esa medicion
    # [999] missing or invalid flag needed because of missing value (None)
    # [632, 665] Es posible poner multiples banderas a una medicion
    metadata = DataObject()
    metadata.comp_name = 'HCB'
    metadata.unit = 'pg/m3'
    # alternatively, you could set all metadata at once:
    # metadata = DataObject(comp_name='HCB', unit = 'pg/m3')
    nas.variables.append(DataObject(values_=values, flags=flags, flagcol=True,
                                    metadata=metadata))
    # variable 2: examples for overridden metadata, uncertainty and detection
    # limit
    values = [1.22, 2.33, 3.44, 4.55]
    flags = [[], [], [], []]
    metadata = DataObject()
    metadata.comp_name = 'benz_a_anthracene'
    metadata.unit = 'ng/m3'
    # matrix is different for this variable. Generally, you can override most
    # elements of nas.metadata on a per-variable basis by just setting the
    # according nas.variables[i].metadata element.
    metadata.matrix = 'air+aerosol'
    # additionally, we also specify uncertainty and detection limit for this
    # variable:
    metadata.detection_limit = [0.10, 'ng/m3']
    # detection limit unit must always be the same as the variable's unit!
    metadata.uncertainty = [0.12, 'ng/m3']
    # uncertainty unit is either the same as the variable's unit, ot '%' for
    # relative uncertainty:
    # metadata.uncertainty = [10.0, '%']
    nas.variables.append(DataObject(values_=values, flags=flags, flagcol=True,
                                    metadata=metadata))


    # variable 3: uncertainty will be specified for each sample (see variable 4)
    values = [1.22, 2.33, 3.44, 4.55]
    flags = [[], [], [], []]
    metadata = DataObject()
    metadata.comp_name = 'PCB_101'
    metadata.unit = 'pg/m3'
    nas.variables.append(DataObject(values_=values, flags=flags, flagcol=True,
                                    metadata=metadata))

    # variable 4: this variable contains the uncertainties for varable 3
    values = [0.22, 0.33, 0.44, 0.55]
    flags = [[], [], [], []]
    metadata = DataObject()
    metadata.comp_name = 'PCB_101'
    metadata.unit = 'pg/m3'
    # this is what makes this variable the uncetainty time series:
    metadata.statistics = 'uncertainty'
    nas.variables.append(DataObject(values_=values, flags=flags, flagcol=True,
                                    metadata=metadata))
                                    """
    # variable 1:
    values = O3Lista
    flags = FlagLista
    metadata = DataObject()
    metadata.comp_name = 'ozone'
    metadata.unit = 'nmol/mol'
    metadata.matrix = 'air'
    # metadata.statistics = 'arithmetic mean'
    nas.variables.append(DataObject(values_=values, flags=flags, flagcol=False,
                                    metadata=metadata))
    # variable 2:
    values = SDLista
    flags = FlagLista
    metadata = DataObject()
    metadata.comp_name = 'ozone'
    metadata.unit = 'nmol/mol'
    metadata.matrix = 'air'
    metadata.statistics = 'stddev'
    nas.variables.append(DataObject(values_=values, flags=flags, flagcol=False,
                                    metadata=metadata))


def ebas_genfile():
    """
    Main program for ebas_flatcsv
    Created for lexical scoping.

    Parameters:
        None
    Returns:
        none
    """

    # Create an EbasNasaAmes file object
    nas = nasa_ames.EbasNasaAmes()

    # Coloca los Metadatos Globales
    set_fileglobal_metadata(nas)

    # Set the time axes and related metadata
    set_time_axes(nas)

    # Set metadata and data for all variables
    set_variables(nas)

    # write the file:
    nas.write(createfiles=True, flags=1)
    # createfiles=True
    #     Actually creates output files, else the output would go to STDOUT.
    # You can also specify:
    #     destdir='path/to/directory'
    #         Specify a specific relative or absolute path to a directory the
    #         files should be written to
    #     flags=FLAGS_COMPRESS
    #         Compresses the file size by reducing flag columns.
    #         Flag columns will be less explicit and thus less intuitive for
    #         humans to read.
    #     flags=FLAGS_ALL
    #         Always generate one flag column per variable. Very intuitive to
    #         read, but increases filesize.
    #     The default for flags is: Generate one flag column per file if the
    #     flags are the same for all variables in the file. Else generate one
    #     flag column per variable.
    #     This is a trade-off between the advantages and disadvantages of the
    #     above mentioned approaches.
    print('Listo')


if __name__ == '__main__':

    DatosNilu = BD.buscarEnBaseDeDatos(engine,Tabla_EBAS_NILU, inicio, fin)

    DatosNilu = DatosNilu.replace({np.nan: None})
    DatosNilu.DateTime = pd.to_datetime(DatosNilu.DateTime)
    DatosNilu['DateTimeMasUno'] = DatosNilu.DateTime + pd.to_timedelta(1, unit='h')

    DateTime = DatosNilu.DateTime.dt.to_pydatetime()
    DateTimeMasUno = DatosNilu.DateTimeMasUno.dt.to_pydatetime()

    #DatosNilu.flag = DatosNilu.flag.astype(str)
    #DatosNilu.flag = DatosNilu.flag.apply(literal_eval)
    DateTimeLista = np.stack((DateTime, DateTimeMasUno), axis=-1)

    O3Lista = DatosNilu.o3.tolist()
    SDLista = DatosNilu['std'].tolist()
    FlagLista = DatosNilu.flag.tolist()

    ebas_genfile()


    """  
    axes = plt.subplot(311)
    axes.set_xlim([inicio, fin])
    axes.set_ylim([0, 40])
    plt.xlabel('Tiempo')
    plt.ylabel('O3 [ppb]')
    lst2 = [item[0] for item in O3_EBAS_NILU.flag.values]
    FLAG_VALID_FULL = np.array(lst2) ==  np.array([0])
    FLAG_VALID_BAJO = np.array(lst2) == np.array([188])
    #plt.scatter(O3_Minutal.index, O3_Minutal.O3, c='grey', s=100)
    plt.scatter(O3_EBAS_NILU.loc[FLAG_VALID_FULL].index,
                O3_EBAS_NILU.o3.loc[FLAG_VALID_FULL], c='blue', s=10, alpha=0.5)

    axes = plt.subplot(312)
    #plt.scatter(O3_Minutal.index, O3_Minutal.O3, c='grey', s=100)
    plt.scatter(O3_EBAS_NILU.loc[FLAG_VALID_BAJO].index,
                O3_EBAS_NILU.o3.loc[FLAG_VALID_BAJO], c='blue', s=10, alpha=0.5)
    axes = plt.subplot(313)
 
    plt.show()
    """