"""
Glue Job — Cronograma de vacaciones y ausentismo
================================================

Construye un tablón analítico que cruza el maestro de empleados con sus
ausentismos del año en curso, para alimentar el cronograma de vacaciones.

Flujo:
  1. Lee las tablas fuente desde el catálogo de Glue como DynamicFrames.
  2. Filtra registros anulados y aplica el esquema con ApplyMapping.
  3. Se queda con el vínculo más reciente de cada empleado (cuenta y
     servicio) usando row_number() sobre una ventana por empleado.
  4. Arma el maestro de datos personales con LEFT JOINs sobre las
     dimensiones (cuenta, subárea, servicio, cargo).
  5. Agrega los ausentismos pendientes de control por empleado y mes.
  6. Borra las particiones y los parquet existentes en S3 antes de
     escribir, para que el job sea idempotente.
  7. Escribe el resultado en S3 en formato parquet con compresión snappy.

Todas las bases, tablas y rutas llegan como parámetros del job; no hay
identificadores de entorno embebidos en el código.
"""

import sys
from awsglue import DynamicFrame
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql.functions import *
from datetime import date, datetime, timedelta
from botocore.exceptions import ClientError
import boto3
import pytz

from pyspark.sql import Window
import pyspark.sql.functions as F

args = getResolvedOptions(sys.argv, ["JOB_NAME", 'AWS_REGION', 'SOURCE_DATABASE','TARGET_DATABASE_ANALYTICS','SOURCE_TABLE_EMPLEADO','SOURCE_TABLE_EMPLEADOCUENTA',
                                'SOURCE_TABLE_CUENTA','SOURCE_TABLE_SUBTAREA','SOURCE_TABLE_EMPLEADOSERVICIO','SOURCE_TABLE_SERVICIOS',
                                'SOURCE_TABLE_CARGO','SOURCE_TABLE_VACACIONESCABECERA','SOURCE_TABLE_VACACIONESDETALLE','SOURCE_TABLE_EMPLEADO_ERP','SOURCE_TABLE_CONTROLVP','SOURCE_TABLE_AUSENTISMO','TARGET_BUCKET',
                                'TARGET_DATABASE','TARGET_TABLE_CRONOGRAMAVACACIONES','PARAMETER_DAYS'])
    
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)
AWS_REGION = args['AWS_REGION']

SOURCE_DATABASE = args["SOURCE_DATABASE"]
TARGET_DATABASE_ANALYTICS = args["TARGET_DATABASE_ANALYTICS"]
SOURCE_TABLE_EMPLEADO = args["SOURCE_TABLE_EMPLEADO"]
SOURCE_TABLE_EMPLEADOCUENTA = args["SOURCE_TABLE_EMPLEADOCUENTA"]
SOURCE_TABLE_CUENTA = args["SOURCE_TABLE_CUENTA"]
SOURCE_TABLE_SUBTAREA = args["SOURCE_TABLE_SUBTAREA"]
SOURCE_TABLE_EMPLEADOSERVICIO = args["SOURCE_TABLE_EMPLEADOSERVICIO"]
SOURCE_TABLE_SERVICIOS = args["SOURCE_TABLE_SERVICIOS"]
SOURCE_TABLE_CARGO = args["SOURCE_TABLE_CARGO"]
SOURCE_TABLE_VACACIONESCABECERA = args["SOURCE_TABLE_VACACIONESCABECERA"]
SOURCE_TABLE_VACACIONESDETALLE = args["SOURCE_TABLE_VACACIONESDETALLE"]
SOURCE_TABLE_EMPLEADO_ERP = args["SOURCE_TABLE_EMPLEADO_ERP"]
SOURCE_TABLE_CONTROLVP = args["SOURCE_TABLE_CONTROLVP"]
SOURCE_TABLE_AUSENTISMO = args["SOURCE_TABLE_AUSENTISMO"]
TARGET_BUCKET = args["TARGET_BUCKET"]
TARGET_DATABASE = args["TARGET_DATABASE"]
TARGET_TABLE_CRONOGRAMAVACACIONES = args["TARGET_TABLE_CRONOGRAMAVACACIONES"]

TARGET_DAYS = int(args["PARAMETER_DAYS"])
TIMEZONE = pytz.timezone('America/Lima')
FECHA_INI = datetime.combine(datetime.now(TIMEZONE).date() - timedelta(days=TARGET_DAYS), datetime.min.time()).strftime('%Y%m')
FECHA_FIN = datetime.combine(datetime.now(TIMEZONE).date(), datetime.min.time()).strftime('%Y%m')

BUCKET_NAME = TARGET_BUCKET.split("/")[2]
CUENTA = TARGET_BUCKET.split("/")[3]
TABLE_NAME = TARGET_BUCKET.split("/")[4]
PREFIX = f'{CUENTA}/{TABLE_NAME}'

fec_ini_mes = datetime.combine(datetime.now(TIMEZONE).date() - timedelta(days=TARGET_DAYS), datetime.min.time()).date()
fec_ini_mes = fec_ini_mes.replace(day = 1)
fec_fin_mes = datetime.now(TIMEZONE).date()

FECHA_INI = int(FECHA_INI)
FECHA_FIN = int(FECHA_FIN)

print(f"FECHA_INI:{fec_ini_mes} - FECHA_FIN:{fec_fin_mes} - FECHA_TDAYS:{fec_ini_mes}")

s3_client = boto3.client('s3', region_name=AWS_REGION)

glue_client = boto3.client("glue", region_name=AWS_REGION)

filtro_sql = f"date_format(fecha, 'yyyy-MM-dd') >= '{fec_ini_mes}'"

# TABLA EMPLEADO
empleadoSrc = glueContext.create_dynamic_frame.from_catalog(
    database=SOURCE_DATABASE,
    table_name=SOURCE_TABLE_EMPLEADO,
    #push_down_predicate = filtro_sql,
    transformation_ctx="empleadoSrc",
)

empleadoMaping = ApplyMapping.apply(
    frame = empleadoSrc,
    mappings = [( "emp_id", "int", "emp_id", "int"),
                ( "emp_apellido", "string", "emp_apellido", "string"),
                ( "emp_nombre", "string", "emp_nombre", "string"),
                ( "emp_num_documento", "int", "emp_num_documento", "int"),
    ],
    transformation_ctx = "empleadoMaping",
)

empleadoMaping = empleadoMaping.toDF()
#empleadoMaping = empleadoMaping.withColumn('periodo', date_format(empleadoMaping['fecha'], 'yyyyMM').cast('int'))

# TABLA EMPLEADO CUENTA
empleadocuentaSrc = glueContext.create_dynamic_frame.from_catalog(
    database=SOURCE_DATABASE,
    table_name=SOURCE_TABLE_EMPLEADOCUENTA,
    #push_down_predicate = filtro_sql,
    transformation_ctx="empleadocuentaSrc",
)

def filter_func(dynamic_record):
    return dynamic_record['anulado'] == 0

empleadocuentaSrc_filtrado = Filter.apply(frame=empleadocuentaSrc, f=filter_func)

empleadocuentaSelectFields = SelectFields.apply(
    frame = empleadocuentaSrc_filtrado,
    paths = ["emp_id", "cue_id", "car_id","ecu_fechadesde", "ecu_fechahasta", "anulado","sar_id"],
    transformation_ctx = "empleadocuentaSelectfields"
    )

empleadocuentaMaping = ApplyMapping.apply(
    frame = empleadocuentaSelectFields,
    mappings = [( "emp_id", "int", "emp_id", "int"),
                ( "cue_id", "int", "cue_id", "int"),
                ( "car_id", "int", "car_id", "int"),
                ( "ecu_fechadesde", "timestamp", "ecu_fechadesde", "timestamp"),
                ( "ecu_fechahasta", "timestamp", "ecu_fechahasta", "timestamp"),
                ( "anulado", "int", "anulado", "int"),
                ( "sar_id", "int", "sar_id", "int"),
    ],
    transformation_ctx = "empleadocuentaMaping",
)

empleadocuentaMaping = empleadocuentaMaping.toDF()
#empleadocuentaMaping = empleadocuentaMaping.withColumn('periodo_empleadocuenta', date_format(empleadocuentaMaping['fecha_pregunta'], 'yyyyMM').cast('int'))

# TABLA CUENTA
cuentaSrc = glueContext.create_dynamic_frame.from_catalog(
    database=SOURCE_DATABASE,
    table_name=SOURCE_TABLE_CUENTA,
    #push_down_predicate = filtro_sql,
    transformation_ctx="cuentaSrc",
)

def filter_func(dynamic_record):
    return dynamic_record['anulado'] == 0

cuentaSrc_filtrado = Filter.apply(frame=cuentaSrc, f=filter_func)

cuentaSelectFields = SelectFields.apply(
    frame = cuentaSrc_filtrado,
    paths = ["cue_id", "cue_nombre", "anulado"],
    transformation_ctx = "cuentaSelectfields"
    )

cuentaMaping = ApplyMapping.apply(
    frame = cuentaSelectFields,
    mappings = [( "cue_id", "int", "cue_id", "int"),
                ( "cue_nombre", "string", "cue_nombre", "string"),
                ( "anulado", "int", "anulado", "int"),
    ],
    transformation_ctx = "cuentaMaping",
)

cuentaMaping = cuentaMaping.toDF()
#cuentaMaping = cuentaMaping.withColumn('periodo_cuenta', date_format(cuentaMaping['fecha_pregunta'], 'yyyyMM').cast('int'))

# TABLA SUBAREA
subareaSrc = glueContext.create_dynamic_frame.from_catalog(
    database=SOURCE_DATABASE,
    table_name=SOURCE_TABLE_SUBTAREA,
    #push_down_predicate = filtro_sql,
    transformation_ctx="subareaSrc",
)

def filter_func(dynamic_record):
    return dynamic_record['anulado'] == 0

subareaSrc_filtrado = Filter.apply(frame=subareaSrc, f=filter_func)

subareaSelectFields = SelectFields.apply(
    frame = subareaSrc_filtrado,
    paths = ["sar_id", "sar_nombre", "anulado"],
    transformation_ctx = "subareaSelectfields"
    )

subareaMaping = ApplyMapping.apply(
    frame = subareaSelectFields,
    mappings = [( "sar_id", "int", "sar_id", "int"),
                ( "sar_nombre", "string", "sar_nombre", "string"),
                ( "anulado", "int", "anulado", "int"),
    ],
    transformation_ctx = "subareaMaping",
)

subareaMaping = subareaMaping.toDF()
#subareaMaping = subareaMaping.withColumn('periodo_subarea', date_format(subareaMaping['fecha_pregunta'], 'yyyyMM').cast('int'))

# TABLA EMPLEADO_ERP
empleadoerpSrc = glueContext.create_dynamic_frame.from_catalog(
    database=SOURCE_DATABASE,
    table_name=SOURCE_TABLE_EMPLEADO_ERP,
    #push_down_predicate = filtro_sql,
    transformation_ctx="empleadoerpSrc",
)

def filter_func(dynamic_record):
    return dynamic_record['eis_anulado'] == 0

empleadoerpSrc_filtrado = Filter.apply(frame=empleadoerpSrc, f=filter_func)

empleadoerpSelectFields = SelectFields.apply(
    frame = empleadoerpSrc_filtrado,
    paths = ["emp_id", "id_personal_erp", "eis_fechaHasta","eis_anulado"],
    transformation_ctx = "empleadoerpSelectfields"
    )

empleadoerpMaping = ApplyMapping.apply(
    frame = empleadoerpSelectFields,
    mappings = [( "emp_id", "int", "emp_id", "int"),
                ( "id_personal_erp", "int", "id_personal_erp", "int"),
                ( "eis_fechaHasta", "timestamp", "eis_fechaHasta", "timestamp"),
                ( "eis_anulado", "int", "eis_anulado", "int"),
    ],
    transformation_ctx = "empleadoerpMaping",
)

empleadoerpMaping = empleadoerpMaping.toDF()
#empleadoerpMaping = empleadoerpMaping.withColumn('periodo_empleadoerp', date_format(empleadoerpMaping['fecha_pregunta'], 'yyyyMM').cast('int'))


# TABLA EMPLEADOSERVICIO
empleadoservicioSrc = glueContext.create_dynamic_frame.from_catalog(
    database=SOURCE_DATABASE,
    table_name=SOURCE_TABLE_EMPLEADOSERVICIO,
    #push_down_predicate = filtro_sql,
    transformation_ctx="empleadoservicioSrc",
)

def filter_func(dynamic_record):
    return dynamic_record['anulado'] == 0

empleadoservicioSrc_filtrado = Filter.apply(frame=empleadoservicioSrc, f=filter_func)

empleadoservicioSelectFields = SelectFields.apply(
    frame = empleadoservicioSrc_filtrado,
    paths = ["emp_id", "srv_id", "empsrv_fechadesde","anulado"],
    transformation_ctx = "empleadoservicioSelectfields"
    )

empleadoservicioMaping = ApplyMapping.apply(
    frame = empleadoservicioSelectFields,
    mappings = [( "emp_id", "int", "emp_id", "int"),
                ( "srv_id", "int", "srv_id", "int"),
                ( "empsrv_fechadesde", "timestamp", "empsrv_fechadesde", "timestamp"),
                ( "anulado", "int", "anulado", "int"),
    ],
    transformation_ctx = "empleadoservicioMaping",
)

empleadoservicioMaping = empleadoservicioMaping.toDF()
#empleadoservicioMaping = empleadoservicioMaping.withColumn('periodo_empleadoservicio', date_format(empleadoservicioMaping['fecha_pregunta'], 'yyyyMM').cast('int'))


# TABLA SERVICIOS
serviciosSrc = glueContext.create_dynamic_frame.from_catalog(
    database=SOURCE_DATABASE,
    table_name=SOURCE_TABLE_SERVICIOS,
    #push_down_predicate = filtro_sql,
    transformation_ctx="serviciosSrc",
)

def filter_func(dynamic_record):
    return dynamic_record['anulado'] == 0

serviciosSrc_filtrado = Filter.apply(frame=serviciosSrc, f=filter_func)

serviciosSelectFields = SelectFields.apply(
    frame = serviciosSrc_filtrado,
    paths = ["srv_id", "srv_nombre", "anulado"],
    transformation_ctx = "serviciosSelectfields"
    )

serviciosMaping = ApplyMapping.apply(
    frame = serviciosSelectFields,
    mappings = [( "srv_id", "int", "srv_id", "int"),
                ( "srv_nombre", "string", "srv_nombre", "string"),
                ( "anulado", "int", "anulado", "int"),
    ],
    transformation_ctx = "serviciosMaping",
)

serviciosMaping = serviciosMaping.toDF()
#serviciosMaping = serviciosMaping.withColumn('periodo_servicios', date_format(serviciosMaping['fecha_pregunta'], 'yyyyMM').cast('int'))


# TABLA CARGO
cargoSrc = glueContext.create_dynamic_frame.from_catalog(
    database=SOURCE_DATABASE,
    table_name=SOURCE_TABLE_CARGO,
    #push_down_predicate = filtro_sql,
    transformation_ctx="cargoSrc",
)

def filter_func(dynamic_record):
    return dynamic_record['anulado'] == 0

cargoSrc_filtrado = Filter.apply(frame=cargoSrc, f=filter_func)

cargoSelectFields = SelectFields.apply(
    frame = cargoSrc_filtrado,
    paths = ["car_id", "car_nombre", "anulado"],
    transformation_ctx = "cargoSelectfields"
    )

cargoMaping = ApplyMapping.apply(
    frame = cargoSelectFields,
    mappings = [( "car_id", "int", "car_id", "int"),
                ( "car_nombre", "string", "car_nombre", "string"),
                ( "anulado", "int", "anulado", "int"),
    ],
    transformation_ctx = "cargoMaping",
)

cargoMaping = cargoMaping.toDF()
#cargoMaping = cargoMaping.withColumn('periodo_cargo', date_format(cargoMaping['fecha_pregunta'], 'yyyyMM').cast('int'))


# TABLA AUSENTISMO
ausentismoSrc = glueContext.create_dynamic_frame.from_catalog(
    database=SOURCE_DATABASE,
    table_name=SOURCE_TABLE_AUSENTISMO,
    #push_down_predicate = filtro_sql,
    transformation_ctx="ausentismoSrc",
)

def filter_func(dynamic_record):
    return dynamic_record['anulado'] == 0

ausentismoSrc_filtrado = Filter.apply(frame=ausentismoSrc, f=filter_func)

ausentismoSelectFields = SelectFields.apply(
    frame = ausentismoSrc_filtrado,
    paths = ["emp_id", "aus_fechadesde", "aus_fechacontrol","anulado"],
    transformation_ctx = "ausentismoSelectfields"
    )

ausentismoMaping = ApplyMapping.apply(
    frame = ausentismoSelectFields,
    mappings = [( "emp_id", "int", "emp_id", "int"),
                ( "amo_id", "int", "amo_id", "int"),
                ( "aus_fechadesde", "timestamp", "aus_fechadesde", "timestamp"),
                ( "aus_fechacontrol", "timestamp", "aus_fechacontrol", "timestamp"),
                ( "anulado", "int", "anulado", "int"),
    ],
    transformation_ctx = "ausentismoMaping",
)

ausentismoMaping = ausentismoMaping.toDF()
#ausentismoMaping = ausentismoMaping.withColumn('periodo_ausentismo', date_format(ausentismoMaping['fecha_pregunta'], 'yyyyMM').cast('int'))



# JOIN TABLAS

# Primero, calculamos las filas más recientes para empleado_cuenta y Empleado_servicio usando ROW_NUMBER()
windowSpecEmpCuenta = Window.partitionBy("emp_id").orderBy(F.col("ecu_fechadesde").desc())
empleadocuentaMaping = empleadocuentaMaping.withColumn("rn", F.row_number().over(windowSpecEmpCuenta)) \
    .filter(F.col("rn") == 1).drop("rn")

windowSpecEmpServicio = Window.partitionBy("emp_id").orderBy(F.col("empsrv_fechadesde").desc())
empleadoservicioMaping = empleadoservicioMaping.withColumn("rn", F.row_number().over(windowSpecEmpServicio)) \
    .filter(F.col("rn") == 1).drop("rn")

# Join y selección
# DATAFRAME DATA EMPLEADOS
datospersonales_df = empleadoMaping.alias("e") \
    .join(empleadocuentaMaping.alias("ec"), F.col("e.emp_id") == F.col("ec.emp_id"), "left") \
    .join(cuentaMaping.alias("c"), F.col("ec.cue_id") == F.col("c.cue_id"), "left") \
    .join(subareaMaping.alias("sa"), F.col("ec.sar_id") == F.col("sa.sar_id"), "left") \
    .join(empleadoerpMaping.filter(F.col("eis_fechaHasta").isNull()).alias("eid"), F.col("e.emp_id") == F.col("eid.emp_id"), "left") \
    .join(empleadoservicioMaping.alias("ser"), F.col("e.emp_id") == F.col("ser.emp_id"), "left") \
    .join(serviciosMaping.alias("s"), F.col("ser.srv_id") == F.col("s.srv_id"), "left") \
    .join(cargoMaping.alias("car"), F.col("ec.car_id") == F.col("car.car_id"), "left") \
    .filter(F.col("eid.id_personal_erp").isNotNull()) \
    .select(
        F.col("eid.id_personal_erp"),
        F.col("e.emp_id"),
        F.concat_ws(' ', F.col("e.emp_apellido"), F.col("e.emp_nombre")).alias("nombre_completo"),
        F.col("e.emp_num_documento"),
        F.col("c.cue_nombre").alias("Cuenta"),
        F.col("sa.sar_nombre").alias("Sub_Cuenta"),
        F.col("s.srv_nombre").alias("Servicio"),
        F.col("car.car_nombre").alias("Cargo")
    )

###################################################################################################################

# Realizar JOINs y calcular columnas finales
resultado_df = ausentismoMaping \
    .filter((F.col("anulado") == 0) & (F.col("aus_fechacontrol").isNull())) \
    .withColumn("Anio", F.year("aus_fechadesde")) \
    .withColumn("Mes", F.month("aus_fechadesde")) \
    .withColumn("Dia", F.dayofmonth("aus_fechadesde")) \
    .withColumn("DiaYMes", F.concat(F.col("Mes"), F.lit('-'), F.col("Dia"))) \
    .filter(F.col("Anio") == F.year(F.current_date())) \
    .groupBy("emp_id", "Anio", "Mes") \
    .agg(F.count("*").alias("cuenta_ausentismo"))

resultado_df = resultado_df.join(datospersonales_df, "emp_id", "semi")

# Realizando JOINs union entre la tabla con datos del personal y conectando por emp_id

cronogramavacacionesMaping = datospersonales_df.alias("a") \
    .join(resultado_df.alias("b"), "emp_id", "right").filter(F.col("a.id_personal_erp").isNotNull()) \



# BORRAR PARTICIONES

try:
    # Verifica la existencia de la tabla
    response = glue_client.get_table(
        DatabaseName=TARGET_DATABASE_ANALYTICS,
        Name=TARGET_TABLE_CRONOGRAMAVACACIONES
    )
    print(f'La tabla {TARGET_DATABASE_ANALYTICS}.{TARGET_TABLE_CRONOGRAMAVACACIONES} existe en el catalogo de Glue.')
    validate = 1
except ClientError as e:
    if e.response['Error']['Code'] == 'EntityNotFoundException':
        print(f'La tabla {TARGET_DATABASE_ANALYTICS}.{TARGET_TABLE_CRONOGRAMAVACACIONES} no existe en el catalogo de Glue.')
        validate = 0
    else:
        print(f'Error al verificar la tabla: {e.response["Error"]["Message"]}')
        sys.exit(1)

if validate == 1:
    def split_s3_path(s3_path):
        path_parts = s3_path.replace("s3://", "").split("/")
        bucket = path_parts.pop(0)
        key = "/".join(path_parts)
        return bucket, key
    
    def get_partitions_where_fecha_between(database: str, table: str):
        _partitions = []
        
        response = glue_client.get_partitions(
            DatabaseName=database,
            TableName=table,
        )
        _partitions = _partitions + response["Partitions"]
    
        while "NextToken" in response:
            response = glue_client.get_partitions(
                DatabaseName=database,
                TableName=table,
                NextToken=response["NextToken"]
            )
            _partitions = _partitions + response["Partitions"]
    
        return _partitions
    
    
    def delete_partitions_data_where_fecha_between(database, table):
        partitions = get_partitions_where_fecha_between(database=database, table=table)
        for partition in partitions:
            _storage_location = partition["StorageDescriptor"]["Location"]
            print(f"delete partition {partition['Values']} stored in {_storage_location}")
    
            _bucket, _key = split_s3_path(_storage_location)
            _objects = s3_client.list_objects_v2(Bucket=_bucket, Prefix=_key)
    
            if 'Contents' in _objects:
                for _object in _objects['Contents']:
                    print('Deleting', _object['Key'])
                    s3_client.delete_object(Bucket=_bucket, Key=_object['Key'])
            else:
                print("No data in partition")
    
    def delete_parquets(bucket_name,folder_prefix):
        try:
            response = s3_client.list_objects(Bucket=bucket_name, Prefix=folder_prefix)
            #print(response['Contents'][1]['Key'])
            if 'Contents' in response:
                # Si hay objetos, eliminar cada uno
                for obj in response['Contents']:
                    print(obj['Key'])
                    a = obj['Key']
                    #print(a)
                    s3_client.delete_object(Bucket=bucket_name, Key=a)
            print("Deletion complete.")
        except Exception as e:
            print(f"An error occurred: {e}")
        
    
    delete_partitions_data_where_fecha_between(database=TARGET_DATABASE_ANALYTICS,
                                              table=TARGET_TABLE_CRONOGRAMAVACACIONES)
    print(BUCKET_NAME)
    print(PREFIX)
    delete_parquets(bucket_name=BUCKET_NAME, folder_prefix=PREFIX)
    
##############################################################
################CREANDO PARTICIONES###########################
##############################################################
cronogramavacacionesMaping = DynamicFrame.fromDF(cronogramavacacionesMaping, glueContext, "TablonGen")
cronogramavacaciones = glueContext.write_dynamic_frame.from_options(
    frame=cronogramavacacionesMaping,
    connection_type="s3",
    format="glueparquet",
    connection_options={"path": TARGET_BUCKET},
    format_options={"compression": "snappy"},
    transformation_ctx="cronogramavacaciones",
)
job.commit()