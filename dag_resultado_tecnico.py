"""
DAG: resultado técnico por certificado

Orquesta ~100 transformaciones de BigQuery agrupadas en 14 líneas de
negocio (primas, ajuste RRC, reaseguros, comisiones, siniestros, gasto
técnico, patrimonios, inversiones, entre otras) para calcular el estado
de resultados a nivel de póliza/certificado.

Los scripts SQL se descubren dinámicamente desde una carpeta versionada;
el DAG sustituye los placeholders de entorno y periodo y genera un
BigQueryInsertJobOperator por cada uno.
"""
import os
import pendulum
from pathlib import Path
from typing import List

from airflow.models.param import Param
from airflow.decorators import dag, task, task_group
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator

from common_plugins import (
    get_scripts_in_folder,
    get_current_file_script,
    get_parameters,
    get_labels,
    update_config_dependencies_status,
    send_email,
    get_owner,
    get_process_id
)



####################
# variables globales
####################
BQ_JOB_MINUTES_TIMEOUT = 10
DAG_MINUTES_TIMEOUT = (BQ_JOB_MINUTES_TIMEOUT * 3)
STATUS_SUCCEEDED = 'SUCCEEDED'
STATUS_FAILED = 'FAILED'
PERIODO_INICIO = pendulum.now().subtract(years=1).start_of('year').format('YYYY-MM-DD') if pendulum.now().month <= 6 else pendulum.now().start_of('year').format('YYYY-MM-DD')
PERIODO_FIN = pendulum.now().start_of('month').format('YYYY-MM-DD')
V_PERIODO = pendulum.now('America/Lima').start_of('month').format('YYYY-MM-DD')
PERIODO_ERP_CORTE = pendulum.date(2024, 4, 1).format('YYYY-MM-DD')

######################
# variables de entorno
######################
environment = os.environ["ENV_DESC"]
ingest_project_id = os.environ["INGEST_PROJECT_ID"]
staging_project_id = os.environ["STAGING_PROJECT_ID"]
analytics_project_id = os.environ["ANALYTICS_PROJECT_ID"]
projectid_transversal = os.environ["PROJECTID_TRANSVERSAL"]
impersonation_chain = os.environ["SERVICE_ACCOUNT_ANL"]
impersonation_chain_transversal = os.environ["SERVICE_ACCOUNT_TRSV_MDM"] if environment in ["nonprod"] else impersonation_chain
sg_api_key_secret_id = os.environ["SENDGRID_API_KEY_SECRET_ID"]
sg_mail_from = os.environ["SENDGRID_MAIL_FROM"]
sg_secrets_project_id = os.environ["SECRETS_PROJECT_ID"]
sg_mails_recipients = os.environ["MAILS_RECIPIENTS"]

#################################
# configuracion y parameters.json
#################################
# identificacion
dag_id = "ue4" + "_" + environment + "_" + "com" + "_" + Path(__file__).stem.split("_com_",1)[1].replace("_dag_","_")
repo = "https://github.com/mi-organizacion/analytics-financiero"
repo_branch = "main"
repo_readme = f'{repo}/blob/{repo_branch}/README.md'

# New: use dag_id value to compute PROCESS_ID and folder for scripts/parameters
PROCESS_ID = get_process_id({"dag_id": dag_id})
PROCESS_ID_GLOB = Path(PROCESS_ID) / "*.sql"
PROCESS_ID_FOLDER = PROCESS_ID_GLOB.as_posix()

# calendarizacion
yesterday = pendulum.yesterday()

# parametrizacion
parameters = {**get_parameters(PROCESS_ID_FOLDER), 'dag_id': dag_id}
parameters.update({"dag_id": dag_id})
parameters_tags = [os.path.dirname(os.path.abspath(__file__)).split('/')[5]]
parameters_tags[len(parameters_tags):] = parameters['dag_tags']
parameters_description = parameters['dag_description']
parameters_scripts = parameters["scripts"]
parameters_owner = get_owner(parameters)
parameters_dag_minutes_timeout = None if environment in ["prod", "nonprod_dd"] else pendulum.duration(minutes=DAG_MINUTES_TIMEOUT)

# argumentos comunes
default_args = {
    'owner': parameters_owner,
    'start_date': yesterday,
    'retries': 0,
    'email_on_failure': False,
    'email_on_retry': False
}

# conexiones Bigquery
bq_project_id = analytics_project_id
bq_gcp_conn_id = "bigquery_anl_custom"
bq_impersonation_chain = impersonation_chain

def generate_periods_desc(start_year, end_year=None):
    now = pendulum.now()
    end_year = end_year or now.year
    end_month = now.month if end_year == now.year else 12
    return [
        pendulum.date(start_year, 1, 1).add(months=i).format("YYYY-MM-01")
        for i in range((end_year - start_year) * 12 + end_month)
    ][::-1]

periods_list = generate_periods_desc(pendulum.now().subtract(years=2).start_of('year').year) #Último 3 años
periods_list_anl = generate_periods_desc(pendulum.now().year)

@dag(
    dag_id = dag_id,
    description = parameters_description,
    schedule_interval = None,
    default_args = default_args,
    dagrun_timeout = parameters_dag_minutes_timeout,
    catchup = False,
    tags = parameters_tags,
    owner_links = {
        parameters_owner: repo_readme
    },
    params = {
        "bq_job_minutes_timeout": Param(
            BQ_JOB_MINUTES_TIMEOUT,
            type = "integer",
            minimum = 1,
            maximum = 10,
            title = "bq job timeout",
            description = "BigQuery cancela el job transcurridos los minutos establecidos. N/A en ambiente Producción"
        ),
        "form_param00_v_periodo": Param(
            V_PERIODO,
            type = "string",
            enum = periods_list_anl,
            title = "periodo actual en anl",
            description = "Periodo para anl en formato (YYYY-MM-DD)."
        ),
        "form_param05_tmp": Param(
            "tmp_oppo",
            type = "string",
            title = "tmp",
            description = "prefijo tabla temporal"
        ),
         "form_param02_periodo_inicio": Param(
            PERIODO_INICIO,
            type = "string",
            enum = periods_list,
            title = "Periodo inicio",
            description = "Periodo inicio (YYYY-MM-DD)"
        ),
        "form_param03_periodo_fin": Param(
            PERIODO_FIN,
            type = "string",
            enum = periods_list,
            title = "Periodo fin",
            description = "Periodo fin (YYYY-MM-DD)"
        )
    }
)
def flujo_tareas():
    
    # dict for bq tasks
    bq_op_task_dict01 = dict()
    # multi-statement query filenames
    all_sql_files = get_scripts_in_folder(PROCESS_ID_FOLDER)
    
    
    @task(task_id = 'inicio')
    def inicio(**context):
        p_periodo_inicio = context["params"]["form_param02_periodo_inicio"]
        p_periodo_fin = context["params"]["form_param03_periodo_fin"]
        if p_periodo_inicio > p_periodo_fin:
            raise ValueError(f"El periodo de inicio no debe ser mayor al periodo fin {p_periodo_inicio} > {p_periodo_fin}.")
        else:
            pass
    
    @task
    def fin():
        pass
    
    @task
    def inicio_lineas():
        pass

    @task
    def fin_lineas():
        pass

    @task
    def inicio_primas():
        pass

    @task
    def fin_primas():
        pass
    
    @task
    def inicio_ajuste_rrc():
        pass

    @task
    def fin_ajuste_rrc():
        pass

    @task
    def inicio_reaseguros():
        pass

    @task
    def fin_reaseguros():
        pass

    @task
    def inicio_dscto_reaseguro():
        pass

    @task
    def fin_dscto_reaseguro():
        pass

    @task
    def inicio_comision():
        pass

    @task
    def fin_comision():
        pass

    @task
    def inicio_siniestros_cedidos():
        pass

    @task
    def fin_siniestros_cedidos():
        pass

    @task
    def inicio_gasto_tecnico():
        pass

    @task
    def fin_gasto_tecnico():
        pass

    @task
    def inicio_pcd():
        pass

    @task
    def fin_pcd():
        pass
    
    @task
    def inicio_patrimonios():
        pass

    @task
    def fin_patrimonios():
        pass

    @task
    def inicio_ajuste_rt_gerencial():
        pass

    @task
    def fin_ajuste_rt_gerencial():
        pass

    @task
    def inicio_gastos_rt():
        pass

    @task
    def fin_gastos_rt():
        pass

    @task
    def inicio_inversiones():
        pass

    @task
    def fin_inversiones():
        pass

    @task
    def inicio_otros_ingresos_financieros():
        pass

    @task
    def fin_otros_ingresos_financieros():
        pass

    @task
    def inicio_siniestros_sin_recuperos():
        pass

    @task
    def fin_siniestros_sin_recuperos():
        pass

    

    task_id_ok = 'update_config_table_with_{0}'.format(STATUS_SUCCEEDED)
    @task(task_id=task_id_ok, trigger_rule="all_success")
    def update_dependencies_status_ok(**context):
        dict_labels = get_labels(parameters, STATUS_SUCCEEDED, ti=context["ti"])
        dict_argumentos = {
            "dag_id": dag_id,
            "status": STATUS_SUCCEEDED,
            "labels": dict_labels,
            "impersonation_chain": impersonation_chain_transversal
        }
        update_config_dependencies_status(dict_argumentos)
    

    task_id_error = 'update_config_table_with_{0}'.format(STATUS_FAILED)
    @task(task_id=task_id_error, trigger_rule="one_failed")
    def update_dependencies_status_error(**context):
        send_email(sendgrid_api_key_secret_id = sg_api_key_secret_id,
                   sendgrid_domain = sg_mail_from,
                   secrets_project_id = sg_secrets_project_id,
                   recipients = sg_mails_recipients.split(","),
                   mail_subject = 'DAG {0}, stg to anl'.format(dag_id),
                   source = dag_id,
                   target = parameters_tags[0],
                   schedule_interval = None,
                   project_id = analytics_project_id,
                   dag_id = dag_id,
                   success = False
        )
        dict_labels = get_labels(parameters, STATUS_FAILED, ti=context["ti"])
        dict_argumentos = {
            "dag_id": dag_id,
            "status": STATUS_FAILED,
            "labels": dict_labels,
            "impersonation_chain": impersonation_chain_transversal
        }
        update_config_dependencies_status(dict_argumentos)

    # Determinar el periodo de inicio y fin
    p_v_periodo = "{{ params.form_param00_v_periodo }}"
    p_periodo_inicio = "{{ params.form_param02_periodo_inicio }}"
    p_periodo_fin = "{{ params.form_param03_periodo_fin }}"
    prefix_tmp = "{{ params.form_param05_tmp }}" #update

    erp_periodo_corte = PERIODO_ERP_CORTE
    script_set_01 = parameters_scripts["bq_scripts"]
    for script_file in all_sql_files:
        # valida si el nombre del archivo se encuentra en el set
        script_name = Path(script_file).stem
        if script_name in script_set_01:
            # determinacion del script
            sql = get_current_file_script(script_file)
            # reemplazos
            sql = sql.replace("{project_raw}", ingest_project_id)
            sql = sql.replace("{project_staging}", staging_project_id)
            sql = sql.replace("{project_analytics}", analytics_project_id)
            sql = sql.replace("{project_gobierno}", projectid_transversal)
            sql = sql.replace("{project_analytics}", analytics_project_id)
            sql = sql.replace("{project_staging}", staging_project_id)
            sql = sql.replace("{erp_periodo_corte}", erp_periodo_corte)
            sql = sql.replace("{v_periodo}", p_v_periodo)
            sql = sql.replace("{periodo_inicio}", p_periodo_inicio)
            sql = sql.replace("{periodo_fin}", p_periodo_fin)
            sql = sql.replace("{tmp}", prefix_tmp)
            sql = sql.replace("{environment}", environment)
            # labels
            dict_labels = get_labels(parameters=parameters)
            # bq job configuration
            bq_job_minutes_timeout = "{{ params.bq_job_minutes_timeout * 60000 }}"
            configuration = {
                "query": {
                    "query": sql,
                    "useQueryCache": "True", 
                    "useLegacySql": "False",
                    "priority": "BATCH"
                },
                "jobTimeoutMs": bq_job_minutes_timeout,
                "labels": dict_labels
            }
            if environment in ["prod", "nonprod_dd"]:
                configuration.pop("jobTimeoutMs")
            # instance task mapping
            task_id = script_name

            if task_id == "bq_borrar_tablas_temporales":

                bq_op_task_dict01[task_id] = BigQueryInsertJobOperator(
                    task_id = task_id,
                    job_id = "{{ ts_nodash }}",
                    project_id = bq_project_id,
                    gcp_conn_id = bq_gcp_conn_id,
                    impersonation_chain = bq_impersonation_chain,
                    configuration = configuration,
                    trigger_rule = "all_done"
                )
            
            else:

                bq_op_task_dict01[task_id] = BigQueryInsertJobOperator(
                    task_id = task_id,
                    job_id = "{{ ts_nodash }}",
                    project_id = bq_project_id,
                    gcp_conn_id = bq_gcp_conn_id,
                    impersonation_chain = bq_impersonation_chain,
                    configuration = configuration
                )
    
    start = inicio()
    end = fin()
    inicio_lineas = inicio_lineas()
    fin_lineas = fin_lineas()
    inicio_primas = inicio_primas()
    fin_primas = fin_primas()
    inicio_ajuste_rrc = inicio_ajuste_rrc()
    fin_ajuste_rrc = fin_ajuste_rrc()
    inicio_reaseguros = inicio_reaseguros()
    fin_reaseguros = fin_reaseguros()
    inicio_dscto_reaseguro = inicio_dscto_reaseguro()
    fin_dscto_reaseguro = fin_dscto_reaseguro()
    inicio_comision = inicio_comision()
    fin_comision = fin_comision()
    inicio_siniestros_cedidos = inicio_siniestros_cedidos()
    fin_siniestros_cedidos = fin_siniestros_cedidos()
    inicio_gasto_tecnico = inicio_gasto_tecnico()
    fin_gasto_tecnico = fin_gasto_tecnico()
    inicio_pcd = inicio_pcd()
    fin_pcd = fin_pcd()
    inicio_patrimonios = inicio_patrimonios()
    fin_patrimonios = fin_patrimonios()
    inicio_ajuste_rt_gerencial = inicio_ajuste_rt_gerencial()
    fin_ajuste_rt_gerencial = fin_ajuste_rt_gerencial()
    inicio_gastos_rt = inicio_gastos_rt()
    fin_gastos_rt = fin_gastos_rt()
    inicio_inversiones = inicio_inversiones()
    fin_inversiones = fin_inversiones()
    inicio_otros_ingresos_financieros = inicio_otros_ingresos_financieros()
    fin_otros_ingresos_financieros = fin_otros_ingresos_financieros()
    inicio_siniestros_sin_recuperos = inicio_siniestros_sin_recuperos()
    fin_siniestros_sin_recuperos = fin_siniestros_sin_recuperos()

    ## SEQUENCE
    start >> bq_op_task_dict01[f"bq_00_carga_inicial"] >> [bq_op_task_dict01[f"bq_00_distribucion_poliza_vigente"], bq_op_task_dict01["bq_00_poliza_sumaseg_principal"], bq_op_task_dict01["bq_00_distribucion_cliente_vigente"], bq_op_task_dict01["bq_00_distribucion_siniestros_ocurrencia"], bq_op_task_dict01["bq_00_distribucion_vencimiento_poliza"], bq_op_task_dict01["bq_00_distribucion_default"], bq_op_task_dict01["bq_00_distribucion_poliza_vigente_canal_sub_canal"]] >> inicio_lineas

    ## PRIMAS
    inicio_lineas >> inicio_primas >> bq_op_task_dict01[f"bq_01_01_prima_carga_directo"] >> bq_op_task_dict01[f"bq_01_02_prima_distribucion_prima"] >> bq_op_task_dict01[f"bq_01_02_prima_distribucion_prima_canal_subcanal_intermediario"] >> bq_op_task_dict01[f"bq_01_03_prima_carga_aman"] >> bq_op_task_dict01[f"bq_01_03_01_prima_carga_aman_reporte"] >> bq_op_task_dict01[f"bq_01_04_prima_carga_pnra"] >> fin_primas >> [bq_op_task_dict01[f"bq_01_05_prima_distribucion_prima_emitida"],bq_op_task_dict01[f"bq_01_06_prima_distribucion_prima_ltm"]] >> fin_lineas

    ## AJUSTE RRC
    inicio_lineas >> inicio_ajuste_rrc >> bq_op_task_dict01[f"bq_02_00_ajuste_rrc_carga_reserva_matematica"] >> bq_op_task_dict01[f"bq_02_00_ajuste_rrc_reserva_matematica_saldo"]
    bq_op_task_dict01[f"bq_02_00_ajuste_rrc_carga_reserva_matematica"] >> bq_op_task_dict01[f"bq_02_01_ajuste_rrc_distribucion_acpe_devengado"] >> bq_op_task_dict01[f"bq_02_02_01_ajuste_rrc_carga_acpe_rrc_directa_reporte"] >> bq_op_task_dict01[f"bq_02_02_ajuste_rrc_carga_acpe_rrc_directa"]
    bq_op_task_dict01[f"bq_02_00_ajuste_rrc_carga_reserva_matematica"] >> bq_op_task_dict01[f"bq_02_08_ajuste_rrc_carga_aman_distribuido"]
    bq_op_task_dict01[f"bq_01_02_prima_distribucion_prima_canal_subcanal_intermediario"] >> bq_op_task_dict01[f"bq_02_05_ajuste_rrc_carga_pnra_distribuido"]
    bq_op_task_dict01[f"bq_02_02_ajuste_rrc_carga_acpe_rrc_directa"] >> [ bq_op_task_dict01[f"bq_02_03_ajuste_rrc_carga_directa_acsele"], bq_op_task_dict01[f"bq_02_04_ajuste_rrc_carga_directa_acselx"], bq_op_task_dict01[f"bq_02_05_ajuste_rrc_carga_pnra_distribuido"] ] >> bq_op_task_dict01[f"bq_02_08_ajuste_rrc_carga_aman_distribuido"] >> bq_op_task_dict01[f"bq_02_09_ajuste_rrc_distribucion_ajuste_rrc"] 
    bq_op_task_dict01[f"bq_02_09_ajuste_rrc_distribucion_ajuste_rrc"] >> bq_op_task_dict01[f"bq_02_10_01_ajuste_rrc_distribucion_rip"] >> bq_op_task_dict01[f"bq_02_10_02_ajuste_rrc_carga_rip"] >> fin_lineas
    bq_op_task_dict01[f"bq_02_00_ajuste_rrc_reserva_matematica_saldo"] >> bq_op_task_dict01[f"bq_02_13_ajuste_rrc_distribucion_prov_reserv_matematica_saldo"] >> bq_op_task_dict01[f"bq_02_14_ajuste_rrc_tipo_cambio_ajuste_rrc"] >> fin_ajuste_rrc
    fin_ajuste_rrc >> fin_lineas

    ## REASEGUROS
    inicio_lineas >> inicio_reaseguros >> bq_op_task_dict01[f"bq_03_01_reaseguro_distribucion_sin_xloss"] >> bq_op_task_dict01[f"bq_03_02_reaseguro_carga_aux_sin_xloss"] >> bq_op_task_dict01[f"bq_03_02_01_reaseguro_carga_aux_sin_xloss_reporte"] >> bq_op_task_dict01[f"bq_03_04_reaseguro_carga_sin_xloss"]
    [inicio_reaseguros,fin_primas] >> bq_op_task_dict01[f"bq_03_03_reaseguro_distribucion_prima_reasegurada"] >> bq_op_task_dict01[f"bq_03_04_reaseguro_carga_sin_xloss"]
    bq_op_task_dict01[f"bq_03_04_reaseguro_carga_sin_xloss"] >> [bq_op_task_dict01[f"bq_03_05_reaseguro_carga_pnra"],bq_op_task_dict01[f"bq_03_06_reaseguro_carga_xloss"]] >> fin_reaseguros
    bq_op_task_dict01[f"bq_01_02_prima_distribucion_prima"] >> bq_op_task_dict01[f"bq_03_06_reaseguro_carga_xloss"] >> fin_reaseguros
    fin_reaseguros >> bq_op_task_dict01[f"bq_03_07_distribucion_prima_cedida"] >> fin_lineas
    
    ## DSCTO REASEUGRO
    inicio_lineas >> inicio_dscto_reaseguro >> bq_op_task_dict01[f"bq_04_01_dscto_reaseguro_carga"]
    bq_op_task_dict01[f"bq_03_07_distribucion_prima_cedida"] >> bq_op_task_dict01[f"bq_04_01_dscto_reaseguro_carga"] >> bq_op_task_dict01[f"bq_04_01_01_dscto_reaseguro_carga_reporte"] >> fin_dscto_reaseguro
    fin_dscto_reaseguro >> fin_lineas

    ## COMISIONES
    inicio_lineas >> inicio_comision >> bq_op_task_dict01[f"bq_05_01_comision_carga_directa"] >> [bq_op_task_dict01[f"bq_05_03_comision_carga_cnt_x_distribucion"],bq_op_task_dict01[f"bq_05_04_comision_carga_broker_y_pu"],bq_op_task_dict01[f"bq_05_05_comision_carga_ffvv"],bq_op_task_dict01[f"bq_05_06_comision_carga_pnra"],bq_op_task_dict01[f"bq_05_07_comision_distribucion_pauta_digital"],bq_op_task_dict01[f"bq_05_08_comision_carga_pauta_digital"]]
    [bq_op_task_dict01[f"bq_01_02_prima_distribucion_prima"]] >> bq_op_task_dict01[f"bq_03_07_distribucion_prima_cedida"] >> bq_op_task_dict01[f"bq_05_03_comision_carga_cnt_x_distribucion"] >> bq_op_task_dict01[f"bq_05_03_01_comision_carga_cnt_x_distribucion_reporte"] >> fin_comision
    bq_op_task_dict01[f"bq_01_02_prima_distribucion_prima"] >> bq_op_task_dict01[f"bq_05_04_comision_carga_broker_y_pu"] >> fin_comision
    bq_op_task_dict01[f"bq_01_05_prima_distribucion_prima_emitida"] >> bq_op_task_dict01[f"bq_05_05_comision_carga_ffvv"] >> fin_comision
    bq_op_task_dict01[f"bq_05_06_comision_carga_pnra"] >> fin_comision
    bq_op_task_dict01[f"bq_05_07_comision_distribucion_pauta_digital"] >> bq_op_task_dict01[f"bq_05_08_comision_carga_pauta_digital"] >> fin_comision
    fin_comision >> fin_lineas

    ## SINIESTROS
    inicio_lineas >> inicio_siniestros_sin_recuperos >> bq_op_task_dict01[f"bq_06_01_01_siniestros_sin_recuperos_variacion_rm_sepelios_suspendidos"] >> bq_op_task_dict01[f"bq_06_01_03_siniestros_sin_recuperos_pagos_enviados_asc"]
    bq_op_task_dict01[f"bq_06_01_01_siniestros_sin_recuperos_variacion_rm_sepelios_suspendidos"]>> bq_op_task_dict01[f"bq_06_01_04_siniestros_sin_recuperos_variacion_retencion_suspendidos_asc"]
    bq_op_task_dict01[f"bq_06_01_01_siniestros_sin_recuperos_variacion_rm_sepelios_suspendidos"]>> bq_op_task_dict01[f"bq_06_01_02_siniestros_sin_recuperos_ax_ae_asi"]
    bq_op_task_dict01[f"bq_06_01_03_siniestros_sin_recuperos_pagos_enviados_asc"] >> bq_op_task_dict01[f"bq_06_01_06_siniestros_sin_recuperos_ajuste_distrib_pol_vigente"]>> bq_op_task_dict01[f"bq_06_01_07_siniestros_sin_recuperos_otros"]
    bq_op_task_dict01[f"bq_06_01_04_siniestros_sin_recuperos_variacion_retencion_suspendidos_asc"] >> bq_op_task_dict01[f"bq_06_01_06_siniestros_sin_recuperos_ajuste_distrib_pol_vigente"]
    bq_op_task_dict01[f"bq_06_01_02_siniestros_sin_recuperos_ax_ae_asi"] >> bq_op_task_dict01[f"bq_06_01_06_siniestros_sin_recuperos_ajuste_distrib_pol_vigente"]
    [fin_primas, fin_ajuste_rrc] >> bq_op_task_dict01[f"bq_06_02_01_01_siniestros_ocurridos_y_no_reportados_registros_prima_devengada"] 
    bq_op_task_dict01[f"bq_06_01_06_siniestros_sin_recuperos_ajuste_distrib_pol_vigente"] >> bq_op_task_dict01[f"bq_06_01_05_01_siniestros_sin_recuperos_reporte"] >> bq_op_task_dict01[f"bq_06_02_01_01_siniestros_ocurridos_y_no_reportados_registros_prima_devengada"]
    bq_op_task_dict01[f"bq_06_02_01_01_siniestros_ocurridos_y_no_reportados_registros_prima_devengada"] >> bq_op_task_dict01[f"bq_06_02_01_02_siniestros_ocurridos_y_no_reportados_distribucion_ibnr_desgrav_mes"] >> [bq_op_task_dict01[f"bq_06_02_02_siniestros_ocurridos_y_no_reportados_ajuste"], bq_op_task_dict01[f"bq_06_02_03_siniestros_ocurridos_y_no_reportados_distribucion_ibnr_desgrav"]] >> bq_op_task_dict01[f"bq_06_02_04_siniestros_ocurridos_y_no_reportados_ajuste_desgrav"] >> bq_op_task_dict01[f"bq_06_02_02_01_siniestros_ocurridos_y_no_reportados_ajuste_reporte"] >> bq_op_task_dict01[f"bq_07_01_01_siniestros_cedidos_con_ibnr"]
    bq_op_task_dict01[f"bq_07_01_01_siniestros_cedidos_con_ibnr"] >> bq_op_task_dict01[f"bq_07_01_02_01_siniestros_cedidos_con_ibnr_bolsa_reporte"] >> bq_op_task_dict01[f"bq_07_01_02_02_siniestros_cedidos_con_ibnr_desgrav"] >> bq_op_task_dict01[f"bq_06_02_05_siniestros_ocurridos_y_no_reportados_ajuste_bolsa"] >> bq_op_task_dict01[f"bq_07_01_02_siniestros_cedidos_con_ibnr_bolsa"] >> fin_siniestros_sin_recuperos >> fin_lineas

    ## SINIESTROS CEDIDOS
    inicio_lineas >> inicio_siniestros_cedidos >> bq_op_task_dict01[f"bq_07_02_01_siniestros_cedidos_sin_ibnr_suma_asegurada_ap"]
    bq_op_task_dict01[f"bq_07_02_01_siniestros_cedidos_sin_ibnr_suma_asegurada_ap"] >> bq_op_task_dict01[f"bq_07_02_02_siniestros_cedidos_sin_ibnr_vida"]
    bq_op_task_dict01[f"bq_07_02_02_siniestros_cedidos_sin_ibnr_vida"] >> bq_op_task_dict01[f"bq_07_02_03_siniestros_cedidos_sin_ibnr_bolsa"]
    bq_op_task_dict01[f"bq_07_02_03_siniestros_cedidos_sin_ibnr_bolsa"] >> bq_op_task_dict01[f"bq_07_02_02_01_siniestros_cedidos_sin_ibnr_vida_reporte"] >> bq_op_task_dict01[f"bq_07_02_04_siniestros_cedidos_sin_ibnr_accidentes"]
    bq_op_task_dict01[f"bq_07_02_04_siniestros_cedidos_sin_ibnr_accidentes"] >> fin_siniestros_cedidos
    fin_siniestros_cedidos >> fin_lineas

    ## GASTO TECNICO
    inicio_lineas >> inicio_gasto_tecnico >> bq_op_task_dict01[f"bq_08_01_gasto_tecnico_asesorias"]
    bq_op_task_dict01[f"bq_01_02_prima_distribucion_prima"] >> bq_op_task_dict01[f"bq_08_02_gasto_tecnico_varios_por_prima"]
    bq_op_task_dict01[f"bq_03_07_distribucion_prima_cedida"] >> bq_op_task_dict01[f"bq_08_06_gasto_tecnico_participacion_utilidades"]
    bq_op_task_dict01[f"bq_08_01_gasto_tecnico_asesorias"] >> bq_op_task_dict01[f"bq_08_01_01_gastos_tecnicos_reporte"] >> [bq_op_task_dict01[f"bq_08_02_gasto_tecnico_varios_por_prima"] , bq_op_task_dict01[f"bq_08_03_gasto_tecnico_peritaje"] , bq_op_task_dict01[f"bq_08_04_gasto_tecnico_distribucion_vencimiento_poliza"] , bq_op_task_dict01[f"bq_08_05_gasto_tecnico_servicio_cobranza"] , bq_op_task_dict01[f"bq_08_06_gasto_tecnico_participacion_utilidades"]] >> fin_gasto_tecnico >> fin_lineas

    ## PCD - PROVISION COBRANZA DUDOSA
    inicio_lineas >> inicio_pcd >> bq_op_task_dict01[f"bq_09_01_pcd_carga_directo"] >> bq_op_task_dict01[f"bq_09_02_pcd_carga_aman"]
    bq_op_task_dict01[f"bq_01_02_prima_distribucion_prima"] >> bq_op_task_dict01[f"bq_09_02_pcd_carga_aman"] >> bq_op_task_dict01[f"bq_09_02_01_pcd_carga_aman_reporte"] >> fin_pcd >> fin_lineas
     
    ## PATRIMONIOS
    inicio_lineas >> inicio_patrimonios >> bq_op_task_dict01[f"bq_10_01_patrimonios_patrimonio_mensualizado"] >> [bq_op_task_dict01[f"bq_10_02_patrimonios_distribucion_prima_aux"],bq_op_task_dict01[f"bq_10_04_patrimonios_distribucion_patrimonio_rm"]]
    bq_op_task_dict01[f"bq_01_06_prima_distribucion_prima_ltm"] >> bq_op_task_dict01[f"bq_10_02_patrimonios_distribucion_prima_aux"] >> bq_op_task_dict01[f"bq_10_03_patrimonios_distribucion_patrimonio_prima_rrc"] >> bq_op_task_dict01[f"bq_10_04_patrimonios_distribucion_patrimonio_rm"]
    bq_op_task_dict01[f"bq_02_09_ajuste_rrc_distribucion_ajuste_rrc"] >> bq_op_task_dict01[f"bq_10_03_patrimonios_distribucion_patrimonio_prima_rrc"]
    bq_op_task_dict01[f"bq_02_13_ajuste_rrc_distribucion_prov_reserv_matematica_saldo"] >> bq_op_task_dict01[f"bq_10_04_patrimonios_distribucion_patrimonio_rm"] >> fin_patrimonios
    fin_patrimonios >> fin_lineas

    ## AJUSTE RT GERENCIAL
    inicio_lineas >> inicio_ajuste_rt_gerencial >> bq_op_task_dict01[f"bq_11_01_ajuste_rt_gerencial_carga_por_prima_ltm"]
    bq_op_task_dict01[f"bq_01_06_prima_distribucion_prima_ltm"] >> bq_op_task_dict01[f"bq_11_01_ajuste_rt_gerencial_carga_por_prima_ltm"]
    bq_op_task_dict01[f"bq_02_13_ajuste_rrc_distribucion_prov_reserv_matematica_saldo"] >> [bq_op_task_dict01[f"bq_11_02_ajuste_rt_gerencial_carga_descalce"],bq_op_task_dict01[f"bq_11_03_ajuste_rt_gerencial_carga_otros"],bq_op_task_dict01[f"bq_11_04_ajuste_rt_gerencial_carga_tecnico"],bq_op_task_dict01[f"bq_11_05_ajuste_rt_gerencial_carga_inflacion"]]
    bq_op_task_dict01[f"bq_11_01_ajuste_rt_gerencial_carga_por_prima_ltm"] >> [bq_op_task_dict01[f"bq_11_03_ajuste_rt_gerencial_carga_otros"],bq_op_task_dict01[f"bq_11_04_ajuste_rt_gerencial_carga_tecnico"],bq_op_task_dict01[f"bq_11_05_ajuste_rt_gerencial_carga_inflacion"]]
    bq_op_task_dict01[f"bq_11_01_ajuste_rt_gerencial_carga_por_prima_ltm"] >> bq_op_task_dict01[f"bq_11_02_ajuste_rt_gerencial_carga_descalce"] >> fin_ajuste_rt_gerencial
    [bq_op_task_dict01[f"bq_11_03_ajuste_rt_gerencial_carga_otros"],bq_op_task_dict01[f"bq_11_04_ajuste_rt_gerencial_carga_tecnico"],bq_op_task_dict01[f"bq_11_05_ajuste_rt_gerencial_carga_inflacion"]] >> fin_ajuste_rt_gerencial
    fin_ajuste_rt_gerencial >> fin_lineas

    ## GASTOS RT
    inicio_lineas >> inicio_gastos_rt >> bq_op_task_dict01[f"bq_12_01_gastos_rt_carga_por_prima_ltm"]
    bq_op_task_dict01[f"bq_01_06_prima_distribucion_prima_ltm"] >> bq_op_task_dict01[f"bq_12_01_gastos_rt_carga_por_prima_ltm"]
    bq_op_task_dict01[f"bq_01_06_prima_distribucion_prima_ltm"] >> bq_op_task_dict01[f"bq_12_03_gastos_rt_carga_por_venta_nueva"]
    bq_op_task_dict01[f"bq_12_01_gastos_rt_carga_por_prima_ltm"] >> [bq_op_task_dict01[f"bq_12_02_gastos_rt_carga_por_nro_polizas_canal_sub_canal"] ,bq_op_task_dict01[f"bq_12_03_gastos_rt_carga_por_venta_nueva"] ,bq_op_task_dict01[f"bq_12_04_gastos_rt_carga_por_nro_siniestros"] ,bq_op_task_dict01[f"bq_12_05_gastos_rt_carga_por_reserva_matematica"] ] >> fin_gastos_rt >> fin_lineas
    bq_op_task_dict01[f"bq_02_13_ajuste_rrc_distribucion_prov_reserv_matematica_saldo"] >> bq_op_task_dict01[f"bq_12_05_gastos_rt_carga_por_reserva_matematica"]

    ## INVERSIONES
    inicio_lineas >> inicio_inversiones >> bq_op_task_dict01[f"bq_13_01_inversiones_carga_por_prima_ltm"]
    bq_op_task_dict01[f"bq_01_06_prima_distribucion_prima_ltm"] >> bq_op_task_dict01[f"bq_13_01_inversiones_carga_por_prima_ltm"]
    bq_op_task_dict01[f"bq_13_01_inversiones_carga_por_prima_ltm"] >> bq_op_task_dict01[f"bq_13_01_inversiones_carga_por_prima_ltm_reporte"] >> fin_inversiones >> fin_lineas

    ## OTROS INGRESOS FINANCIEROS
    inicio_lineas >> inicio_otros_ingresos_financieros >> bq_op_task_dict01[f"bq_14_01_otros_ingresos_financieros_carga_por_prima_ltm"]
    bq_op_task_dict01[f"bq_01_06_prima_distribucion_prima_ltm"] >> bq_op_task_dict01[f"bq_14_01_otros_ingresos_financieros_carga_por_prima_ltm"] >> bq_op_task_dict01[f"bq_14_01_otros_ingresos_financieros_carga_por_prima_ltm_reporte"] >> fin_otros_ingresos_financieros >> fin_lineas
                                 
    fin_lineas >> bq_op_task_dict01[f"bq_resultado_tecnico_certificado_vida"] >> bq_op_task_dict01[f"bq_borrar_tablas_temporales"] >> end
    
    # Control de errores
    start >> end
 
    for key in bq_op_task_dict01.keys():
        bq_op_task_dict01[key] >> end
        if key not in ["bq_borrar_tablas_temporales", "bq_resultado_tecnico_certificado_vida"]:
            bq_op_task_dict01[key] >> fin_lineas
        

    end >> update_dependencies_status_error()
    end >> update_dependencies_status_ok()

flujo_tareas()
