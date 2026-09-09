/*
################################################################################
## Script:     00_carga_inicial.sql
## Proyecto:   Resultado técnico por certificado
## Propósito:  Construye las tablas de dimensión compartidas por el resto del
##             pipeline: catálogo de producto, equivalencias técnico/contable,
##             póliza vigente y tipo de cambio.
##
##             Lee las fuentes con FOR SYSTEM_TIME AS OF, fijando el timestamp
##             en la última corrida exitosa del proceso upstream, para
##             garantizar una lectura consistente.
##
## Fuentes:    analytics_finanzas.resultado_tecnico_gerencial
##             staging_producto.producto / staging_producto.ramo
##             analytics_produccion.poliza
##             staging_finanzas.tasa_cambio
##             staging_resultado_tecnico.dato_financiero_consolidado
##
## Destino:    analytics_finanzas.{tmp}_producto_vida
##             analytics_finanzas.{tmp}_producto_tec_a_cont_vida
##             analytics_finanzas.{tmp}_ramo_tec_a_cont_vida
##             analytics_finanzas.{tmp}_poliza_vida
##             analytics_finanzas.{tmp}_tasa_cambio_vida
################################################################################
*/

DECLARE periodo_inicio DATE DEFAULT '{periodo_inicio}';
DECLARE periodo_fin DATE DEFAULT '{periodo_fin}';
DECLARE v_periodo DATE DEFAULT '{v_periodo}';
DECLARE time_travel_rtg TIMESTAMP;
DECLARE time_travel_dato_financiero TIMESTAMP;

#Obtener la última fecha de ejecución del DAG del RTG dentro de los últimos 7 días sino, se toma la fecha de hace 1 día.
SET time_travel_rtg = (
                SELECT IF(TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 7 DAY) < MAX(fecha_estado_fin_dag),
                            MAX(fecha_estado_fin_dag),
                            TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 1 DAY))
                FROM `{project_gobierno}.gobierno_monitoreo.config_dependencias_dag`
                WHERE
                    recurso = 'dag_upstream_rt_gerencial');

SET time_travel_dato_financiero = (
                SELECT IF(TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 7 DAY) < MAX(fecha_estado_fin_dag),
                            MAX(fecha_estado_fin_dag),
                            TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 1 DAY))
                FROM `{project_gobierno}.gobierno_monitoreo.config_dependencias_dag`
                WHERE
                    recurso = 'dag_upstream_dato_financiero');
                    

/***************************************** RESULTADO TECNICO GERENCIAL *****************************************/

DROP TABLE IF EXISTS `{project_analytics}.analytics_finanzas.{tmp}_resultado_tecnico_gerencial_vida`;

CREATE TABLE `{project_analytics}.analytics_finanzas.{tmp}_resultado_tecnico_gerencial_vida` AS
WITH rt_gerencial AS (
    SELECT 
        id_periodo,
        id_producto,
        id_ramo_contable,
        moneda,
        mnt_contabilizado_neto_usd,
        mnt_ingresado_neto_moneda,
        des_ramo_riesgo,
        des_ramo_contable,
        des_cuenta_agrupador_1,
        des_cuenta_agrupador_2,
        des_agrup_origen_asiento,
        des_cuenta_contable,
        des_categoria,
        des_lineas,
        des_glosa,
        ind_rtg,
        tip_cambio_sol_usd,
        mnt_ajuste_descalce_usd,
        mnt_ajuste_otro_usd,
        mnt_ajuste_tecnico_usd,
        mnt_ajuste_inflacion_usd,
        periodo,
        TIMESTAMP_ADD(time_travel_rtg, INTERVAL 5 HOUR) AS time_travel_rtg
    FROM `{project_analytics}.analytics_finanzas.resultado_tecnico_gerencial`
    --Se agregan 5 horas porque la fecha original está en UTC pero representa una hora local UTC-5
    FOR SYSTEM_TIME AS OF TIMESTAMP_ADD(time_travel_rtg, INTERVAL 5 HOUR)
    WHERE periodo >= periodo_inicio
    AND periodo <= periodo_fin
)
SELECT
    periodo,
    id_periodo,
    id_producto,
    id_ramo_contable,
    moneda,
    mnt_contabilizado_neto_usd,
    mnt_ingresado_neto_moneda,
    des_ramo_riesgo,
    des_ramo_contable,
    des_cuenta_agrupador_1,
    des_cuenta_agrupador_2,
    des_agrup_origen_asiento,
    des_cuenta_contable,
    des_categoria,
    des_lineas,
    des_glosa,
    ind_rtg,
    tip_cambio_sol_usd,
    mnt_ajuste_descalce_usd,
    mnt_ajuste_otro_usd,
    mnt_ajuste_tecnico_usd,
    mnt_ajuste_inflacion_usd,
    time_travel_rtg
FROM rt_gerencial;

/***************************************** PRODUCTO *****************************************/

DROP TABLE IF EXISTS `{project_analytics}.analytics_finanzas.{tmp}_producto_vida`;

CREATE TABLE `{project_analytics}.analytics_finanzas.{tmp}_producto_vida` AS
WITH producto AS (
    SELECT DISTINCT
        pj.nom_riesgo,
        p.id_producto,
        p.nom_producto,
        por.cod_producto,
        pj.agrupacion_n1,
        pj.agrupacion_n2,
        pj.agrupacion_n3,
        pj.agrupacion_n4,
        pj.agrupacion_n5,
        pj.id_ramo_contable,
        r.des_ramo_contable,
        r.id_ramo,
        r.des_ramo,
        ROW_NUMBER() OVER (PARTITION BY id_producto ORDER BY pj.id_ramo_contable, r.id_ramo) ord,
        ROW_NUMBER() OVER (PARTITION BY pj.agrupacion_n1, pj.agrupacion_n2, pj.agrupacion_n3, pj.agrupacion_n4, pj.agrupacion_n5 ORDER BY pj.id_ramo_contable, p.id_producto) ord1
    FROM `{project_staging}.staging_producto.producto` p
    LEFT JOIN UNNEST(producto_origen) por
    LEFT JOIN UNNEST(producto_jerarquia) pj
     INNER JOIN 
      ( select r1.id_ramo,r1.des_ramo,  r1.id_ramo_contable, r1.des_ramo_contable, er.cod_ramo_contable 
        from `{project_staging}.staging_producto.ramo` r1
       LEFT JOIN UNNEST(arr_equivalencia_contable) er) r
        ON 'S'||pj.id_ramo_contable in ( 'S'||r.id_ramo_contable, ifnull(cod_ramo_contable,'0'))
    WHERE por.id_origen = 'AX'
      AND pj.nom_riesgo = 'VIDA' 
)
    SELECT * FROM producto;

/************************** PRODUCTO TECNICO A CONTABLE**************************************/

DROP TABLE IF EXISTS `{project_analytics}.analytics_finanzas.{tmp}_producto_tec_a_cont_vida`;

CREATE TABLE `{project_analytics}.analytics_finanzas.{tmp}_producto_tec_a_cont_vida` AS
    SELECT
        id_producto,
        ec.id_companhia as codcia,
        MAX(CASE WHEN ec.id_origen = 'ERP_LEGADO' THEN ec.cod_producto_contable ELSE CAST(NULL AS STRING) END) AS id_producto_cont_erp_legado,
        MAX(CASE WHEN ec.id_origen = 'ERP_NUEVO' THEN ec.cod_producto_contable ELSE CAST(NULL AS STRING) END) AS id_producto_cont_erp_nuevo
    FROM  `{project_staging}.staging_producto.producto`
    LEFT JOIN UNNEST(arr_equivalencia_contable) ec
    WHERE
        ind_contable_principal = 'SI'
    GROUP BY ALL;

/************************** PRODUCTO CONTABLE A TECNICO**************************************/

DROP TABLE IF EXISTS `{project_analytics}.analytics_finanzas.{tmp}_producto_cont_a_tec_vida`;

CREATE TABLE `{project_analytics}.analytics_finanzas.{tmp}_producto_cont_a_tec_vida` AS
  SELECT DISTINCT
       ec.cod_producto_contable,
       id_producto,
       ec.id_companhia as codcia
    FROM `{project_staging}.staging_producto.producto`
    LEFT JOIN UNNEST(arr_equivalencia_contable) ec
    WHERE
        ind_tecnico_principal = 'SI';

/************************** RAMO CONTABLE A TECNICO *****************************************/

DROP TABLE IF EXISTS `{project_analytics}.analytics_finanzas.{tmp}_ramo_cont_a_tec_vida`;

CREATE TABLE `{project_analytics}.analytics_finanzas.{tmp}_ramo_cont_a_tec_vida` AS
    SELECT
        DISTINCT
        h.cod_ramo_contable AS id_ramo_contable,
        pr.id_ramo,
        pr.des_ramo,
        h.des_ramo_erp, 
        h.des_ramo_contable, 
        h.id_companhia AS codcia
        FROM `{project_staging}.staging_producto.ramo` pr
        LEFT JOIN UNNEST (arr_equivalencia_contable) AS h
            ON h.ind_tecnico_principal = 'SI'
        WHERE
            h.id_companhia = '01';
  
/************************** RAMO TECNICO A CONTABLE *****************************************/

DROP TABLE IF EXISTS `{project_analytics}.analytics_finanzas.{tmp}_ramo_tec_a_cont_vida`;

CREATE TABLE `{project_analytics}.analytics_finanzas.{tmp}_ramo_tec_a_cont_vida` AS
    SELECT
        DISTINCT
        pr.id_ramo,
		pr.des_ramo, -- se agrega la descripción del ramo
        h.cod_ramo_contable AS id_ramo_contable,
        h.des_ramo_erp, 
        h.des_ramo_contable, 
        h.id_companhia AS codcia,
        ROW_NUMBER() OVER (PARTITION BY h.cod_ramo_contable ORDER BY pr.id_ramo) AS ord,
        ROW_NUMBER() OVER (PARTITION BY h.des_ramo_contable ORDER BY pr.id_ramo) AS ord2
    FROM `{project_staging}.staging_producto.ramo` pr
    LEFT JOIN UNNEST (arr_equivalencia_contable) AS h
        ON h.ind_contable_principal = 'SI'
    WHERE
        h.id_companhia = '01';

/*************************************** POLIZA_ANL *****************************************/

DROP TABLE IF EXISTS `{project_analytics}.analytics_finanzas.{tmp}_poliza_vida`;

CREATE TABLE `{project_analytics}.analytics_finanzas.{tmp}_poliza_vida` AS
    SELECT
        po.periodo,
        po.id_poliza,
        po.id_poliza_origen,
        po.id_contratante,
        po.id_certificado,
        pr.cod_producto,
        po.id_titular,
        po.num_poliza,
        po.id_est_poliza,
        po.id_est_certificado,
        po.id_origen,
        po.id_producto,
        po.id_intermediario,
        po.id_canal,
        po.des_canal,
        po.fec_inicio_vigencia,
        po.fec_fin_vigencia,
        po.fec_inicio_vigencia_certificado,
        po.fec_fin_vigencia_certificado,
        po.fec_anulacion,
        po.fec_exclusion_certificado,
        po.ind_fronting,
        po.id_moneda,
        po.id_subcanal,
        po.des_subtipocanal,
        bi.id_equivalencia_canal,
        te.id_estructura_comercial,
        te.id_canal_equivalente,
        te.des_canal_equivalente,
        te.id_subcanal_equivalente,
        te.des_subcanal_equivalente,
        te.id_segmentacion_01,
        IF(te.id_subcanal_equivalente = '301', po.des_subtipocanal, des_segmentacion_01) AS des_segmentacion_01,
        te.id_segmentacion_02,
        te.des_segmentacion_02,
        te.cod_agrupacion_canal,
        te.des_agrupacion_canal
    FROM `{project_analytics}.analytics_produccion.poliza` po
    INNER JOIN `{project_analytics}.analytics_finanzas.{tmp}_producto_vida` pr
        ON po.id_producto = pr.id_producto
        AND pr.ord = 1
    LEFT JOIN `{project_staging}.staging_comercial.bitacora_mc_poliza` bi
        ON FORMAT_DATE('%Y%m', po.periodo) = bi.dsc_periodo
        AND po.id_poliza = bi.id_poliza
    LEFT JOIN `{project_staging}.staging_comercial.equivalencia_canal` te
        ON CAST(bi.id_equivalencia_canal AS STRING) = te.id_equivalencia_canal
    WHERE
        po.periodo = v_periodo
        AND IFNULL(po.id_poliza, '0') <> '0'
    QUALIFY ROW_NUMBER() OVER(PARTITION BY po.periodo,po.id_poliza,po.id_certificado ORDER BY po.id_titular) = 1;


/*************************************** TASA_CAMBIO *****************************************/

DROP TABLE IF EXISTS `{project_analytics}.analytics_finanzas.{tmp}_tasa_cambio_vida`;

CREATE TABLE `{project_analytics}.analytics_finanzas.{tmp}_tasa_cambio_vida` AS
    SELECT fec_tasa,
      MAX(IF(id_moneda_origen = 'SOL', val_tasa_cambio, 0)) AS tc_a_usd,
      MAX(IF(id_moneda_origen = 'USD', val_tasa_cambio, 0)) AS tc_a_sol
    FROM `{project_staging}.staging_finanzas.tasa_cambio`
    WHERE tip_tasa_cambio = 'M'
        AND ((id_moneda_origen = 'SOL'
            AND id_moneda_fin = 'USD')
          OR (id_moneda_origen = 'USD'
            AND id_moneda_fin = 'SOL'))
        --AND (fec_tasa BETWEEN periodo_inicio AND periodo_fin)
    GROUP BY fec_tasa;

/********************** PRODUCTO VIDA NO RAMO ******************************/

DROP TABLE IF EXISTS `{project_analytics}.analytics_finanzas.{tmp}_producto_vida_no_ramo`;

CREATE TABLE `{project_analytics}.analytics_finanzas.{tmp}_producto_vida_no_ramo` AS
WITH producto_no_ramo AS (
    SELECT DISTINCT
        pj.nom_riesgo,
        p.id_producto,
        p.nom_producto,
        por.cod_producto,
        pj.agrupacion_n1,
        pj.agrupacion_n2,
        pj.agrupacion_n3,
        pj.agrupacion_n4,
        pj.id_ramo_contable,
        row_number() over (partition by id_producto order by pj.id_ramo_contable) ord
    FROM `{project_staging}.staging_producto.producto` p
    LEFT JOIN UNNEST(producto_origen) por
    LEFT JOIN UNNEST(producto_jerarquia) pj
    WHERE por.id_origen = 'AX'
        AND nom_riesgo = 'VIDA'
) 
  SELECT * FROM producto_no_ramo;

/*************************************** trs_dato_financiero_consolidado *****************************************/

DROP TABLE IF EXISTS `{project_analytics}.analytics_finanzas.{tmp}_trs_dato_financiero_consolidado_vida`;

CREATE TABLE `{project_analytics}.analytics_finanzas.{tmp}_trs_dato_financiero_consolidado_vida` AS
SELECT 
    id_dato_financiero, 
    periodo, 
    cod_periodo, 
    des_riesgo_negocio, 
    tip_concepto_financiero, 
    cta_agrupador_n2, 
    des_agrupacion_n1, 
    des_agrupacion_n2, 
    des_agrupacion_n3, 
    des_agrupacion_n4, 
    des_agrupacion_n5, 
    id_ramo_contable, 
    des_ramo_contable, 
    id_canal, 
    des_canal, 
    des_segmento_empresa_persona, 
    id_intermediario, 
    des_intermediario, 
    id_producto, 
    cod_producto_origen, 
    des_producto, 
    id_subcanal,
    des_subcanal, 
    des_subtipocanal, 
    id_contratante, 
    des_contratante, 
    id_poliza, 
    ind_fronting, 
    segmento_empresa,
    tip_gasto,
    tip_distribucion, 
    mnt_ajuste_reserva_usd, 
    mnt_usd
FROM `{project_staging}.staging_resultado_tecnico.trs_dato_financiero_consolidado`
FOR SYSTEM_TIME AS OF TIMESTAMP_ADD(time_travel_dato_financiero, INTERVAL 5 HOUR)
WHERE 1=1 and
    (
        periodo >= periodo_inicio
        AND periodo <= periodo_fin
        AND upper(des_riesgo_negocio) in ( 'VIDA-AJUSTE', 'VIDA')
    )
    or
    (
        upper(des_riesgo_negocio) = 'VIDA'
        AND upper(tip_concepto_financiero) = 'SINIESTROS SIN RECUPEROS'
        AND TRIM(UPPER(tip_distribucion)) = 'SIN AJUSTE'
    )
;