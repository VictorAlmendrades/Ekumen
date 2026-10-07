/*
################################################################################
## Script:     01_01_prima_carga_directo.sql
## Proyecto:   Resultado técnico por certificado
## Propósito:  Carga la prima contable directa a nivel de póliza/certificado.
##             Resuelve el producto contable contra uno u otro ERP según la
##             fecha de corte de migración, y convierte moneda cuando aplica.
##
## Fuentes:    analytics_produccion.produccion
##             analytics_finanzas.{tmp}_producto_vida_no_ramo
##             analytics_finanzas.{tmp}_ramo_tec_a_cont_vida
##             analytics_finanzas.{tmp}_producto_tec_a_cont_vida
##             analytics_finanzas.{tmp}_poliza_vida
##
## Destino:    analytics_finanzas.{tmp}_prima   (particionada por periodo)
################################################################################
*/
DECLARE periodo_inicio DATE DEFAULT DATE_SUB('{periodo_inicio}', INTERVAL 11 MONTH);
DECLARE periodo_fin DATE DEFAULT '{periodo_fin}'; ##19
DECLARE erp_periodo_corte DATE DEFAULT '{erp_periodo_corte}';
DECLARE v_periodo DATE DEFAULT DATE_TRUNC(CURRENT_DATE('America/Lima'), MONTH);

DROP TABLE IF EXISTS `{project_analytics}.analytics_finanzas.{tmp}_prima`;

CREATE OR REPLACE TABLE `{project_analytics}.analytics_finanzas.{tmp}_prima`
PARTITION BY periodo AS
WITH
producto AS (
    SELECT *
    FROM `{project_analytics}.analytics_finanzas.{tmp}_producto_vida_no_ramo`

),
 ramo_tec_a_cont AS (
    SELECT * 
    FROM `{project_analytics}.analytics_finanzas.{tmp}_ramo_tec_a_cont_vida`

),producto_tec_a_cont AS (
    SELECT *
    FROM `{project_analytics}.analytics_finanzas.{tmp}_producto_tec_a_cont_vida` 


), poliza_anl AS (
    SELECT po.periodo,po.id_poliza,po.id_certificado,po.id_poliza_origen,po.id_contratante,po.id_titular
    FROM `{project_analytics}.analytics_finanzas.{tmp}_poliza_vida` po
)
SELECT
    CONCAT(DATE_TRUNC(rp.fec_procesamiento, MONTH), 'Primas', 'Primas', 'Directo', rp.ram_id_ramo_tecnico, rp.id_producto, 'Directo', rp.cod_producto_origen, rp.pol_id_poliza,
        IFNULL(rp.cer_id_certificado, '0') , IFNULL(po.id_poliza_origen, '0'), rp.tip_registro_primas, rp.pol_id_moneda_origen) AS id_primas,
    rp.periodo AS periodo,
    'Primas' AS des_cuenta_agrupador_1,
    'Primas' AS des_cuenta_agrupador_2,
    'Directo' AS des_concepto_cuenta,
    rerp.id_ramo_contable,
    rp.ram_id_ramo_tecnico AS id_ramo,
    rp.id_producto,
    IF(rp.periodo < erp_periodo_corte ,perp.id_producto_cont_erp_legado,perp.id_producto_cont_erp_nuevo) AS id_producto_erp,
    'Directo' AS id_origen,
    rp.pol_id_poliza AS id_poliza,
    IFNULL(rp.cer_id_certificado, '0') AS id_certificado,
    IFNULL(po.id_contratante, '0') AS id_contratante,
    IFNULL(po.id_titular, '0') AS id_titular,
    rp.cod_producto_origen AS id_producto_origen,
    IFNULL(po.id_poliza_origen, '0') AS num_poliza_origen,
    rp.tip_registro_primas,
    rp.pol_id_moneda_origen  AS id_moneda,
    SUM(IF(rp.pol_id_moneda_origen = 'SOL',round(rp.tc_a_sol*(rp.mnt_prima_np_usd + rp.mnt_derecho_emision_usd),2),rp.mnt_prima_np_usd + rp.mnt_derecho_emision_usd)) AS mnt_moneda,
    SUM(rp.mnt_prima_np_usd + rp.mnt_derecho_emision_usd) AS mnt_usd,
    CAST(NULL AS STRING) AS agrupacion_n1,
    CAST(NULL AS STRING) AS agrupacion_n2,
    CAST(NULL AS STRING) AS agrupacion_n3,
    CAST(NULL AS STRING) AS agrupacion_n4,
    CAST(NULL AS STRING) AS agrupacion_n5,
    'PRIMAS' AS des_componente,
    'DIRECTO' AS des_sub_componente,
    'DIRECTO' AS des_origen_data
FROM `{project_analytics}.analytics_produccion.produccion` rp
INNER JOIN producto p
    ON rp.id_producto = p.id_producto
        AND p.ord = 1
LEFT JOIN producto_tec_a_cont perp
    ON rp.id_producto = perp.id_producto and perp.codcia = '01'
LEFT JOIN ramo_tec_a_cont rerp
    ON rp.ram_id_ramo_tecnico = rerp.id_ramo and rerp.codcia = '01'
LEFT JOIN poliza_anl po
    ON rp.pol_id_poliza = po.id_poliza
    AND rp.cer_id_certificado = po.id_certificado
WHERE
    (rp.periodo BETWEEN periodo_inicio AND periodo_fin)
    AND rp.tip_registro_primas <> 'ASIENTOS MANUALES'
    AND rp.ind_acumulado ='PRODUCCION'
    AND rp.ram_id_ramo_tecnico NOT IN ('AX-ASME')
GROUP BY 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18
;


