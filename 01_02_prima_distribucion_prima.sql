/*
################################################################################
## Script:     01_02_prima_distribucion_prima.sql
## Proyecto:   Resultado técnico por certificado
## Propósito:  Genera las bases de distribución a nivel póliza/certificado que
##             usan el resto de los componentes del resultado técnico.
##
##             Calcula 14 totales de prima con funciones ventana, de la
##             jerarquía más granular a la más general. La cascada permite
##             degradar la base de distribución cuando no hay prima al nivel
##             de detalle y evita divisiones entre cero.
##
## Fuentes:    analytics_finanzas.{tmp}_prima
##             analytics_finanzas.{tmp}_producto_vida
##             analytics_finanzas.{tmp}_poliza_vida
##
## Destino:    analytics_finanzas.{tmp}_distribucion_prima  (part. por periodo)
################################################################################
*/
DECLARE v_periodo DATE DEFAULT DATE_TRUNC(CURRENT_DATE('America/Lima'), MONTH);
DECLARE periodo_inicio DATE DEFAULT '{periodo_inicio}';
DECLARE periodo_fin DATE DEFAULT '{periodo_fin}';

DROP TABLE IF EXISTS `{project_analytics}.analytics_finanzas.{tmp}_distribucion_prima`;

CREATE OR REPLACE TABLE `{project_analytics}.analytics_finanzas.{tmp}_distribucion_prima`
PARTITION BY periodo AS
WITH periodo AS (
    SELECT
        FORMAT_DATE('%Y%m', d) AS periodo,
        FORMAT_DATE('%Y%m', DATE_TRUNC(DATE_SUB(d, INTERVAL 2 MONTH), MONTH)) AS periodo_ant,
        CAST(EXTRACT(YEAR FROM d) AS INT64) AS anio,
        EXTRACT(MONTH FROM d) AS mes,
        DATE_TRUNC(d, MONTH) AS fec_inicio,
        LAST_DAY(d, MONTH) AS fec_fin,
        DATE_TRUNC(DATE_SUB(d, INTERVAL 2 MONTH), MONTH) AS fec_inicio_ant,
        LAST_DAY(DATE_SUB(d, INTERVAL 2 MONTH), MONTH) AS fec_fin_ant
    FROM (
        SELECT *
        FROM UNNEST(GENERATE_DATE_ARRAY(periodo_inicio, periodo_fin, INTERVAL 1 MONTH)) AS d)
    GROUP BY periodo, periodo_ant, anio, mes, fec_inicio, fec_fin, fec_inicio_ant, fec_fin_ant

), producto AS (
    SELECT *
    FROM `{project_analytics}.analytics_finanzas.{tmp}_producto_vida` 

), poliza AS (
    SELECT DISTINCT po.id_poliza
    FROM `{project_analytics}.analytics_finanzas.{tmp}_poliza_vida` po

), prima as
-- Se totaliza la tabla prima por que un certificado puede tener 
-- mas de un movimiento en el periodo : emision, anulacion
-- necesitamos el monto total para calcular bien los porcentajes 
(
    Select  pr.periodo,
            pr.id_ramo_contable,
            pr.id_producto,
            pr.id_poliza,
            IFNULL(pr.id_certificado, '0') AS id_certificado,
            pr.id_moneda,
            Sum(pr.mnt_moneda) mnt_moneda,
            Sum(pr.mnt_usd) mnt_usd           
    FROM `{project_analytics}.analytics_finanzas.{tmp}_prima` pr
    Where pr.des_cuenta_agrupador_1 = 'Primas'
    And pr.des_cuenta_agrupador_2 = 'Primas'
    And pr.des_concepto_cuenta = 'Directo'
    And pr.id_origen = 'Directo'
    And pr.mnt_moneda <> 0
    Group by all    
)

    SELECT
        per.fec_inicio AS periodo_trim,
        pr.periodo,
        p.agrupacion_n1,
        p.agrupacion_n2,
        p.agrupacion_n3,
        p.agrupacion_n4,
        p.agrupacion_n5,
        pr.id_ramo_contable,
        pr.id_producto,
        pr.id_poliza,
        IFNULL(pr.id_certificado, '0') AS id_certificado,
        pr.id_moneda,
        pr.mnt_moneda,
        pr.mnt_usd,
        SUM(pr.mnt_moneda) OVER (
                PARTITION BY
                    per.fec_inicio,
                    pr.periodo,
                    p.agrupacion_n1,
                    p.agrupacion_n2,
                    p.agrupacion_n3,
                    p.agrupacion_n4,
                    p.agrupacion_n5,
                    pr.id_ramo_contable,
                    pr.id_producto,
                    pr.id_moneda
                ORDER BY pr.id_poliza, pr.id_certificado
                ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) as porc_nivel_1,
        SUM(pr.mnt_moneda) OVER (
                PARTITION BY
                    per.fec_inicio,
                    p.agrupacion_n1,
                    p.agrupacion_n2,
                    p.agrupacion_n3,
                    p.agrupacion_n4,
                    p.agrupacion_n5,
                    pr.id_ramo_contable,
                    pr.id_producto,
                    pr.id_moneda
                ORDER BY pr.id_poliza, pr.id_certificado
                ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) as porc_nivel_2,
        SUM(pr.mnt_usd) OVER (
                PARTITION BY
                    per.fec_inicio,
                    pr.periodo,
                    p.agrupacion_n1,
                    p.agrupacion_n2,
                    p.agrupacion_n3,
                    p.agrupacion_n4,
                    p.agrupacion_n5,
                    pr.id_ramo_contable,
                    pr.id_producto
                ORDER BY pr.id_poliza, pr.id_certificado
                ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) as porc_nivel_3,
        SUM(pr.mnt_usd) OVER (
                PARTITION BY
                    per.fec_inicio,
                    p.agrupacion_n1,
                    p.agrupacion_n2,
                    p.agrupacion_n3,
                    p.agrupacion_n4,
                    p.agrupacion_n5,
                    pr.id_ramo_contable,
                    pr.id_producto
                ORDER BY pr.id_poliza, pr.id_certificado
                ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) as porc_nivel_4,
        SUM(pr.mnt_usd) OVER (
                PARTITION BY
                    per.fec_inicio,
                    pr.periodo,
                    p.agrupacion_n1,
                    p.agrupacion_n2,
                    p.agrupacion_n3,
                    p.agrupacion_n4,
                    p.agrupacion_n5,
                    pr.id_ramo_contable
                ORDER BY pr.id_poliza, pr.id_certificado
                ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) as porc_nivel_5,
        SUM(pr.mnt_usd) OVER (
                PARTITION BY
                    per.fec_inicio,
                    p.agrupacion_n1,
                    p.agrupacion_n2,
                    p.agrupacion_n3,
                    p.agrupacion_n4,
                    p.agrupacion_n5,
                    pr.id_ramo_contable
                ORDER BY pr.id_poliza, pr.id_certificado
                ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) as porc_nivel_6,
        SUM(pr.mnt_usd) OVER (
                PARTITION BY
                    per.fec_inicio,
                    pr.periodo,
                    p.agrupacion_n1,
                    p.agrupacion_n2,
                    p.agrupacion_n3,
                    p.agrupacion_n4,
                    p.agrupacion_n5
                ORDER BY pr.id_poliza, pr.id_certificado
                ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) as porc_nivel_7,
        SUM(pr.mnt_usd) OVER (
                PARTITION BY
                    per.fec_inicio,
                    p.agrupacion_n1,
                    p.agrupacion_n2,
                    p.agrupacion_n3,
                    p.agrupacion_n4,
                    p.agrupacion_n5
                ORDER BY pr.id_poliza, pr.id_certificado
                ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) as porc_nivel_8,
        SUM(pr.mnt_usd) OVER (
                PARTITION BY
                    per.fec_inicio,
                    pr.periodo,
                    p.agrupacion_n1,
                    p.agrupacion_n2,
                    p.agrupacion_n3,
                    p.agrupacion_n4
                ORDER BY pr.id_poliza, pr.id_certificado
                ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) as porc_nivel_9,
        SUM(pr.mnt_usd) OVER (
                PARTITION BY
                    per.fec_inicio,
                    p.agrupacion_n1,
                    p.agrupacion_n2,
                    p.agrupacion_n3,
                    p.agrupacion_n4
                ORDER BY pr.id_poliza, pr.id_certificado
                ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) as porc_nivel_10,
        SUM(pr.mnt_usd) OVER (
                PARTITION BY
                    per.fec_inicio,
                    pr.periodo,
                    p.agrupacion_n1,
                    p.agrupacion_n2,
                    p.agrupacion_n3
                ORDER BY pr.id_poliza, pr.id_certificado
                ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS porc_nivel_11,
        SUM(pr.mnt_usd) OVER (
                PARTITION BY
                    per.fec_inicio,
                    p.agrupacion_n1,
                    p.agrupacion_n2,
                    p.agrupacion_n3,
                ORDER BY pr.id_poliza, pr.id_certificado
                ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS porc_nivel_12,
        SUM(pr.mnt_usd) OVER (
                PARTITION BY
                    per.fec_inicio,
                    pr.periodo,
                    p.agrupacion_n1,
                    p.agrupacion_n2
                ORDER BY pr.id_poliza, pr.id_certificado
                ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS porc_nivel_13,
        SUM(pr.mnt_usd) OVER (
                PARTITION BY
                    per.fec_inicio,
                    p.agrupacion_n1,
                    p.agrupacion_n2
                ORDER BY pr.id_poliza, pr.id_certificado
                ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS porc_nivel_14
    FROM prima pr
    INNER JOIN poliza po -- solo polizas que existen en ANL
        ON pr.id_poliza = po.id_poliza
    LEFT JOIN periodo per
        ON 1 = 1
    INNER JOIN producto p
      ON pr.id_producto = p.id_producto
        AND p.ord = 1
    WHERE pr.periodo >= per.fec_inicio_ant
        AND pr.periodo <= per.fec_fin
