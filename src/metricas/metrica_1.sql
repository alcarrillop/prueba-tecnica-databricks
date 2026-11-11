-- Métrica 1 (Sección 3 de enunciado.md): número de empleados contratados en 2021 por
-- departamento y cargo, dividido por trimestre, ordenado alfabéticamente por departamento
-- y cargo.
--
-- Parámetros (marcadores con nombre; el job los pasa a la tarea SQL):
--   catalogo, esquema   dónde están las tablas de origen y dónde quedan las de destino.
--   prefijo             prefijo de todos los nombres de tabla: vacío en el job,
--                       test_<sufijo>_ en las pruebas.
-- Lee:   <prefijo>plata_empleados, <prefijo>plata_departamentos, <prefijo>plata_cargos
-- Crea:  <prefijo>oro_contratados_trimestre_2021            tabla Delta (columnas exactas)
--        <prefijo>oro_contratados_trimestre_2021_ordenada   vista con el orden oficial
--
-- Decisiones (detalle en el README):
--   * Año y trimestre en UTC: la fecha se compara contra instantes fijos escritos en UTC
--     (TIMESTAMP con Z). Es exacto con cualquier zona horaria de sesión. year(fecha) o
--     quarter(fecha) usarían la zona de la sesión, y convert_timezone se equivoca en la hora
--     repetida de los cambios de horario de verano.
--   * Solo aparecen las combinaciones de departamento y cargo con al menos una contratación
--     en 2021 (los trimestres sin contrataciones quedan en 0).
--   * Se cuenta por ids y los nombres se agregan al final: dos ids con el mismo nombre no
--     se fusionan.
--   * Orden alfabético con la collation ES (orden del español: sin distinguir mayúsculas
--     ni tildes salvo para desempatar, la ñ después de la n). Una tabla Delta no garantiza
--     el orden al leerla, así que el orden oficial es el de la vista _ordenada. La tabla se
--     escribe ya ordenada solo como ayuda.
--   * Se reconstruye completa en cada corrida (CREATE OR REPLACE): mismo plata, mismo
--     resultado.
--
-- Formato del archivo: cada sentencia termina con ';' al final de una línea. No poner ';'
-- al final de una línea dentro de comentarios o textos (ejecutar.py separa por ahí).

-- CREATE OR REPLACE ... AS SELECT no admite lista de columnas: los tipos salen de la
-- consulta (departamento y cargo STRING, Q1 a Q4 BIGINT) y la descripción va en el COMMENT.
CREATE OR REPLACE TABLE IDENTIFIER(:catalogo || '.' || :esquema || '.' || :prefijo || 'oro_contratados_trimestre_2021')
COMMENT 'Métrica 1: empleados contratados en 2021 por departamento, cargo y trimestre, con año y trimestre en UTC (Q1 enero a marzo, Q2 abril a junio, Q3 julio a septiembre, Q4 octubre a diciembre). Solo combinaciones con contrataciones en 2021. Para leerla en orden usar la vista oro_contratados_trimestre_2021_ordenada (ORDER BY departamento COLLATE ES, cargo COLLATE ES).'
AS
WITH contrataciones_2021 AS (
  SELECT
    id_departamento,
    id_cargo,
    CASE
      WHEN fecha_contratacion < TIMESTAMP'2021-04-01T00:00:00Z' THEN 1
      WHEN fecha_contratacion < TIMESTAMP'2021-07-01T00:00:00Z' THEN 2
      WHEN fecha_contratacion < TIMESTAMP'2021-10-01T00:00:00Z' THEN 3
      ELSE 4
    END AS trimestre
  FROM IDENTIFIER(:catalogo || '.' || :esquema || '.' || :prefijo || 'plata_empleados')
  WHERE fecha_contratacion >= TIMESTAMP'2021-01-01T00:00:00Z'
    AND fecha_contratacion <  TIMESTAMP'2022-01-01T00:00:00Z'
),
por_trimestre AS (
  SELECT
    id_departamento,
    id_cargo,
    count_if(trimestre = 1) AS Q1,
    count_if(trimestre = 2) AS Q2,
    count_if(trimestre = 3) AS Q3,
    count_if(trimestre = 4) AS Q4
  FROM contrataciones_2021
  GROUP BY id_departamento, id_cargo
)
SELECT
  d.departamento,
  c.cargo,
  t.Q1,
  t.Q2,
  t.Q3,
  t.Q4
FROM por_trimestre AS t
JOIN IDENTIFIER(:catalogo || '.' || :esquema || '.' || :prefijo || 'plata_departamentos') AS d
  ON d.id = t.id_departamento
JOIN IDENTIFIER(:catalogo || '.' || :esquema || '.' || :prefijo || 'plata_cargos') AS c
  ON c.id = t.id_cargo
ORDER BY d.departamento COLLATE ES, c.cargo COLLATE ES;

-- La vista guarda el texto de su consulta, que no admite marcadores de parámetro: se arma
-- con el nombre ya resuelto y se crea con EXECUTE IMMEDIATE.
EXECUTE IMMEDIATE
  'CREATE OR REPLACE VIEW ' || :catalogo || '.' || :esquema || '.' || :prefijo || 'oro_contratados_trimestre_2021_ordenada'
  || ' COMMENT "Métrica 1 en el orden oficial: departamento y cargo en orden alfabético del español (collation ES)."'
  || ' AS SELECT departamento, cargo, Q1, Q2, Q3, Q4 FROM '
  || :catalogo || '.' || :esquema || '.' || :prefijo || 'oro_contratados_trimestre_2021'
  || ' ORDER BY departamento COLLATE ES, cargo COLLATE ES';
