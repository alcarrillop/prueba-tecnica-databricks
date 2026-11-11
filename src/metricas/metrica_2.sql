-- Métrica 2 (Sección 3 de enunciado.md): id, nombre y número de contratados de cada
-- departamento que contrató más empleados que el promedio de todos los departamentos en
-- 2021, ordenado por número de contratados de mayor a menor.
--
-- Parámetros (marcadores con nombre; el job los pasa a la tarea SQL):
--   catalogo, esquema   dónde están las tablas de origen y dónde quedan las de destino.
--   prefijo             prefijo de todos los nombres de tabla: vacío en el job,
--                       test_<sufijo>_ en las pruebas.
-- Lee:   <prefijo>plata_empleados, <prefijo>plata_departamentos
-- Crea:  <prefijo>oro_departamentos_sobre_promedio_2021            tabla Delta (columnas exactas)
--        <prefijo>oro_departamentos_sobre_promedio_2021_ordenada   vista con el orden oficial
--
-- Decisiones (detalle en el README):
--   * Año en UTC: la fecha se compara contra instantes fijos escritos en UTC (TIMESTAMP
--     con Z), sin depender de la zona horaria de la sesión.
--   * Promedio = contrataciones de 2021 / número de departamentos en plata_departamentos.
--     Los departamentos que no contrataron a nadie cuentan con 0 ("todos los departamentos").
--   * "Más que el promedio" es estricto. Se compara en enteros,
--     contratados * departamentos > total, que equivale a contratados > total / departamentos
--     sin redondeos.
--   * id = plata_departamentos.id (el id del departamento, no de un empleado).
--   * Orden: contratados de mayor a menor; desempate por departamento (collation ES) y
--     luego por id, así el orden es total. Una tabla Delta no garantiza el orden al leerla:
--     el orden oficial es el de la vista _ordenada.
--   * Se reconstruye completa en cada corrida (CREATE OR REPLACE): mismo plata, mismo
--     resultado.
--
-- Formato del archivo: cada sentencia termina con ';' al final de una línea. No poner ';'
-- al final de una línea dentro de comentarios o textos (ejecutar.py separa por ahí).

-- CREATE OR REPLACE ... AS SELECT no admite lista de columnas: los tipos salen de la
-- consulta (id INT, departamento STRING, contratados BIGINT) y la descripción va en el COMMENT.
CREATE OR REPLACE TABLE IDENTIFIER(:catalogo || '.' || :esquema || '.' || :prefijo || 'oro_departamentos_sobre_promedio_2021')
COMMENT 'Métrica 2: id (plata_departamentos.id), nombre y contratados en 2021 (UTC) de los departamentos que contrataron estrictamente más que el promedio de todos los departamentos, incluidos los que no contrataron. Para leerla en orden usar la vista oro_departamentos_sobre_promedio_2021_ordenada (ORDER BY contratados DESC, departamento COLLATE ES, id).'
AS
WITH contrataciones_2021 AS (
  SELECT id_departamento
  FROM IDENTIFIER(:catalogo || '.' || :esquema || '.' || :prefijo || 'plata_empleados')
  WHERE fecha_contratacion >= TIMESTAMP'2021-01-01T00:00:00Z'
    AND fecha_contratacion <  TIMESTAMP'2022-01-01T00:00:00Z'
),
por_departamento AS (
  -- Todos los departamentos, también los que no contrataron en 2021 (quedan con 0).
  SELECT
    d.id,
    d.departamento,
    count(c.id_departamento) AS contratados
  FROM IDENTIFIER(:catalogo || '.' || :esquema || '.' || :prefijo || 'plata_departamentos') AS d
  LEFT JOIN contrataciones_2021 AS c
    ON c.id_departamento = d.id
  GROUP BY d.id, d.departamento
),
totales AS (
  SELECT
    sum(contratados) AS contratados_total,
    count(*)         AS departamentos
  FROM por_departamento
)
SELECT
  p.id,
  p.departamento,
  p.contratados
FROM por_departamento AS p
CROSS JOIN totales AS t
WHERE p.contratados * t.departamentos > t.contratados_total
ORDER BY p.contratados DESC, p.departamento COLLATE ES, p.id;

-- La vista guarda el texto de su consulta, que no admite marcadores de parámetro: se arma
-- con el nombre ya resuelto y se crea con EXECUTE IMMEDIATE.
EXECUTE IMMEDIATE
  'CREATE OR REPLACE VIEW ' || :catalogo || '.' || :esquema || '.' || :prefijo || 'oro_departamentos_sobre_promedio_2021_ordenada'
  || ' COMMENT "Métrica 2 en el orden oficial: contratados de mayor a menor, desempate por departamento (collation ES) y por id."'
  || ' AS SELECT id, departamento, contratados FROM '
  || :catalogo || '.' || :esquema || '.' || :prefijo || 'oro_departamentos_sobre_promedio_2021'
  || ' ORDER BY contratados DESC, departamento COLLATE ES, id';
