-- Verificación de las métricas en cada corrida del job. Corre después de metrica_1.sql y
-- metrica_2.sql y falla (raise_error) si algún chequeo encuentra diferencias. No reemplaza
-- las pruebas automáticas: es una verificación de los datos reales, como la conciliación
-- de calidad. Si falla, las tablas oro ya se reemplazaron, pero el job queda en error.
--
-- Parámetros: los mismos de metrica_1.sql y metrica_2.sql (catalogo, esquema, prefijo).
--
-- Los cálculos alternos están escritos a propósito de otra forma que las métricas:
--   * El año y el trimestre salen del día UTC calculado con aritmética (segundos desde 1970
--     divididos por 86400), no de comparar contra fechas fijas.
--   * Métrica 1: los nombres se agregan antes de contar, se agrupa por nombre y los
--     trimestres salen de un PIVOT.
--   * Métrica 2: el umbral es avg() OVER () en DECIMAL, no la comparación en enteros.
--
-- Formato del archivo: cada sentencia termina con ';' al final de una línea. No poner ';'
-- al final de una línea dentro de comentarios o textos (ejecutar.py separa por ahí).

-- 1. Chequeos: cada fila cuenta diferencias. Si alguna no es 0, raise_error hace fallar
--    la sentencia (y la tarea del job).
WITH empleados AS (
  SELECT
    id_departamento,
    id_cargo,
    date_from_unix_date(CAST(floor(unix_seconds(fecha_contratacion) / 86400) AS INT)) AS dia_utc
  FROM IDENTIFIER(:catalogo || '.' || :esquema || '.' || :prefijo || 'plata_empleados')
),
departamentos AS (
  SELECT id, departamento
  FROM IDENTIFIER(:catalogo || '.' || :esquema || '.' || :prefijo || 'plata_departamentos')
),
cargos AS (
  SELECT id, cargo
  FROM IDENTIFIER(:catalogo || '.' || :esquema || '.' || :prefijo || 'plata_cargos')
),
metrica_1 AS (
  SELECT departamento, cargo, Q1, Q2, Q3, Q4
  FROM IDENTIFIER(:catalogo || '.' || :esquema || '.' || :prefijo || 'oro_contratados_trimestre_2021')
),
metrica_2 AS (
  SELECT id, departamento, contratados
  FROM IDENTIFIER(:catalogo || '.' || :esquema || '.' || :prefijo || 'oro_departamentos_sobre_promedio_2021')
),
vista_1 AS (
  SELECT departamento, cargo, Q1, Q2, Q3, Q4
  FROM IDENTIFIER(:catalogo || '.' || :esquema || '.' || :prefijo || 'oro_contratados_trimestre_2021_ordenada')
),
vista_2 AS (
  SELECT id, departamento, contratados
  FROM IDENTIFIER(:catalogo || '.' || :esquema || '.' || :prefijo || 'oro_departamentos_sobre_promedio_2021_ordenada')
),
empleados_2021 AS (
  SELECT * FROM empleados WHERE year(dia_utc) = 2021
),
alterna_1 AS (
  SELECT
    departamento,
    cargo,
    coalesce(Q1, 0) AS Q1,
    coalesce(Q2, 0) AS Q2,
    coalesce(Q3, 0) AS Q3,
    coalesce(Q4, 0) AS Q4
  FROM (
    SELECT d.departamento, c.cargo, quarter(e.dia_utc) AS trimestre
    FROM empleados_2021 AS e
    JOIN departamentos AS d ON d.id = e.id_departamento
    JOIN cargos AS c ON c.id = e.id_cargo
  )
  PIVOT (count(*) FOR trimestre IN (1 AS Q1, 2 AS Q2, 3 AS Q3, 4 AS Q4))
),
conteo_departamentos AS (
  SELECT d.id, d.departamento, count_if(year(e.dia_utc) = 2021) AS contratados
  FROM departamentos AS d
  LEFT JOIN empleados AS e ON e.id_departamento = d.id
  GROUP BY d.id, d.departamento
),
alterna_2 AS (
  SELECT id, departamento, contratados
  FROM (
    SELECT *, avg(CAST(contratados AS DECIMAL(20, 0))) OVER () AS promedio
    FROM conteo_departamentos
  )
  WHERE contratados > promedio
),
chequeos AS (
  SELECT
    'empleados de 2021 sin departamento o cargo en plata' AS chequeo,
    (SELECT count(*) FROM empleados_2021 AS e
     WHERE NOT EXISTS (SELECT 1 FROM departamentos AS d WHERE d.id = e.id_departamento)
        OR NOT EXISTS (SELECT 1 FROM cargos AS c WHERE c.id = e.id_cargo)) AS diferencias
  UNION ALL
  SELECT
    'suma de Q1 a Q4 frente al total de contratados en 2021',
    abs((SELECT coalesce(sum(Q1 + Q2 + Q3 + Q4), 0) FROM metrica_1) - (SELECT count(*) FROM empleados_2021))
  UNION ALL
  SELECT
    'Métrica 1 frente al cálculo alterno (filas distintas)',
    (SELECT count(*) FROM (
       (SELECT * FROM metrica_1 EXCEPT ALL SELECT * FROM alterna_1)
       UNION ALL
       (SELECT * FROM alterna_1 EXCEPT ALL SELECT * FROM metrica_1)))
  UNION ALL
  SELECT
    'Métrica 2 frente al cálculo alterno (filas distintas)',
    (SELECT count(*) FROM (
       (SELECT * FROM metrica_2 EXCEPT ALL SELECT * FROM alterna_2)
       UNION ALL
       (SELECT * FROM alterna_2 EXCEPT ALL SELECT * FROM metrica_2)))
  UNION ALL
  SELECT
    'vistas _ordenada frente a sus tablas (filas distintas)',
    (SELECT count(*) FROM (
       (SELECT * FROM metrica_1 EXCEPT ALL SELECT * FROM vista_1)
       UNION ALL
       (SELECT * FROM vista_1 EXCEPT ALL SELECT * FROM metrica_1)))
    + (SELECT count(*) FROM (
       (SELECT * FROM metrica_2 EXCEPT ALL SELECT * FROM vista_2)
       UNION ALL
       (SELECT * FROM vista_2 EXCEPT ALL SELECT * FROM metrica_2)))
)
SELECT
  chequeo,
  diferencias,
  CASE
    WHEN diferencias = 0 THEN 'ok'
    ELSE raise_error(concat('Verificación de métricas falló: ', chequeo, ' = ', diferencias))
  END AS resultado
FROM chequeos;

-- 2. Resumen para la salida de la tarea: totales, los dos promedios (con y sin los
--    departamentos que no contrataron) y el tamaño de cada métrica.
WITH conteo_departamentos AS (
  SELECT
    d.id,
    count_if(
      year(date_from_unix_date(CAST(floor(unix_seconds(e.fecha_contratacion) / 86400) AS INT))) = 2021
    ) AS contratados
  FROM IDENTIFIER(:catalogo || '.' || :esquema || '.' || :prefijo || 'plata_departamentos') AS d
  LEFT JOIN IDENTIFIER(:catalogo || '.' || :esquema || '.' || :prefijo || 'plata_empleados') AS e
    ON e.id_departamento = d.id
  GROUP BY d.id
)
SELECT
  sum(contratados)                                                         AS contratados_2021,
  count(*)                                                                 AS departamentos,
  count_if(contratados > 0)                                                AS departamentos_con_contrataciones,
  CAST(sum(contratados) / count(*) AS DECIMAL(10, 4))                      AS promedio_todos,
  CAST(sum(contratados) / count_if(contratados > 0) AS DECIMAL(10, 4))     AS promedio_con_contrataciones,
  (SELECT count(*)
   FROM IDENTIFIER(:catalogo || '.' || :esquema || '.' || :prefijo || 'oro_contratados_trimestre_2021'))
                                                                           AS filas_metrica_1,
  (SELECT count(*)
   FROM IDENTIFIER(:catalogo || '.' || :esquema || '.' || :prefijo || 'oro_departamentos_sobre_promedio_2021'))
                                                                           AS filas_metrica_2
FROM conteo_departamentos;
