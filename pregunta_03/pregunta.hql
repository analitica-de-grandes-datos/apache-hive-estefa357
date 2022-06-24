/* 
Pregunta
===========================================================================

Para resolver esta pregunta use el archivo `data.tsv`.

Escriba una consulta que devuelva los cinco valores diferentes más pequeños 
de la tercera columna.

Apache Hive se ejecutará en modo local (sin HDFS).

Escriba el resultado a la carpeta `output` de directorio de trabajo.

        >>> Escriba su respuesta a partir de este punto <<<
DROP TABLE IF EXISTS data;
DROP TABLE IF EXISTS result3;
CREATE TABLE data (line STRING);
CREATE TABLE result3(letter STRING, fecha date, value int);
LOAD DATA LOCAL INPATH "pregunta_03/SOURCE/" OVERWRITE INTO TABLE data;

INSERT INTO result3
SELECT split(line, '\\s')[0] AS letter, split(line, '\\s')[1] AS fecha, split(line, '\\s')[2] AS value FROM data;

SELECT DISTINCT(value) AS value From result3 ORDER BY value ASC LIMIT 5;


