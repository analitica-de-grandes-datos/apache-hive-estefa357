/*

Pregunta
===========================================================================

Escriba una consulta que compute la cantidad de registros por letra de la 
columna 2 y clave de la columna 3; esto es, por ejemplo, la cantidad de 
registros en tienen la letra `a` en la columna 2 y la clave `aaa` en la 
columna 3 es:

    a    aaa    5

Apache Hive se ejecutará en modo local (sin HDFS).

Escriba el resultado a la carpeta `output` de directorio de trabajo.

*/
DROP TABLE IF EXISTS t0;
CREATE TABLE t0 (
    c1 STRING,
    c2 ARRAY<CHAR(1)>, 
    c3 MAP<STRING, INT>
    )
    ROW FORMAT DELIMITED 
        FIELDS TERMINATED BY '\t'
        COLLECTION ITEMS TERMINATED BY ','
        MAP KEYS TERMINATED BY '#'
        LINES TERMINATED BY '\n';
LOAD DATA LOCAL INPATH 'data.tsv' INTO TABLE t0;

/*
    >>> Escriba su respuesta a partir de este punto <<<
*/
DROP TABLE IF EXISTS t0;
CREATE TABLE t0 (
    c1 STRING,
    c2 ARRAY<CHAR(1)>, 
    c3 MAP<STRING, INT>
    )
    ROW FORMAT DELIMITED 
        FIELDS TERMINATED BY '\t'
        COLLECTION ITEMS TERMINATED BY ','
        MAP KEYS TERMINATED BY '#'
        LINES TERMINATED BY '\n';
LOAD DATA LOCAL INPATH 'pregunta_12/SOURCE/data.tsv' INTO TABLE t0;

SELECT c2, c3 from t0;
SELECT c2, map_keys(c3) from t0;

SELECT P.C2 ,letra FROM t0 P LATERAL VIEW EXPLODE(map_keys(P.c3)) fv as letra; 

SELECT CONCAT('"',b.letra1,',',b.letra,',',COUNT(b.letra),'",') from (
SELECT letra1, R.letra from (
SELECT P.c2 ,letra FROM t0 P LATERAL VIEW EXPLODE(map_keys(P.c3)) fv as letra) R
LATERAL VIEW EXPLODE(R.c2) fv as letra1) b GROUP BY b.letra1,b.letra;
