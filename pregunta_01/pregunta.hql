/* 
Pregunta
===========================================================================

Para resolver esta pregunta use el archivo `data.tsv`.

Compute la cantidad de registros por cada letra de la columna 1.
Escriba el resultado ordenado por letra. 

Apache Hive se ejecutará en modo local (sin HDFS).

Escriba el resultado a la carpeta `output` de directorio de trabajo.

        >>> Escriba su respuesta a partir de este punto <<<
DROP TABLE IF EXISTS data;
DROP TABLE IF EXISTS result;
CREATE TABLE data (line STRING);
CREATE TABLE result(letter STRING, value int);
LOAD DATA LOCAL INPATH "pregunta_01/Source/" OVERWRITE INTO TABLE data;
SELECT * FROM data LIMIT 5;
SELECT explode(split(line, '\\s')) AS word FROM data;

INSERT INTO result
SELECT letter, count(letter) AS value
    FROM
        (SELECT split(line, '\\s')[0] AS letter FROM data) C1
GROUP BY
    letter
ORDER BY
    letter;

Select * from result;

Select CONCAT('"',letter,',',value,'"') as Rtaprofe from result;

Select CONCAT('[', (Select CONCAT('"',letter,',',value,'"') as Rtaprofe from result),']') 
from result limit 1;

hive -S -e 'Select CONCAT(''"'',letter,'','',value,''"'') as Rtaprofe from result;' > output/result.csv
