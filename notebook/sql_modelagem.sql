-- Databricks notebook source
-- MAGIC %python
-- MAGIC dbutils.fs.rm("dbfs:/user/hive/warehouse/dim_tempo", recurse=True)
-- MAGIC
-- MAGIC

-- COMMAND ----------

CREATE TABLE IF NOT EXISTS dim_tempo (
  data_id STRING,
  data DATE,
  ano INT,
  mes INT
)
USING PARQUET;


-- COMMAND ----------

SELECT 
  f.data_id,
  t.ano,
  t.mes,
  i.ipca_valor,
  s.selic_valor,
  b.ibov_valor
FROM fato_economia f
JOIN dim_tempo t ON f.data_id = t.data_id
JOIN dim_ipca i ON f.ipca_id = i.ipca_id
JOIN dim_selic s ON f.selic_id = s.selic_id
JOIN dim_ibov b ON f.ibov_id = b.ibov_id
LIMIT 20;


-- COMMAND ----------

INSERT INTO fato_economia (data_id, ipca_id, selic_id, ibov_id)
VALUES 
  ('2009-01-01', '2009-01-01', '2009-01-01', '2009-01-01'),
  ('2009-02-01', '2009-02-01', '2009-02-01', '2009-02-01'),
  ('2009-03-01', '2009-03-01', '2009-03-01', '2009-03-01');


-- COMMAND ----------

SELECT 
  f.data_id,
  t.ano,
  t.mes,
  i.ipca_valor,
  s.selic_valor,
  b.ibov_valor
FROM fato_economia f
JOIN dim_tempo t ON f.data_id = t.data_id
JOIN dim_ipca i ON f.ipca_id = i.ipca_id
JOIN dim_selic s ON f.selic_id = s.selic_id
JOIN dim_ibov b ON f.ibov_id = b.ibov_id
LIMIT 20;


-- COMMAND ----------

INSERT INTO fato_economia (data_id, ipca_id, selic_id, ibov_id)
VALUES 
  ('2009-01-01', '2009-01-01', '2009-01-01', '2009-01-01'),
  ('2009-02-01', '2009-02-01', '2009-02-02', '2009-02-01'),
  ('2009-03-01', '2009-03-01', '2009-03-01', '2009-03-01');


-- COMMAND ----------

SELECT 
  f.data_id,
  t.ano,
  t.mes,
  i.ipca_valor,
  s.selic_valor,
  b.ibov_valor
FROM fato_economia f
JOIN dim_tempo t ON f.data_id = t.data_id
JOIN dim_ipca i ON f.ipca_id = i.ipca_id
JOIN dim_selic s ON f.selic_id = s.selic_id
JOIN dim_ibov b ON f.ibov_id = b.ibov_id
LIMIT 20;


-- COMMAND ----------

INSERT INTO fato_economia (data_id, ipca_id, selic_id, ibov_id)
VALUES 
  ('2009-01-01', '2009-01-01', '2009-01-01', '2009-01-01'),
  ('2009-02-01', '2009-02-01', '2009-02-01', '2009-02-01'),
  ('2009-03-01', '2009-03-01', '2009-03-01', '2009-03-01');


-- COMMAND ----------

SELECT * 
--  f.data_id,
--  t.ano,
 -- t.mes,
 -- i.ipca_valor,
 -- s.selic_valor,
 -- b.ibov_valor

FROM fato_economia f
-- JOIN dim_tempo t ON f.data_id = t.data_id
-- JOIN dim_ipca i ON f.ipca_id = i.ipca_id
-- JOIN dim_selic s ON f.selic_id = s.selic_id
-- JOIN dim_ibov b ON f.ibov_id = b.ibov_id
LIMIT 20;


-- COMMAND ----------

-- MAGIC %python
-- MAGIC # Leitura da camada GOLD
-- MAGIC df_gold = spark.read.parquet("/mnt/gold/economia_correlacao")
-- MAGIC
-- MAGIC # Criando colunas ID para cada dimensão
-- MAGIC from pyspark.sql.functions import date_format
-- MAGIC
-- MAGIC df_dim = df_gold.withColumn("data_id", date_format("data", "yyyy-MM-dd")) \
-- MAGIC     .withColumn("ipca_id", date_format("data", "yyyy-MM-dd")) \
-- MAGIC     .withColumn("selic_id", date_format("data", "yyyy-MM-dd")) \
-- MAGIC     .withColumn("ibov_id", date_format("data", "yyyy-MM-dd"))
-- MAGIC
-- MAGIC # Selecionar apenas as colunas da fato
-- MAGIC df_fato = df_dim.select("data_id", "ipca_id", "selic_id", "ibov_id")
-- MAGIC
-- MAGIC # Salvar como tabela SQL
-- MAGIC df_fato.write.mode("overwrite").saveAsTable("fato_economia")
-- MAGIC

-- COMMAND ----------

SHOW TABLES LIKE 'fato_economia';


-- COMMAND ----------

SELECT * FROM fato_economia LIMIT 10;


-- COMMAND ----------

SELECT
  f.data_id,
  t.ano,
  t.mes,
  i.ipca_valor,
  s.selic_valor,
  b.ibov_valor
FROM fato_economia f
JOIN dim_tempo t ON f.data_id = t.data_id
JOIN dim_ipca i ON f.ipca_id = i.ipca_id
JOIN dim_selic s ON f.selic_id = s.selic_id
JOIN dim_ibov b ON f.ibov_id = b.ibov_id
LIMIT 20;


-- COMMAND ----------

SELECT 
    f.data_id,
    t.ano,
    t.mes,
    i.ipca_valor,
    s.selic_valor,
    b.ibov_valor
FROM fato_economia f
LEFT JOIN dim_tempo t ON f.data_id = t.data_id
LEFT JOIN dim_ipca i ON f.ipca_id = i.ipca_id
LEFT JOIN dim_selic s ON f.selic_id = s.selic_id
LEFT JOIN dim_ibov b ON f.ibov_id = b.ibov_id
LIMIT 20;


-- COMMAND ----------

-- Verifique se os IDs da tabela de tempo batem
SELECT * FROM dim_tempo LIMIT 10;

-- Veja o formato do ipca_id
SELECT * FROM dim_ipca LIMIT 10;

-- Veja o formato do selic_id
SELECT * FROM dim_selic LIMIT 10;

-- Veja o formato do ibov_id
SELECT * FROM dim_ibov LIMIT 10;


-- COMMAND ----------

SELECT DISTINCT ibov_id FROM fato_economia LIMIT 10;
SELECT DISTINCT selic_id FROM fato_economia LIMIT 10;
SELECT DISTINCT ipca_id FROM fato_economia LIMIT 10;
SELECT DISTINCT data_id FROM fato_economia LIMIT 10;


-- COMMAND ----------

-- 1. Tabela de Tempo
CREATE OR REPLACE TABLE dim_tempo AS
SELECT DISTINCT 
    data_id,
    SUBSTRING(CAST(data_id AS STRING), 1, 4) AS ano,
    SUBSTRING(CAST(data_id AS STRING), 5, 2) AS mes
FROM fato_economia;

-- 2. IPCA
CREATE OR REPLACE TABLE dim_ipca AS
SELECT DISTINCT 
    ipca_id,
    ipca_valor
FROM fato_economia
WHERE ipca_valor IS NOT NULL;

-- 3. SELIC
CREATE OR REPLACE TABLE dim_selic AS
SELECT DISTINCT 
    selic_id,
    selic_valor
FROM fato_economia
WHERE selic_valor IS NOT NULL;

-- 4. IBOVESPA
CREATE OR REPLACE TABLE dim_ibov AS
SELECT DISTINCT 
    ibov_id,
    ibov_valor
FROM fato_economia
WHERE ibov_valor IS NOT NULL;


-- COMMAND ----------

DROP TABLE IF EXISTS dim_tempo;
DROP TABLE IF EXISTS dim_ipca;
DROP TABLE IF EXISTS dim_selic;
DROP TABLE IF EXISTS dim_ibov;


-- COMMAND ----------

-- 🧹 Limpar tabelas antigas
DROP TABLE IF EXISTS dim_tempo;
DROP TABLE IF EXISTS dim_ipca;
DROP TABLE IF EXISTS dim_selic;
DROP TABLE IF EXISTS dim_ibov;

-- 1. Tabela de Tempo
CREATE TABLE dim_tempo AS
SELECT DISTINCT 
    data_id,
    SUBSTRING(CAST(data_id AS STRING), 1, 4) AS ano,
    SUBSTRING(CAST(data_id AS STRING), 5, 2) AS mes
FROM fato_economia;

-- 2. IPCA
CREATE TABLE dim_ipca AS
SELECT DISTINCT 
    ipca_id,
    ipca_valor
FROM fato_economia
WHERE ipca_valor IS NOT NULL;

-- 3. SELIC
CREATE TABLE dim_selic AS
SELECT DISTINCT 
    selic_id,
    selic_valor
FROM fato_economia
WHERE selic_valor IS NOT NULL;

-- 4. IBOVESPA
CREATE TABLE dim_ibov AS
SELECT DISTINCT 
    ibov_id,
    ibov_valor
FROM fato_economia
WHERE ibov_valor IS NOT NULL;


-- COMMAND ----------

CREATE TABLE dim_ipca AS
SELECT DISTINCT ipca_id
FROM fato_economia
WHERE ipca_id IS NOT NULL;

CREATE TABLE dim_selic AS
SELECT DISTINCT selic_id
FROM fato_economia
WHERE selic_id IS NOT NULL;

CREATE TABLE dim_ibov AS
SELECT DISTINCT ibov_id
FROM fato_economia
WHERE ibov_id IS NOT NULL;


-- COMMAND ----------

DROP TABLE IF EXISTS dim_tempo;
DROP TABLE IF EXISTS dim_ipca;
DROP TABLE IF EXISTS dim_selic;
DROP TABLE IF EXISTS dim_ibov;

-- Tempo
CREATE TABLE dim_tempo AS
SELECT DISTINCT 
    data_id,
    SUBSTRING(CAST(data_id AS STRING), 1, 4) AS ano,
    SUBSTRING(CAST(data_id AS STRING), 5, 2) AS mes
FROM fato_economia;

-- IPCA
CREATE TABLE dim_ipca AS
SELECT DISTINCT ipca_id FROM fato_economia WHERE ipca_id IS NOT NULL;

-- SELIC
CREATE TABLE dim_selic AS
SELECT DISTINCT selic_id FROM fato_economia WHERE selic_id IS NOT NULL;

-- IBOV
CREATE TABLE dim_ibov AS
SELECT DISTINCT ibov_id FROM fato_economia WHERE ibov_id IS NOT NULL;


-- COMMAND ----------

SELECT COUNT(*) FROM fato_economia WHERE ipca_id IS NOT NULL;
SELECT COUNT(*) FROM fato_economia WHERE selic_id IS NOT NULL;
SELECT COUNT(*) FROM fato_economia WHERE ibov_id IS NOT NULL;


-- COMMAND ----------

INSERT INTO dim_ipca VALUES (1), (2), (3);
INSERT INTO dim_selic VALUES (1), (2), (3);
INSERT INTO dim_ibov VALUES (1), (2), (3);


-- COMMAND ----------

SELECT 
    f.data_id,
    t.ano,
    t.mes,
    i.ipca_id,
    s.selic_id,
    b.ibov_id
FROM fato_economia f
JOIN dim_tempo t ON f.data_id = t.data_id
JOIN dim_ipca i ON f.ipca_id = i.ipca_id
JOIN dim_selic s ON f.selic_id = s.selic_id
JOIN dim_ibov b ON f.ibov_id = b.ibov_id
LIMIT 20;


-- COMMAND ----------

SELECT * FROM fato_economia LIMIT 10;


-- COMMAND ----------

SELECT
  f.data_id,
  t.ano,
  t.mes,
  i.ipca_valor,
  s.selic_valor,
  b.ibov_valor
FROM fato_economia f
JOIN dim_tempo t ON f.data_id = t.data_id
JOIN dim_ipca i ON f.ipca_id = i.ipca_id
JOIN dim_selic s ON f.selic_id = s.selic_id
JOIN dim_ibov b ON f.ibov_id = b.ibov_id
LIMIT 20;


-- COMMAND ----------

SELECT * FROM fato_economia LIMIT 10;


-- COMMAND ----------

SELECT
  t.ano,
  ROUND(AVG(i.ipca_valor), 2) AS media_ipca,
  ROUND(AVG(s.selic_valor), 2) AS media_selic,
  ROUND(AVG(b.ibov_valor), 2) AS media_ibov
FROM fato_economia f
JOIN dim_tempo t ON f.data_id = t.data_id
JOIN dim_ipca i ON f.ipca_id = i.ipca_id
JOIN dim_selic s ON f.selic_id = s.selic_id
JOIN dim_ibov b ON f.ibov_id = b.ibov_id
GROUP BY t.ano
ORDER BY t.ano;


-- COMMAND ----------

SELECT t.ano, ROUND(AVG(i.ipca_valor), 2) AS media_ipca
FROM fato_economia f
JOIN dim_tempo t ON f.data_id = t.data_id
JOIN dim_ipca i ON f.ipca_id = i.ipca_id
GROUP BY t.ano
ORDER BY media_ipca DESC
LIMIT 1;


-- COMMAND ----------

SELECT t.ano, ROUND(AVG(i.ipca_valor), 2) AS media_ipca
FROM fato_economia f
JOIN dim_tempo t ON f.data_id = t.data_id
JOIN dim_ipca i ON f.ipca_id = i.ipca_id
GROUP BY t.ano
ORDER BY media_ipca DESC
LIMIT 1;


-- COMMAND ----------

SELECT
  t.ano,
  ROUND(AVG(i.ipca_valor), 2) AS media_ipca
FROM fato_economia f
JOIN dim_tempo t ON f.data_id = t.data_id
JOIN dim_ipca i ON f.ipca_id = i.ipca_id
GROUP BY t.ano
ORDER BY media_ipca DESC
LIMIT 1;


-- COMMAND ----------

SELECT
  t.ano,
  ROUND(AVG(s.selic_valor), 2) AS media_selic
FROM fato_economia f
JOIN dim_tempo t ON f.data_id = t.data_id
JOIN dim_selic s ON f.selic_id = s.selic_id
GROUP BY t.ano
ORDER BY media_selic DESC
LIMIT 1;


-- COMMAND ----------

SELECT
  t.ano,
  ROUND(AVG(b.ibov_valor), 2) AS media_ibov
FROM fato_economia f
JOIN dim_tempo t ON f.data_id = t.data_id
JOIN dim_ibov b ON f.ibov_id = b.ibov_id
GROUP BY t.ano
ORDER BY media_ibov DESC
LIMIT 1;


-- COMMAND ----------

SELECT 
  t.ano,
  ROUND(AVG(i.ipca_valor), 2) AS media_ipca,
  ROUND(AVG(s.selic_valor), 2) AS media_selic,
  ROUND(AVG(b.ibov_valor), 2) AS media_ibov
FROM fato_economia f
JOIN dim_tempo t ON f.data_id = t.data_id
JOIN dim_ipca i ON f.ipca_id = i.ipca_id
JOIN dim_selic s ON f.selic_id = s.selic_id
JOIN dim_ibov b ON f.ibov_id = b.ibov_id
GROUP BY t.ano
ORDER BY t.ano;


-- COMMAND ----------

SELECT t.ano,
  ROUND(AVG(i.ipca_valor), 2) AS media_ipca,
  ROUND(AVG(s.selic_valor), 2) AS media_selic,
  ROUND(AVG(b.ibov_valor), 2) AS media_ibov
...
