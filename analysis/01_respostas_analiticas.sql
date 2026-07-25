-- Databricks notebook source
-- MAGIC %md
-- MAGIC # Respostas analíticas — NYC TLC Yellow Taxi
-- MAGIC
-- MAGIC Fonte de consumo:
-- MAGIC `case_ifood.tlc_data_gold.gold_yellow_trips_consumption`
-- MAGIC
-- MAGIC Premissas:
-- MAGIC
-- MAGIC - cada registro representa uma viagem;
-- MAGIC - a média mensal é a média de `total_amount` por viagem;
-- MAGIC - valores nulos são naturalmente desconsiderados por `AVG`;
-- MAGIC - os intervalos temporais são fechados no início e abertos no fim;
-- MAGIC - valores negativos não são removidos, pois podem representar ajustes
-- MAGIC   presentes nos dados oficiais.

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Cobertura dos dados usados
-- MAGIC
-- MAGIC Esta verificação evidencia o período, a quantidade de viagens e a
-- MAGIC quantidade de valores disponíveis para cada métrica.

-- COMMAND ----------

SELECT
  MIN(tpep_pickup_datetime) AS primeira_coleta,
  MAX(tpep_pickup_datetime) AS ultima_coleta,
  COUNT(*) AS quantidade_viagens,
  COUNT(total_amount) AS viagens_com_total_amount,
  COUNT(passenger_count) AS viagens_com_passenger_count
FROM case_ifood.tlc_data_gold.gold_yellow_trips_consumption
WHERE tpep_pickup_datetime >= TIMESTAMP '2023-01-01 00:00:00'
  AND tpep_pickup_datetime <  TIMESTAMP '2023-06-01 00:00:00';

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## 1. Média de valor total recebido por mês
-- MAGIC
-- MAGIC Como a pergunta não determina um único mês, o resultado apresenta a
-- MAGIC média por viagem para cada mês disponível entre janeiro e maio de 2023.

-- COMMAND ----------

SELECT
  DATE_FORMAT(tpep_pickup_datetime, 'yyyy-MM') AS mes_referencia,
  ROUND(AVG(total_amount), 2) AS media_total_amount,
  COUNT(total_amount) AS viagens_com_total_amount
FROM case_ifood.tlc_data_gold.gold_yellow_trips_consumption
WHERE tpep_pickup_datetime >= TIMESTAMP '2023-01-01 00:00:00'
  AND tpep_pickup_datetime <  TIMESTAMP '2023-06-01 00:00:00'
GROUP BY DATE_FORMAT(tpep_pickup_datetime, 'yyyy-MM')
ORDER BY mes_referencia;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## 2. Média de passageiros por hora do dia em maio de 2023
-- MAGIC
-- MAGIC A hora representa a hora de início da viagem, extraída de
-- MAGIC `tpep_pickup_datetime`.

-- COMMAND ----------

SELECT
  HOUR(tpep_pickup_datetime) AS hora_do_dia,
  ROUND(AVG(passenger_count), 2) AS media_passageiros,
  COUNT(passenger_count) AS viagens_com_contagem
FROM case_ifood.tlc_data_gold.gold_yellow_trips_consumption
WHERE tpep_pickup_datetime >= TIMESTAMP '2023-05-01 00:00:00'
  AND tpep_pickup_datetime <  TIMESTAMP '2023-06-01 00:00:00'
GROUP BY HOUR(tpep_pickup_datetime)
ORDER BY hora_do_dia;
