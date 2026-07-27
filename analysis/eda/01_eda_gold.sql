-- Databricks notebook source
-- MAGIC %md
-- MAGIC # EDA — NYC TLC Yellow Taxi
-- MAGIC
-- MAGIC Fonte detalhada:
-- MAGIC `case_ifood.tlc_data_gold.gold_yellow_trips_consumption`
-- MAGIC
-- MAGIC Este notebook explora as viagens Yellow entre janeiro e maio de 2023.
-- MAGIC O mês é determinado por `_reference_month`, preservando o período do
-- MAGIC arquivo de origem mesmo quando um registro possui timestamp anômalo.
-- MAGIC
-- MAGIC Premissas:
-- MAGIC
-- MAGIC - cada registro representa uma corrida;
-- MAGIC - `total_amount` é o valor total registrado, não necessariamente
-- MAGIC   receita financeira liquidada;
-- MAGIC - valores nulos são apresentados como problema de completude e não
-- MAGIC   são imputados;
-- MAGIC - valores não positivos e datas anômalas são mantidos para exploração;
-- MAGIC - duração é a diferença entre desembarque e embarque em minutos.

-- COMMAND ----------

CREATE OR REPLACE TEMP VIEW eda_yellow_trips AS
SELECT
  VendorID,
  passenger_count,
  total_amount,
  tpep_pickup_datetime,
  tpep_dropoff_datetime,
  _reference_month,
  TIMESTAMPDIFF(
    SECOND,
    tpep_pickup_datetime,
    tpep_dropoff_datetime
  ) / 60.0 AS trip_duration_minutes
FROM case_ifood.tlc_data_gold.gold_yellow_trips_consumption
WHERE _reference_month BETWEEN '2023-01' AND '2023-05';

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## 1. Evolução mensal da quantidade de corridas

-- COMMAND ----------

SELECT
  _reference_month AS mes_referencia,
  COUNT(*) AS quantidade_corridas,
  ROUND(
    100.0 * (
      COUNT(*) - LAG(COUNT(*)) OVER (ORDER BY _reference_month)
    ) / NULLIF(LAG(COUNT(*)) OVER (ORDER BY _reference_month), 0),
    2
  ) AS variacao_percentual_mes_anterior
FROM eda_yellow_trips
GROUP BY _reference_month
ORDER BY mes_referencia;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### 1.1 Evolução mensal pela data de pickup
-- MAGIC
-- MAGIC Esta visão usa o mês em que a corrida ocorreu. Ela complementa
-- MAGIC `_reference_month`, que representa o mês declarado pelo arquivo de
-- MAGIC origem. A comparação torna visíveis registros carregados em um arquivo
-- MAGIC diferente do mês indicado por `tpep_pickup_datetime`.

-- COMMAND ----------

WITH reference_counts AS (
  SELECT
    _reference_month AS mes,
    COUNT(*) AS corridas_mes_arquivo
  FROM eda_yellow_trips
  GROUP BY _reference_month
),
pickup_counts AS (
  SELECT
    DATE_FORMAT(tpep_pickup_datetime, 'yyyy-MM') AS mes,
    COUNT(*) AS corridas_mes_pickup
  FROM eda_yellow_trips
  WHERE tpep_pickup_datetime >= TIMESTAMP '2023-01-01 00:00:00'
    AND tpep_pickup_datetime <  TIMESTAMP '2023-06-01 00:00:00'
  GROUP BY DATE_FORMAT(tpep_pickup_datetime, 'yyyy-MM')
)
SELECT
  reference_counts.mes AS mes_pickup,
  reference_counts.corridas_mes_arquivo,
  pickup_counts.corridas_mes_pickup,
  pickup_counts.corridas_mes_pickup
    - reference_counts.corridas_mes_arquivo AS diferenca,
  ROUND(
    100.0 * (
      pickup_counts.corridas_mes_pickup
      - LAG(pickup_counts.corridas_mes_pickup)
        OVER (ORDER BY reference_counts.mes)
    ) / NULLIF(
      LAG(pickup_counts.corridas_mes_pickup)
        OVER (ORDER BY reference_counts.mes),
      0
    ),
    2
  ) AS variacao_percentual_mes_anterior
FROM reference_counts
LEFT JOIN pickup_counts
  ON reference_counts.mes = pickup_counts.mes
ORDER BY mes_pickup;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## 2. Receita total e ticket médio por mês
-- MAGIC
-- MAGIC O total inclui ajustes negativos existentes na fonte. A quantidade de
-- MAGIC valores disponíveis evidencia o denominador efetivamente usado.

-- COMMAND ----------

SELECT
  _reference_month AS mes_referencia,
  ROUND(SUM(total_amount), 2) AS receita_total_registrada,
  ROUND(AVG(total_amount), 2) AS ticket_medio,
  COUNT(total_amount) AS corridas_com_valor,
  COUNT(*) - COUNT(total_amount) AS corridas_sem_valor
FROM eda_yellow_trips
GROUP BY _reference_month
ORDER BY mes_referencia;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## 3. Corridas e receita por VendorID

-- COMMAND ----------

SELECT
  COALESCE(CAST(VendorID AS STRING), 'NÃO INFORMADO') AS vendor_id,
  COUNT(*) AS quantidade_corridas,
  ROUND(SUM(total_amount), 2) AS receita_total_registrada,
  ROUND(AVG(total_amount), 2) AS ticket_medio,
  ROUND(100.0 * COUNT(*) / SUM(COUNT(*)) OVER (), 2) AS percentual_corridas
FROM eda_yellow_trips
GROUP BY VendorID
ORDER BY quantidade_corridas DESC;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## 4. Média de passageiros por corrida
-- MAGIC
-- MAGIC São exibidas a média dos valores informados e a média restrita a
-- MAGIC quantidades positivas. Assim, a EDA não confunde ausência com zero e
-- MAGIC mantém visível o impacto da regra de validade.

-- COMMAND ----------

SELECT
  _reference_month AS mes_referencia,
  ROUND(AVG(passenger_count), 2) AS media_passageiros_informada,
  ROUND(
    AVG(CASE WHEN passenger_count > 0 THEN passenger_count END),
    2
  ) AS media_passageiros_validos,
  COUNT(*) AS quantidade_corridas,
  COUNT(passenger_count) AS corridas_com_passageiros_informados,
  COUNT_IF(passenger_count IS NULL) AS corridas_sem_passageiros_informados,
  COUNT_IF(passenger_count <= 0) AS corridas_com_passageiros_nao_positivos
FROM eda_yellow_trips
GROUP BY _reference_month
ORDER BY mes_referencia;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## 5. Distribuição da duração das corridas
-- MAGIC
-- MAGIC Faixas inválidas e superiores a 24 horas permanecem explícitas porque
-- MAGIC alimentam regras de Data Quality.

-- COMMAND ----------

WITH duration_buckets AS (
  SELECT
    CASE
      WHEN trip_duration_minutes IS NULL THEN '00. Não informada'
      WHEN trip_duration_minutes < 0 THEN '01. Negativa'
      WHEN trip_duration_minutes <= 5 THEN '02. Até 5 min'
      WHEN trip_duration_minutes <= 15 THEN '03. 5 a 15 min'
      WHEN trip_duration_minutes <= 30 THEN '04. 15 a 30 min'
      WHEN trip_duration_minutes <= 60 THEN '05. 30 a 60 min'
      WHEN trip_duration_minutes <= 120 THEN '06. 1 a 2 h'
      WHEN trip_duration_minutes <= 1440 THEN '07. 2 a 24 h'
      ELSE '08. Acima de 24 h'
    END AS faixa_duracao
  FROM eda_yellow_trips
)
SELECT
  faixa_duracao,
  COUNT(*) AS quantidade_corridas,
  ROUND(100.0 * COUNT(*) / SUM(COUNT(*)) OVER (), 4) AS percentual_corridas
FROM duration_buckets
GROUP BY faixa_duracao
ORDER BY faixa_duracao;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## 6. Percentual sinalizado pelas regras de qualidade
-- MAGIC
-- MAGIC O pipeline atual monitora violações e não rejeita fisicamente
-- MAGIC registros. Como os resultados são agregados por regra, uma mesma
-- MAGIC corrida pode violar mais de uma regra. Portanto, o percentual abaixo
-- MAGIC representa sinalizações por regra, e não corridas únicas rejeitadas.

-- COMMAND ----------

SELECT
  layer AS camada,
  table_name AS tabela,
  rule_id AS regra,
  total_rows AS registros_avaliados,
  violation_count AS registros_sinalizados,
  ROUND(violation_percentage, 6) AS percentual_sinalizado,
  status
FROM case_ifood.tlc_data_quality.dq_execution_summary
WHERE table_name LIKE '%yellow%'
ORDER BY camada, tabela, regra;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Resumo de sinalizações por camada
-- MAGIC
-- MAGIC Esta taxa usa pares registro-regra como denominador. Ela não deve ser
-- MAGIC interpretada como percentual de registros únicos com problema.

-- COMMAND ----------

SELECT
  layer AS camada,
  table_name AS tabela,
  SUM(total_rows) AS verificacoes_executadas,
  SUM(violation_count) AS sinalizacoes,
  ROUND(
    100.0 * SUM(violation_count) / NULLIF(SUM(total_rows), 0),
    6
  ) AS percentual_sinalizacoes
FROM case_ifood.tlc_data_quality.dq_execution_summary
WHERE table_name LIKE '%yellow%'
GROUP BY layer, table_name
ORDER BY camada, tabela;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## 7. Relação entre duração e valor total
-- MAGIC
-- MAGIC A correlação é calculada somente para cronologias válidas com duração
-- MAGIC de até 24 horas e valor informado. Isso evita que anomalias temporais
-- MAGIC dominem a medida, sem removê-las da tabela de origem.

-- COMMAND ----------

SELECT
  ROUND(CORR(trip_duration_minutes, total_amount), 4)
    AS correlacao_duracao_valor,
  COUNT(*) AS corridas_consideradas
FROM eda_yellow_trips
WHERE trip_duration_minutes BETWEEN 0 AND 1440
  AND total_amount IS NOT NULL;

-- COMMAND ----------

WITH valid_trips AS (
  SELECT
    CASE
      WHEN trip_duration_minutes <= 5 THEN '01. Até 5 min'
      WHEN trip_duration_minutes <= 15 THEN '02. 5 a 15 min'
      WHEN trip_duration_minutes <= 30 THEN '03. 15 a 30 min'
      WHEN trip_duration_minutes <= 60 THEN '04. 30 a 60 min'
      WHEN trip_duration_minutes <= 120 THEN '05. 1 a 2 h'
      ELSE '06. 2 a 24 h'
    END AS faixa_duracao,
    trip_duration_minutes,
    total_amount
  FROM eda_yellow_trips
  WHERE trip_duration_minutes BETWEEN 0 AND 1440
    AND total_amount IS NOT NULL
)
SELECT
  faixa_duracao,
  COUNT(*) AS quantidade_corridas,
  ROUND(AVG(trip_duration_minutes), 2) AS duracao_media_minutos,
  ROUND(AVG(total_amount), 2) AS valor_total_medio,
  ROUND(PERCENTILE_APPROX(total_amount, 0.5), 2) AS mediana_total_amount
FROM valid_trips
GROUP BY faixa_duracao
ORDER BY faixa_duracao;
