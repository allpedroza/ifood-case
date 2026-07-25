# Análises

- `01_respostas_analiticas.sql`: notebook Databricks SQL com as respostas
  analíticas sobre valor total mensal e passageiros por hora em maio de 2023.
- `02_profiling_gold.py`: notebook Databricks Python com métricas completas em
  Spark e profiling detalhado de uma amostra Pandas da tabela Gold.

Os notebooks consultam a camada Gold por meio de nomes totalmente qualificados,
sem depender do catálogo ou schema selecionado na sessão.
