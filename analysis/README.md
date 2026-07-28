# Análises

- `perguntas/01_respostas_analiticas.sql`: notebook Databricks SQL com as respostas
  analíticas sobre valor total mensal e passageiros por hora em maio de 2023.
- `perguntas/01_respostas_analiticas.md`: snapshot interpretado dos resultados
  retornados pelo notebook SQL do case.
- `perguntas/01_media_total_mensal.csv`: resultado tabular da média mensal de
  `total_amount`.
- `perguntas/02_media_passageiros_hora_maio.csv`: resultado tabular da média de
  passageiros por hora em maio para Yellow + Green, apenas Yellow e apenas
  Green.
- `eda/01_eda_gold.sql`: notebook Databricks SQL com evolução mensal, receita,
  fornecedores, passageiros, duração, sinalizações de qualidade e relação
  entre duração e valor na camada Gold.
- `eda/01_eda_gold.md`: snapshot interpretado das sete análises executadas pelo
  notebook de EDA.

Os notebooks consultam a camada Gold por meio de nomes totalmente qualificados,
sem depender do catálogo ou schema selecionado na sessão.
