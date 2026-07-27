# Análises

- `perguntas/01_respostas_analiticas.sql`: notebook Databricks SQL com as respostas
  analíticas sobre valor total mensal e passageiros por hora em maio de 2023.
- `perguntas/01_respostas_analiticas.md`: snapshot interpretado dos resultados
  retornados pelo notebook SQL do case.
- `eda/01_eda_gold.sql`: notebook Databricks SQL com evolução mensal, receita,
  fornecedores, passageiros, duração, sinalizações de qualidade e relação
  entre duração e valor na camada Gold.
- `eda/01_eda_gold.md`: snapshot interpretado das sete análises executadas pelo
  notebook de EDA.

Os notebooks consultam a camada Gold por meio de nomes totalmente qualificados,
sem depender do catálogo ou schema selecionado na sessão.
