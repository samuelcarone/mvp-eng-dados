# MVP – Engenharia de Dados

Este projeto desenvolve um pipeline de dados para analisar a correlação entre IPCA, SELIC e IBOVESPA nos últimos 15 anos.

## Objetivo
Implementar um pipeline de dados baseado nas camadas BRONZE, SILVER e GOLD no Databricks, realizar modelagem em estrela e aplicar análises estatísticas.

## Estrutura do Projeto

- **Camada BRONZE:** Dados brutos carregados de arquivos CSV.
- **Camada SILVER:** Dados tratados, padronizados e limpos.
- **Camada GOLD:** Dados organizados em tabelas Delta registradas no metastore.
- **Modelagem Estrela:** Fato e dimensões para análise econômica.
- **Consultas SQL:** JOINs entre tabelas fato e dimensão.
- **Análises Estatísticas:** Correlação e regressão linear.
- **Visualizações:** Gráficos gerados para análise de tendência.

## Arquivos Principais

- [`Final_MVP.ipynb`](./Final_MVP.ipynb): Notebook único contendo todo o pipeline e as análises.

## Prints

Prints dos principais resultados estão disponíveis na pasta [`prints/`](./prints/):
- Criação da camada GOLD
- Consultas SQL com JOINs
- Matriz de Correlação
- Gráfico da Regressão Linear

## Como Rodar

1. Subir os arquivos de dados no Databricks.
2. Executar o notebook `Final_MVP.ipynb`.
3. Validar a criação das camadas e realizar as análises.

## Observações

- Projeto desenvolvido no **Databricks Community Edition**.
- Banco de dados: `mvp_economia`.
- Linguagens: **Python**, **SQL** e **Markdown**.

