# 📊 MVP - Engenharia de Dados

**Tema:**  
Análise da Correlação entre SELIC, IPCA e IBOVESPA

**Aluno:** Samuel Carone  
**Instituição:** PUC-Rio  
**Disciplina:** Fundamentos de Bancos de Dados

---

## 🔥 Objetivo

**Problema:**  
Investigar a correlação entre a taxa básica de juros (SELIC), a inflação (IPCA) e o desempenho do IBOVESPA no Brasil ao longo dos últimos 15 anos.

**Perguntas de Negócio:**
1. Existe correlação entre a taxa SELIC e o desempenho do IBOVESPA?
2. Existe correlação entre o IPCA e o desempenho do IBOVESPA?
3. Existe correlação entre o IPCA e a taxa SELIC?
4. Em quais períodos o IBOVESPA teve seu melhor desempenho em relação às variações da SELIC e do IPCA?
5. Como a inflação (IPCA) influencia o comportamento da bolsa de valores (IBOVESPA)?
6. Como a taxa de juros (SELIC) influencia o comportamento da bolsa de valores (IBOVESPA)?

---

## 🛠️ Pipeline de Dados

**Plataforma utilizada:** Databricks Community Edition

**Etapas:**
- **Coleta:** Dados públicos extraídos de fontes confiáveis (Banco Central do Brasil e Yahoo Finance).
- **Modelagem:** Esquema Estrela com tabelas fato e dimensões.
- **Transformação:** Padronização e limpeza dos dados.
- **Carga:** Persistência nas camadas Bronze, Silver e Gold no Databricks.
- **Análise:** Análise de qualidade e análise de correlação entre variáveis.

---

## 📚 Catálogo de Dados

| Coluna         | Tipo de Dado | Descrição | Domínio / Valores Esperados | Fonte |
|----------------|--------------|-----------|------------------------------|-------|
| ano            | Inteiro      | Ano da observação | 2008–2023 | Todos |
| mes            | Inteiro      | Mês da observação | 1–12 | Todos |
| ipca_variacao  | Float        | Variação mensal do IPCA (%) | -1% a +2% | Banco Central |
| selic_media    | Float        | Taxa média mensal da SELIC (%) | 2% a 15% | Banco Central (simulado) |
| ibov_fechamento| Float        | Fechamento mensal do IBOVESPA (pontos) | 40.000 a 130.000 | Yahoo Finance |

**Linhagem:**  
- Coleta manual de dados públicos.
- Transformação via PySpark.
- Modelagem e carga em tabelas no metastore Databricks.

---

## 🔍 Análise de Qualidade

- **IPCA:** Sem valores inconsistentes.
- **SELIC:** Dados simulados, controlados.
- **IBOVESPA:** Dados reais, consistentes.

---

## 📈 Resultados da Análise

- Correlação negativa moderada entre a SELIC e o IBOVESPA.
- Correlação negativa fraca entre o IPCA e o IBOVESPA.
- Correlação positiva moderada entre o IPCA e a SELIC.

Gráficos e matriz de correlação confirmam os resultados esperados.

---

## ✍️ Autoavaliação

**Atingimento dos Objetivos:**  
Todos os objetivos iniciais foram atingidos, com a construção de um pipeline completo e análise de correlação.

**Dificuldades Encontradas:**  
- Modelagem de tabelas fato e dimensão.
- Salvamento correto no metastore do Databricks.

**Trabalhos Futuros:**  
- Coletar dados em tempo real via APIs.
- Aplicar machine learning para previsão de variáveis econômicas.

---

## 📚 Referências

- [Banco Central do Brasil](https://www.bcb.gov.br)
- [Yahoo Finance](https://finance.yahoo.com)
- [Documentação Databricks](https://docs.databricks.com)



