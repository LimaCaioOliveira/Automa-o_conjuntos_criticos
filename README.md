# 🤖 Bot de Monitoramento de Conjuntos Críticos (Telegram Alerts)

![Python](https://img.shields.io/badge/Python-3.8%2B-blue)
![ETL](https://img.shields.io/badge/Type-ETL%20Automation-green)
![Oracle](https://img.shields.io/badge/Database-Oracle-red)

## 📋 Visão Geral

Este script implementa um **robô de automação (RPA/ETL)** responsável por monitorar em tempo real a ocorrência de eventos em conjuntos elétricos considerados críticos.

O sistema conecta-se diretamente ao Data Warehouse (Oracle), cruza as informações com uma matriz de criticidade (Excel) e dispara alertas formatados para a equipe de operação via **Telegram**, permitindo uma resposta ágil a incidentes de alto impacto.

---

## 🚀 Funcionalidades Principais

* **Extração de Dados (Oracle SQL):** Conexão via `pyodbc` para execução de queries complexas que calculam SLA e tempo de atendimento em tempo real.
* **Filtro de Criticidade (Excel):** Cruzamento de dados (`inner join`) com planilha de controle (`Conj critico.xlsx`) para filtrar apenas ativos prioritários.
* **Gestão de Estado (`State Management`):** Sistema inteligente que armazena o panorama anterior em JSON (`panorama_conjunto.json`) para calcular a variação de ocorrências (+/-) entre execuções.
* **Mensageria Formatada:** Envio de alertas ricos (Markdown + Emojis) via API do Telegram, detalhando regional, abrangência e impacto (CI).
* **Logging Robusto:** Registro detalhado de execução em arquivo (`robo_conjunto_critico.log`) para auditoria e debugging.
* **Modo Contingência:** Mecanismo de segurança que evita falhas silenciosas caso o banco de dados esteja inacessível.

---

## 🛠️ Tecnologias e Bibliotecas

* **Python 3.10+**
* `pyodbc`: Conectividade ODBC com Oracle Database.
* `pandas`: Manipulação, limpeza e cruzamento de DataFrames.
* `requests`: Comunicação com a API do Telegram.
* `python-dotenv`: Gestão segura de credenciais.
* `json` & `logging`: Persistência de estado e rastro de execução.

---

## ⚙️ Configuração e Instalação

### 1. Pré-requisitos
* Driver ODBC para Oracle instalado e configurado no sistema operacional.
* Arquivo `Conj critico.xlsx` presente na raiz do diretório (contendo as colunas `CONJUNTO` e `CRITICO?`).

### 2. Instalação das Dependências
```bash
pip install pandas pyodbc requests python-dotenv openpyxl
