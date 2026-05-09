# textproject — ETL Assíncrono Production-Ready (OpenAI + Streamlit)

![Python](https://img.shields.io/badge/Python-3.10%2B-blue)
![OpenAI](https://img.shields.io/badge/OpenAI-Async%20SDK-black)
![Streamlit](https://img.shields.io/badge/Streamlit-App-red)
![Pandas](https://img.shields.io/badge/Pandas-DataFrame-purple)

## Visão Geral

Este projeto implementa um pipeline **ETL (Extract, Transform, Load)** com qualidade de produção para um desafio de portfólio, executável no **GitHub Codespaces** e renderizado via **Streamlit**.

O objetivo é:
- Ler um CSV mockado com usuários (`data/SDW2023.csv`)
- Enriquecer os registros com uma mensagem personalizada (via OpenAI quando disponível)
- Salvar o output em `data/SDW2023_Enriched.csv` (gerado automaticamente em runtime)

## Fluxo ETL

### 1) Extract (extração)
- Implementado em `extract_data()`
- Lê `data/SDW2023.csv` com Pandas
- Valida colunas obrigatórias: `UserID`, `Nome`, `Conta`, `Cartao`

### 2) Transform (transformação / enriquecimento)
- Implementado em `transform_data()` (assíncrono)
- Usa `AsyncOpenAI` (nova SDK) e `asyncio`
- Possui **controle de concorrência** com `asyncio.Semaphore(5)` para reduzir risco de **Rate Limit (HTTP 429)**
- Processa em paralelo com `asyncio.gather()`

**Regras de fallback (prioridade):**
1. Se `OPENAI_API_KEY` estiver ausente → **bypass total** e aplica fallback genérico para todos (sem instanciar cliente).
2. Se a chave existir, mas ocorrer erro por usuário (timeout, rate limit, erro de API, etc.) → `logging.warning()` e fallback **apenas para aquele usuário**.

Fallback usado:
> `Olá {Nome}, invista hoje e garanta o seu futuro!`

### 3) Load (carga / persistência)
- Implementado em `load_data()`
- Salva CSV enriquecido com `encoding='utf-8-sig'` para compatibilidade com Microsoft Excel

## Como executar no Codespaces (passo a passo)

1. Clone o repositório (ou abra no Codespaces)
2. Instale dependências:
   ```bash
   pip install -r requirements.txt
   ```
3. Configure variáveis de ambiente:
   - Copie `.env.example` para `.env`
   - Preencha sua chave:
     ```bash
     cp .env.example .env
     # edite o arquivo .env e informe sua OPENAI_API_KEY
     ```
4. Rode o app:
   ```bash
   streamlit run app.py
   ```

## Observações de Engenharia

- A UI (`app.py`) **não contém** lógica de ETL nem chamadas diretas de API.
- O ETL (`etl_pipeline.py`) é **isolado** e pode ser testado/reutilizado.
- Logs em formato estruturado e nível INFO.

---

## License

MIT License

Copyright (c) 2026

Permission is hereby granted, free of charge, to any person obtaining a copy
of this software and associated documentation files (the "Software"), to deal
in the Software without restriction, including without limitation the rights
to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
copies of the Software, and to permit persons to whom the Software is
furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in all
copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
SOFTWARE.