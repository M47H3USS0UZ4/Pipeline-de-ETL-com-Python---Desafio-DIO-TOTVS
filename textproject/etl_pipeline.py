import asyncio
import logging
import os
from dataclasses import dataclass
from typing import Any, Dict, List, Optional

import pandas as pd
from openai import AsyncOpenAI

# Logging estruturado (obrigatório)
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")


@dataclass(frozen=True)
class PipelinePaths:
    """Estrutura imutável com caminhos padrão do pipeline."""

    input_csv: str
    output_csv: str


def extract_data(input_path: str) -> pd.DataFrame:
    """Extrai os dados do CSV de entrada.

    Args:
        input_path: Caminho do arquivo CSV de entrada.

    Returns:
        DataFrame contendo os dados brutos.

    Raises:
        OSError: Se houver falha de I/O ao ler o arquivo.
        ValueError: Se o arquivo não contiver as colunas esperadas.
    """
    logging.info("Iniciando extração de dados do arquivo: %s", input_path)

    try:
        df: pd.DataFrame = pd.read_csv(input_path)
    except OSError as exc:
        # logging.exception é reservado para erros críticos de I/O (exigência)
        logging.exception("Falha crítica de I/O ao ler o CSV de entrada.")
        raise

    required_columns: set[str] = {"UserID", "Nome", "Conta", "Cartao"}
    missing: set[str] = required_columns - set(df.columns)

    if missing:
        raise ValueError(f"CSV de entrada inválido. Colunas ausentes: {sorted(missing)}")

    logging.info("Extração concluída. Registros carregados: %d", len(df))
    return df


async def _enrich_message_for_user(
    client: AsyncOpenAI,
    semaphore: asyncio.Semaphore,
    user_name: str,
) -> str:
    """Gera mensagem enriquecida via OpenAI, com controle de concorrência e fallback por usuário.

    Regras:
      - Qualquer falha na chamada (timeout, rate limit, erro de API, etc.) deve:
        - registrar logging.warning()
        - retornar o fallback apenas para este usuário

    Args:
        client: Cliente assíncrono da OpenAI.
        semaphore: Semáforo para limitar concorrência.
        user_name: Nome do usuário (coluna Nome).

    Returns:
        Mensagem enriquecida (ou fallback individual).
    """
    fallback: str = f"Olá {user_name}, invista hoje e garanta o seu futuro!"

    prompt: str = (
        "Você é um assistente que cria mensagens curtas, amigáveis e personalizadas "
        "em português do Brasil para incentivar educação financeira e investimento responsável. "
        f"Crie uma mensagem de até 1 frase para a pessoa chamada {user_name}."
    )

    async with semaphore:
        try:
            response = await client.chat.completions.create(
                model="gpt-4.1-mini",
                messages=[
                    {"role": "system", "content": "Responda sempre em pt-BR, de forma objetiva."},
                    {"role": "user", "content": prompt},
                ],
                temperature=0.7,
            )
            content: str = (response.choices[0].message.content or "").strip()
            return content if content else fallback
        except Exception as exc:
            logging.warning(
                "Falha ao enriquecer mensagem para '%s'. Aplicando fallback individual. Motivo: %s",
                user_name,
                str(exc),
            )
            return fallback


async def transform_data(df: pd.DataFrame) -> pd.DataFrame:
    """Transforma os dados, enriquecendo mensagens por usuário (assíncrono).

    Regras de fallback (prioridade):
      1) Se OPENAI_API_KEY não existir: bypass total e fallback genérico imediato para todos.
         (não instanciar cliente)
      2) Se existir, mas houver falha na chamada por usuário: warning e fallback apenas para ele.

    Controle de concorrência:
      - asyncio.Semaphore(5)
      - asyncio.gather() para paralelizar

    Args:
        df: DataFrame de entrada.

    Returns:
        DataFrame enriquecido com a coluna "Mensagem".
    """
    logging.info("Iniciando transformação/enriquecimento dos dados (async).")

    api_key: Optional[str] = os.getenv("OPENAI_API_KEY")
    df_out: pd.DataFrame = df.copy()

    # Regra 1: bypass total se não houver chave
    if not api_key:
        logging.info(
            "OPENAI_API_KEY ausente. Bypass total das chamadas OpenAI e aplicação de fallback genérico."
        )
        df_out["Mensagem"] = df_out["Nome"].astype(str).map(
            lambda nome: f"Olá {nome}, invista hoje e garanta o seu futuro!"
        )
        return df_out

    client: AsyncOpenAI = AsyncOpenAI(api_key=api_key)
    semaphore: asyncio.Semaphore = asyncio.Semaphore(5)

    nomes: List[str] = df_out["Nome"].astype(str).tolist()

    tasks: List[asyncio.Task[str]] = [
        asyncio.create_task(_enrich_message_for_user(client, semaphore, nome)) for nome in nomes
    ]

    mensagens: List[str] = await asyncio.gather(*tasks)
    df_out["Mensagem"] = mensagens

    logging.info("Transformação concluída. Mensagens enriquecidas geradas: %d", len(mensagens))
    return df_out


def load_data(df_enriched: pd.DataFrame, output_path: str) -> None:
    """Carrega/salva o DataFrame enriquecido em CSV.

    Requisito:
      - encoding='utf-8-sig' para compatibilidade com Microsoft Excel.

    Args:
        df_enriched: DataFrame já enriquecido.
        output_path: Caminho do arquivo CSV de saída.

    Raises:
        OSError: Se houver falha crítica de I/O ao escrever o arquivo.
    """
    logging.info("Iniciando carga (save) do CSV enriquecido em: %s", output_path)

    try:
        df_enriched.to_csv(output_path, index=False, encoding="utf-8-sig")
    except OSError:
        # logging.exception é reservado para erros críticos de I/O (exigência)
        logging.exception("Falha crítica de I/O ao escrever o CSV de saída.")
        raise

    logging.info("Carga concluída com sucesso. Arquivo gerado: %s", output_path)


async def _run_transform_async(df: pd.DataFrame) -> pd.DataFrame:
    """Wrapper assíncrono para transformar dados.

    Args:
        df: DataFrame extraído.

    Returns:
        DataFrame enriquecido.
    """
    return await transform_data(df)


def run_pipeline(
    input_path: str = "data/SDW2023.csv",
    output_path: str = "data/SDW2023_Enriched.csv",
) -> pd.DataFrame:
    """Executa o pipeline ETL completo (síncrono), orquestrando as etapas.

    Requisito:
      - run_pipeline() deve ser síncrona e chamar a parte async via asyncio.run().

    Args:
        input_path: Caminho do CSV de entrada.
        output_path: Caminho do CSV de saída.

    Returns:
        DataFrame enriquecido pronto para uso na UI.

    Raises:
        OSError: Para falhas críticas de I/O (leitura/escrita).
        ValueError: Para problemas de schema do CSV de entrada.
    """
    logging.info("Iniciando pipeline ETL (Extract -> Transform -> Load).")

    df_raw: pd.DataFrame = extract_data(input_path)

    # Invocação da parte assíncrona a partir de função síncrona
    df_enriched: pd.DataFrame = asyncio.run(_run_transform_async(df_raw))

    load_data(df_enriched, output_path)

    logging.info("Pipeline concluído com sucesso.")
    return df_enriched