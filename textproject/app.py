from __future__ import annotations

import streamlit as st
import pandas as pd

from etl_pipeline import run_pipeline


def _init_session_state() -> None:
    """Inicializa variáveis explícitas de estado na sessão do Streamlit.

    Returns:
        None
    """
    if "pipeline_ran" not in st.session_state:
        st.session_state.pipeline_ran = False  # booleano, default False
    if "df_enriched" not in st.session_state:
        st.session_state.df_enriched = None  # DataFrame resultante


def main() -> None:
    """Renderiza a UI e executa o pipeline sob demanda.

    Regras:
      - app.py NÃO contém lógica de ETL nem chamadas de API diretamente.
      - Envolve run_pipeline() em try/except.
      - Se pipeline_ran=True, exibe cache sem re-executar.

    Returns:
        None
    """
    st.set_page_config(page_title="ETL Assíncrono (OpenAI) - Portfólio", layout="wide")
    _init_session_state()

    st.title("Pipeline ETL Production-Ready (Async) — Desafio de Portfólio")
    st.caption("Interface em Streamlit. Extração/Transformação/Carga isoladas em etl_pipeline.py.")

    col1, col2 = st.columns([1, 2])

    with col1:
        st.subheader("Controles")
        if st.session_state.pipeline_ran:
            st.success("Pipeline já foi executado nesta sessão. Usando dados em cache.")
        else:
            st.info("Pipeline ainda não foi executado nesta sessão.")

        run_clicked: bool = st.button("Executar Pipeline", type="primary", use_container_width=True)

    with col2:
        st.subheader("Resultado")

        if st.session_state.pipeline_ran and isinstance(st.session_state.df_enriched, pd.DataFrame):
            st.dataframe(st.session_state.df_enriched, use_container_width=True)
            st.caption("Exibindo DataFrame em cache (sem re-executar o pipeline).")
            return

        if run_clicked:
            try:
                df_enriched: pd.DataFrame = run_pipeline()
                st.session_state.pipeline_ran = True
                st.session_state.df_enriched = df_enriched
                st.success("Pipeline executado com sucesso!")
                st.dataframe(df_enriched, use_container_width=True)
            except Exception as exc:
                st.error(
                    "Ocorreu um erro ao executar o pipeline. "
                    "Verifique os logs e a configuração do ambiente (ex.: .env)."
                )
                st.caption(f"Detalhes técnicos (para depuração): {type(exc).__name__}: {exc}")


if __name__ == "__main__":
    main()