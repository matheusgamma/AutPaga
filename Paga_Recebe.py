import streamlit as st
import pandas as pd
from io import BytesIO
import numpy as np
import yfinance as yf


st.set_page_config(page_title="Unificação de Operações", layout="wide")


# ==============================
# Funções auxiliares
# ==============================

@st.cache_data(show_spinner=False, ttl=60 * 10)
def preco_mercado_b3(ativo: str) -> float | None:
    if not ativo or pd.isna(ativo):
        return None

    ativo = str(ativo).strip().upper()

    # padrão B3
    ticker = ativo if ativo.endswith(".SA") else f"{ativo}.SA"

    try:
        tk = yf.Ticker(ticker)
        # tenta fast_info (mais rápido)
        price = None
        if hasattr(tk, "fast_info") and tk.fast_info:
            price = tk.fast_info.get("last_price", None)

        # fallback: histórico
        if price is None:
            hist = tk.history(period="5d")
            if hist is not None and not hist.empty:
                price = float(hist["Close"].dropna().iloc[-1])

        return float(price) if price is not None else None
    except Exception:
        return None

def carregar_arquivo(uploaded_file) -> pd.DataFrame | None:
    """Lê CSV ou Excel e retorna DataFrame."""
    if uploaded_file is None:
        return None

    nome = uploaded_file.name.lower()

    try:
        if nome.endswith(".csv"):
            df = pd.read_csv(uploaded_file, sep=";", decimal=",")
        else:
            df = pd.read_excel(uploaded_file)
        return df
    except Exception as e:
        st.error(f"Erro ao ler o arquivo {uploaded_file.name}: {e}")
        return None


def processar_dados(df_assessores: pd.DataFrame, df_ops: pd.DataFrame, df_dash: pd.DataFrame) -> tuple[pd.DataFrame, list[str]]:
    def br_to_float(x):
        if pd.isna(x):
            return pd.NA
        if isinstance(x, (int, float)):
            return float(x)
        s = str(x).strip()
        if s == "":
            return pd.NA
        s = s.replace("R$", "").replace(" ", "")
        has_comma = "," in s
        has_dot = "." in s
        try:
            if has_comma and has_dot:
                if s.rfind(",") > s.rfind("."):   # BR
                    s = s.replace(".", "").replace(",", ".")
                else:                              # US
                    s = s.replace(",", "")
            elif has_comma and not has_dot:
                s = s.replace(",", ".")
            return float(s)
        except Exception:
            return pd.NA

    def norm_date(x):
        dt = pd.to_datetime(x, errors="coerce", dayfirst=True)
        if pd.isna(dt):
            return pd.NaT
        return dt.date()

    # Valida colunas mínimas
    cols_assessores_obrig = {"Conta", "Nome", "Assessor"}
    cols_ops_obrig = {
        "Data Operação", "Conta Cliente", "Ativo", "Preço Exercício",
        "Quantidade", "Fixing", "Estrutura", "Ref", "Bid(+)/Offer(-)", "Código do Produto",
        "Tipo Operação", "Tipo Opção"
    }
    cols_dash_obrig = {"Conta", "Ativo", "Data de Fixing", "Preço de Abertura"}

    faltando_ass = cols_assessores_obrig - set(df_assessores.columns)
    faltando_ops = cols_ops_obrig - set(df_ops.columns)
    faltando_dash = cols_dash_obrig - set(df_dash.columns)

    if faltando_ass:
        raise ValueError(f"Faltam colunas na base de assessores: {faltando_ass}")
    if faltando_ops:
        raise ValueError(f"Faltam colunas na planilha padrão: {faltando_ops}")
    if faltando_dash:
        raise ValueError(f"Faltam colunas no Dash: {faltando_dash}")

    df_ops = df_ops.copy()
    df_assessores = df_assessores.copy()
    df_dash = df_dash.copy()

    avisos: list[str] = []

    # 1) Unificar operações
    group_cols = [
        "Data Operação", "Conta Cliente", "Ativo", "Fixing", "Estrutura", "Ref", "Código do Produto"
    ]

    agg_dict = {
        "Tipo Operação": lambda x: ", ".join(sorted(set(x.dropna()))),
        "Tipo Opção": lambda x: ", ".join(sorted(set(x.dropna()))),
        "Preço Exercício": "min",
        "Quantidade": "max",
        "Bid(+)/Offer(-)": "sum",
    }

    df_grouped = (
        df_ops.groupby(group_cols, dropna=False)
        .agg(agg_dict)
        .reset_index()
    )

    # 2) Cruzar com assessores
    df_merged = df_grouped.merge(
        df_assessores[["Conta", "Nome", "Assessor"]],
        left_on="Conta_Cliente",
        right_on="Conta",
        how="left",
    ).rename(columns={
        "Nome": "Nome Cliente",
        "Bid(+)/Offer(-)": "Paga/Recebe",
    })

    n_sem_assessor = df_merged["Assessor"].isna().sum()
    if n_sem_assessor:
        avisos.append(
            f"{n_sem_assessor} linha(s) sem correspondência na base de Assessores "
            "(Conta_Cliente não encontrada)."
        )

    # 3) Normalizar tipos
    df_merged["Conta_Cliente"] = pd.to_numeric(df_merged["Conta_Cliente"], errors="coerce")
    df_merged["Ativo"] = df_merged["Ativo"].astype(str).str.strip().str.upper()
    df_merged["Fixing_norm"] = df_merged["Fixing"].apply(norm_date)

    df_merged["Preço Exercício"] = df_merged["Preço Exercício"].apply(br_to_float)
    df_merged["Quantidade"] = df_merged["Quantidade"].apply(br_to_float)
    df_merged["Paga/Recebe"] = df_merged["Paga/Recebe"].apply(br_to_float)

    # 4) Dash: usar só Preço Abertura (entrada)
    df_dash["Conta"] = pd.to_numeric(df_dash["Conta"], errors="coerce")
    df_dash["Ativo"] = df_dash["Ativo"].astype(str).str.strip().str.upper()
    df_dash["Fixing_norm"] = df_dash["Data de Fixing"].apply(norm_date)
    df_dash["Preço Abertura"] = df_dash["Preço de Abertura"].apply(br_to_float)

    dash_keys = ["Conta", "Ativo", "Fixing_norm"]
    df_dash_min = (
        df_dash[dash_keys + ["Preço Abertura"]]
        .dropna(subset=dash_keys)
        .drop_duplicates(dash_keys)
    )

    df_merged = df_merged.merge(
        df_dash_min,
        left_on=["Conta_Cliente", "Ativo", "Fixing_norm"],
        right_on=["Conta", "Ativo", "Fixing_norm"],
        how="left",
    )

    n_sem_abertura = df_merged["Preço Abertura"].isna().sum()
    if n_sem_abertura:
        avisos.append(
            f"{n_sem_abertura} linha(s) sem correspondência no Dash "
            "(Conta/Ativo/Data de Fixing não encontrados) — 'Lucro saindo' ficará em branco para elas."
        )

    # 5) Preço de mercado REAL (yfinance)
    df_merged["Preço mercado"] = df_merged["Ativo"].apply(preco_mercado_b3)

    n_sem_preco_mercado = df_merged["Preço mercado"].isna().sum()
    if n_sem_preco_mercado:
        avisos.append(
            f"{n_sem_preco_mercado} ativo(s) sem preço de mercado retornado pelo yfinance."
        )

    # 6) Colunas finais
    df_merged["Cliente_Paga_Recebe"] = df_merged["Paga/Recebe"].apply(
        lambda x: "PAGA" if pd.notnull(x) and x < 0 else ("RECEBE" if pd.notnull(x) and x > 0 else "NEUTRO")
    )

    df_merged["Preço final"] = df_merged["Preço mercado"] + df_merged["Paga/Recebe"]

    # Lucro saindo (%) -> percentual mesmo
    df_merged["Lucro saindo"] = ((df_merged["Preço final"] / df_merged["Preço Abertura"]) - 1) * 100

    df_merged["Bid total"] = df_merged["Quantidade"] * df_merged["Paga/Recebe"]
    df_merged["Nocional entrada"] = df_merged["Preço Abertura"] * df_merged["Quantidade"]
    df_merged["Nocional saida"] = df_merged["Preço final"] * df_merged["Quantidade"]
    df_merged["Lucro $ Saindo hoje"] = df_merged["Nocional saida"] - df_merged["Nocional entrada"]

    colunas_saida = [
        "Data Operação", "Conta Cliente", "Assessor", "Nome Cliente",
        "Ativo", "Preço Exercício", "Quantidade", "Fixing", "Estrutura",
        "Paga/Recebe", "Cliente_Paga_Recebe",
        "Preço Abertura", "Preço mercado", "Preço final", "Lucro saindo",
        "Bid total", "Nocional entrada", "Nocional saida", "Lucro $ Saindo hoje"
    ]

    colunas_saida = [c for c in colunas_saida if c in df_merged.columns]
    return df_merged[colunas_saida], avisos



def gerar_excel_para_download(df: pd.DataFrame) -> BytesIO:
    output = BytesIO()

    with pd.ExcelWriter(output, engine="xlsxwriter") as writer:
        df.to_excel(writer, index=False, sheet_name="Resultado")

        wb = writer.book
        ws = writer.sheets["Resultado"]

        fmt_money = wb.add_format({"num_format": 'R$ #,##0.00'})
        fmt_pct = wb.add_format({"num_format": '0.00"%"'})

        # Mapa de colunas por nome -> índice
        col_index = {name: i for i, name in enumerate(df.columns)}

        # Percentual
        if "Lucro saindo" in col_index:
            ws.set_column(col_index["Lucro saindo"], col_index["Lucro saindo"], 14, fmt_pct)

        # Moeda (a partir de Preço Abertura)
        money_cols = [
            "Preço Abertura", "Preço mercado", "Preço final",
            "Bid total", "Nocional entrada", "Nocional saida", "Lucro $ Saindo hoje"
        ]
        for c in money_cols:
            if c in col_index:
                ws.set_column(col_index[c], col_index[c], 16, fmt_money)

        # Ajustar largura das colunas “texto”
        for c in ["Assessor", "Nome Cliente", "Estrutura", "Ativo"]:
            if c in col_index:
                ws.set_column(col_index[c], col_index[c], 18)

    output.seek(0)
    return output


# ==============================
# Interface Streamlit
# ==============================

st.title("Unificação de Operações - Renova")

st.markdown(
    """
    **Fluxo:**
    1. Envie a **Base de Assessores**
    2. Envie a **Planilha Padrão de Operações**  
    3. Clique em **Processar** para unificar operações  
    """
)

col1, col2, col3 = st.columns(3)

with col1:
    file_assessores = st.file_uploader(
        "📂 Base de Assessores",
        type=["xlsx", "xls", "csv"],
        key="file_assessores",
    )

with col2:
    file_ops = st.file_uploader(
        "📂 Planilha Padrão de Operações",
        type=["xlsx", "xls", "csv"],
        key="file_ops",
    )

with col3:
    file_dash = st.file_uploader(
        "📂 Dash Preço de Abertura",
        type=["xlsx", "xls", "csv"],
        key="file_dash",
    )



if st.button("🚀 Processar"):
    if not file_assessores or not file_ops or not file_dash:
        st.warning("Envie as **três** planilhas antes de processar.")
    else:
        df_assessores = carregar_arquivo(file_assessores)
        df_ops = carregar_arquivo(file_ops)
        df_dash = carregar_arquivo(file_dash)

        if df_assessores is None or df_ops is None or df_dash is None:
            st.stop()

        try:
            df_resultado, avisos = processar_dados(df_assessores, df_ops, df_dash)
        except Exception as e:
            st.error(f"Erro ao processar os dados: {e}")
            st.stop()

        st.success("Processamento concluído com sucesso! ✅")

        for aviso in avisos:
            st.info(aviso)

        st.subheader("Prévia do Resultado Unificado")
        st.dataframe(df_resultado.head(100))

        excel_bytes = gerar_excel_para_download(df_resultado)

        st.download_button(
            label="📥 Baixar resultado em Excel",
            data=excel_bytes,
            file_name="resultado_unificado.xlsx",
            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        )

