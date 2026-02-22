import streamlit as st
import pandas as pd
from io import BytesIO
import numpy as np
import yfinance as yf


st.set_page_config(page_title="Unificação de Operações", layout="wide")


# ==============================
# Funções auxiliares
# ==============================

# =========================
# NORMALIZAÇÃO DE PREÇO (centavos / lote)
# =========================
def normalizar_preco(p):
    if pd.isna(p):
        return p
    # preços absurdos pra ação brasileira → provavelmente centavos ou lote
    if p > 1000:
        return p / 100
    return p

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


def primeira_nao_nula(serie: pd.Series):
    """Retorna o primeiro valor não nulo da série, ou None."""
    serie_drop = serie.dropna()
    return serie_drop.iloc[0] if not serie_drop.empty else None

@st.cache_data(show_spinner=False, ttl=60 * 30)
def get_preco_mercado_yf(ativo: str) -> float | None:
    """
    Puxa o preço de mercado via yfinance.
    Para B3, tenta sufixo .SA (ex: RAIL3 -> RAIL3.SA).
    """
    if not ativo or pd.isna(ativo):
        return None

    ativo = str(ativo).strip().upper()

    # tenta como veio
    tickers_try = [ativo]

    # se parece ticker B3, tenta .SA
    if ativo.endswith(("3", "4", "11", "5", "6")) and ".SA" not in ativo:
        tickers_try.append(f"{ativo}.SA")

    for t in tickers_try:
        try:
            tk = yf.Ticker(t)
            # fast_info costuma ser mais rápido quando disponível
            price = None
            if hasattr(tk, "fast_info") and tk.fast_info:
                price = tk.fast_info.get("last_price", None)

            if price is None:
                hist = tk.history(period="5d")
                if hist is not None and not hist.empty:
                    price = float(hist["Close"].dropna().iloc[-1])

            if price is not None and not (isinstance(price, float) and np.isnan(price)):
                return float(price)
        except Exception:
            continue

    return None




def br_to_float(x):
    """
    Converte número vindo como '20,67' ou '20.67' para float.
    """
    if pd.isna(x):
        return np.nan
    if isinstance(x, (int, float, np.number)):
        return float(x)
    s = str(x).strip()
    if s == "":
        return np.nan
    # remove milhares e troca vírgula por ponto quando for padrão BR
    s = s.replace(".", "").replace(",", ".")
    try:
        return float(s)
    except Exception:
        return np.nan



def processar_dados(df_assessores: pd.DataFrame, df_ops: pd.DataFrame, df_dash: pd.DataFrame) -> pd.DataFrame:
    """
    Unifica operações multi-pernas da planilha padrão, cruza com a base de assessores,
    cruza com o Dash (Preço de Abertura e Preço de Mercado) e calcula colunas finais.
    """

    # ==============================
    # Helpers locais
    # ==============================
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
                # decide qual é o decimal olhando o último separador
                if s.rfind(",") > s.rfind("."):
                    # BR: 1.234,56
                    s = s.replace(".", "").replace(",", ".")
                else:
                    # US: 1,234.56
                    s = s.replace(",", "")
            elif has_comma and not has_dot:
                # 149,30
                s = s.replace(",", ".")
            # else: 149.30 ou 14930 -> mantém
            return float(s)
        except Exception:
            return pd.NA

    def norm_date(x):
        dt = pd.to_datetime(x, errors="coerce", dayfirst=True)
        if pd.isna(dt):
            return pd.NaT
        return dt.date()

    # ==============================
    # Validar colunas
    # ==============================
    cols_assessores_obrig = {"Conta", "Nome", "Assessor"}
    cols_ops_obrig = {
        "Data_Operação",
        "Conta_Cliente",
        "Tipo Operação",
        "Tipo Opção",
        "Ativo",
        "Preço Exercício",
        "Quantidade",
        "Fixing",
        "Estrutura",
        "Ref",
        "Bid(+)/Offer(-)",
        "Código do Produto",
    }
    cols_dash_obrig = {"Conta", "Ativo", "Data de Fixing", "Preço de Abertura", "Preço de Mercado"}

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

    # ==============================
    # 1) Unificar operações (planilha padrão)
    # ==============================
    group_cols = [
        "Data_Operação",
        "Conta_Cliente",
        "Ativo",
        "Fixing",
        "Estrutura",
        "Ref",
        "Código do Produto",
    ]

    agg_dict = {
        "Tipo Operação": lambda x: ", ".join(sorted(set(x.dropna()))),
        "Tipo Opção": lambda x: ", ".join(sorted(set(x.dropna()))),
        "Preço Exercício": "min",
        "Quantidade": "max",
        "Bid(+)/Offer(-)": "sum",
    }

    df_grouped = (
        df_ops
        .groupby(group_cols, dropna=False)
        .agg(agg_dict)
        .reset_index()
    )

    # ==============================
    # 2) Cruzar com base de assessores
    # ==============================
    df_merged = df_grouped.merge(
        df_assessores[["Conta", "Nome", "Assessor"]],
        left_on="Conta_Cliente",
        right_on="Conta",
        how="left",
    )

    df_merged = df_merged.rename(columns={
        "Nome": "Nome Cliente",
        "Bid(+)/Offer(-)": "Paga/Recebe",
    })

    # ==============================
    # 3) Normalizar tipos e fazer merge com Dash
    # ==============================
    df_merged["Conta_Cliente"] = pd.to_numeric(df_merged["Conta_Cliente"], errors="coerce")
    df_merged["Ativo"] = df_merged["Ativo"].astype(str).str.strip().str.upper()
    df_merged["Fixing_norm"] = df_merged["Fixing"].apply(norm_date)

    df_merged["Preço Exercício"] = df_merged["Preço Exercício"].apply(br_to_float)
    df_merged["Quantidade"] = df_merged["Quantidade"].apply(br_to_float)
    df_merged["Paga/Recebe"] = df_merged["Paga/Recebe"].apply(br_to_float)

    df_dash["Conta"] = pd.to_numeric(df_dash["Conta"], errors="coerce")
    df_dash["Ativo"] = df_dash["Ativo"].astype(str).str.strip().str.upper()
    df_dash["Fixing_norm"] = df_dash["Data de Fixing"].apply(norm_date)
    df_dash["Preço Abertura"] = df_dash["Preço de Abertura"].apply(br_to_float)
    df_dash["Preço mercado"] = df_dash["Preço de Mercado"].apply(br_to_float)

    # Se o Dash tiver duplicidade por Conta+Ativo+Fixing, pegamos a primeira ocorrência
    dash_keys = ["Conta", "Ativo", "Fixing_norm"]
    df_dash_min = (
        df_dash[dash_keys + ["Preço Abertura", "Preço mercado"]]
        .dropna(subset=dash_keys)
        .drop_duplicates(dash_keys)
    )

    df_merged = df_merged.merge(
        df_dash_min,
        left_on=["Conta_Cliente", "Ativo", "Fixing_norm"],
        right_on=["Conta", "Ativo", "Fixing_norm"],
        how="left",
    )

    # ==============================
    # 4) Colunas finais
    # ==============================
    # Paga/Recebe em texto
    df_merged["Cliente_Paga_Recebe"] = df_merged["Paga/Recebe"].apply(
        lambda x: "PAGA" if pd.notnull(x) and x < 0 else ("RECEBE" if pd.notnull(x) and x > 0 else "NEUTRO")
    )

    # Preço final = Preço mercado + Bid
    df_merged["Preço final"] = df_merged["Preço mercado"] + df_merged["Paga/Recebe"]

    # Lucro saindo (%) = (Preço final / Preço Abertura - 1) * 100
    df_merged["Lucro saindo"] = ((df_merged["Preço final"] / df_merged["Preço Abertura"]) - 1) * 100

    # Bid total = Quantidade * Bid
    df_merged["Bid total"] = df_merged["Quantidade"] * df_merged["Paga/Recebe"]

    # Nocional entrada = Preço Abertura * Quantidade
    df_merged["Nocional entrada"] = df_merged["Preço Abertura"] * df_merged["Quantidade"]

    # Nocional saida = Preço final * Quantidade
    df_merged["Nocional saida"] = df_merged["Preço final"] * df_merged["Quantidade"]

    # Lucro $ Saindo hoje = Nocional saida - Nocional entrada
    df_merged["Lucro $ Saindo hoje"] = df_merged["Nocional saida"] - df_merged["Nocional entrada"]

    # ==============================
    # 5) Seleção/ordem das colunas (como você pediu)
    # ==============================
    colunas_saida = [
        "Data_Operação",
        "Conta_Cliente",
        "Assessor",
        "Nome Cliente",
        "Ativo",
        "Preço Exercício",
        "Quantidade",
        "Fixing",
        "Estrutura",
        "Paga/Recebe",
        "Cliente_Paga_Recebe",
        "Preço Abertura",
        "Preço mercado",
        "Preço final",
        "Lucro saindo",
        "Bid total",
        "Nocional entrada",
        "Nocional saida",
        "Lucro $ Saindo hoje",
    ]

    # Só por segurança (se alguma coluna faltar)
    colunas_saida = [c for c in colunas_saida if c in df_merged.columns]

    return df_merged[colunas_saida]



def gerar_excel_para_download(df: pd.DataFrame) -> BytesIO:
    output = BytesIO()
    with pd.ExcelWriter(output, engine="xlsxwriter") as writer:
        df.to_excel(writer, index=False, sheet_name="Resultado")
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
            df_resultado = processar_dados(df_assessores, df_ops, df_dash)
        except Exception as e:
            st.error(f"Erro ao processar os dados: {e}")
            st.stop()

        st.success("Processamento concluído com sucesso! ✅")
        st.subheader("Prévia do Resultado Unificado")
        st.dataframe(df_resultado.head(100))

        excel_bytes = gerar_excel_para_download(df_resultado)

        st.download_button(
            label="📥 Baixar resultado em Excel",
            data=excel_bytes,
            file_name="resultado_unificado.xlsx",
            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        )

