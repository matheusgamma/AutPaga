import streamlit as st
import pandas as pd
from io import BytesIO
import numpy as np
import yfinance as yf


st.set_page_config(page_title="Unificação de Operações", layout="wide")


# ==============================
# Funções auxiliares
# ==============================

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
    cruza com o Dash (Preço Abertura / Mercado) e calcula resultado saindo hoje.
    """

    # ==============================
    # Helpers locais (pra não "fugir" do seu arquivo)
    # ==============================
    def br_to_float(x):
        if pd.isna(x):
            return pd.NA
        if isinstance(x, (int, float)):
            return float(x)
        s = str(x).strip()
        if s == "":
            return pd.NA
        s = s.replace(".", "").replace(",", ".")
        try:
            return float(s)
        except Exception:
            return pd.NA

    def norm_date(x):
        # normaliza para date (sem horário)
        dt = pd.to_datetime(x, errors="coerce", dayfirst=True)
        if pd.isna(dt):
            return pd.NaT
        return dt.date()

    # --- Garantir que as colunas necessárias existem ---
    cols_assessores_obrig = {"Conta", "Nome", "Assessor"}
    cols_ops_obrig = {
        "Data_Operação",
        "Conta_Cliente",
        "Tipo Operação",
        "Tipo Opção",
        "Ativo",
        "Preço Exercício",
        "Quantidade",
        "Barreira Knock In",
        "Barreira Knock Out",
        "Direção da Barreira",
        "Rebate",
        "Fixing",
        "KnockInAtingido",
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
        raise ValueError(f"Faltam colunas no Dash (preço abertura): {faltando_dash}")

    df_ops = df_ops.copy()
    df_assessores = df_assessores.copy()
    df_dash = df_dash.copy()

    # --- Agrupamento para unificar operações ---
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

        "Barreira Knock In": primeira_nao_nula,
        "Barreira Knock Out": primeira_nao_nula,
        "Direção da Barreira": primeira_nao_nula,
        "Rebate": primeira_nao_nula,
        "KnockInAtingido": primeira_nao_nula,

        "Bid(+)/Offer(-)": "sum",
    }

    df_grouped = (
        df_ops
        .groupby(group_cols, dropna=False)
        .agg(agg_dict)
        .reset_index()
    )

    # --- Cruzar com base de assessores ---
    df_merged = df_grouped.merge(
        df_assessores[["Conta", "Nome", "Assessor"]],
        left_on="Conta_Cliente",
        right_on="Conta",
        how="left",
    )

    # Renomear / montar colunas finais
    df_merged = df_merged.rename(columns={
        "Nome": "Nome Cliente",
        "Bid(+)/Offer(-)": "Paga/Recebe",
        "Código do Produto": "Cod Produto",
    })

    # =========================
    # Normalização de tipos p/ cálculo e merge com Dash
    # =========================
    # Ops
    df_merged["Conta_Cliente"] = pd.to_numeric(df_merged["Conta_Cliente"], errors="coerce")
    df_merged["Ativo"] = df_merged["Ativo"].astype(str).str.strip().str.upper()
    df_merged["Fixing_norm"] = df_merged["Fixing"].apply(norm_date)

    df_merged["Ref"] = df_merged["Ref"].apply(br_to_float)
    df_merged["Paga/Recebe"] = df_merged["Paga/Recebe"].apply(br_to_float)
    df_merged["Quantidade"] = df_merged["Quantidade"].apply(br_to_float)
    df_merged["Preço Exercício"] = df_merged["Preço Exercício"].apply(br_to_float)

    # Dash
    df_dash["Conta"] = pd.to_numeric(df_dash["Conta"], errors="coerce")
    df_dash["Ativo"] = df_dash["Ativo"].astype(str).str.strip().str.upper()
    df_dash["Fixing_norm"] = df_dash["Data de Fixing"].apply(norm_date)
    df_dash["Preço Abertura"] = df_dash["Preço de Abertura"].apply(br_to_float)
    df_dash["Preço Mercado"] = df_dash["Preço de Mercado"].apply(br_to_float)

    # --- Merge com Dash pelo identificador da operação: Conta + Ativo + Data de Fixing ---
    dash_keys = ["Conta", "Ativo", "Fixing_norm"]
    df_dash_min = (
        df_dash[dash_keys + ["Preço Abertura", "Preço Mercado"]]
        .dropna(subset=["Conta", "Ativo", "Fixing_norm"])
        .drop_duplicates(dash_keys)
    )

    df_merged = df_merged.merge(
        df_dash_min,
        left_on=["Conta_Cliente", "Ativo", "Fixing_norm"],
        right_on=["Conta", "Ativo", "Fixing_norm"],
        how="left",
    )

    # =========================
    # Cálculos “saindo hoje”
    # =========================
    # Resultado Prévio = (Preço Mercado - Preço Abertura) * Quantidade
    df_merged["Resultado Prévio"] = (df_merged["Preço Mercado"] - df_merged["Preço Abertura"]) * df_merged["Quantidade"]

    # Bid Total = Paga/Recebe * Quantidade
    df_merged["Bid Total"] = df_merged["Paga/Recebe"] * df_merged["Quantidade"]

    # Resultado Saindo Hoje = Resultado Prévio + Bid Total
    df_merged["Resultado Saindo Hoje"] = df_merged["Resultado Prévio"] + df_merged["Bid Total"]

    # % Saindo Hoje
    # Base = Quantidade * Preço Abertura
    df_merged["Base (Abertura)"] = df_merged["Quantidade"] * df_merged["Preço Abertura"]
    df_merged["% Saindo Hoje"] = ((df_merged["Base (Abertura)"] + df_merged["Resultado Saindo Hoje"]) / df_merged["Base (Abertura)"] - 1) * 100

    # Classificação textual: PAGA / RECEBE / NEUTRO
    df_merged["Cliente_Paga_Recebe"] = df_merged["Paga/Recebe"].apply(
        lambda x: "PAGA" if pd.notnull(x) and x < 0 else ("RECEBE" if pd.notnull(x) and x > 0 else "NEUTRO")
    )

    # =========================
    # Formatação (texto) p/ visual/excel
    # =========================
    def fmt_rs(x):
        return f"R$ {x:,.2f}".replace(",", "X").replace(".", ",").replace("X", ".") if pd.notnull(x) else ""

    def fmt_pct(x):
        return f"{x:.2f}%".replace(".", ",") if pd.notnull(x) else ""

    df_merged["Resultado Prévio"] = df_merged["Resultado Prévio"].apply(fmt_rs)
    df_merged["Bid Total"] = df_merged["Bid Total"].apply(fmt_rs)
    df_merged["Resultado Saindo Hoje"] = df_merged["Resultado Saindo Hoje"].apply(fmt_rs)
    df_merged["% Saindo Hoje"] = df_merged["% Saindo Hoje"].apply(fmt_pct)

    # Ordenar / selecionar colunas finais
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
        "Ref",
        "Paga/Recebe",
        "Cliente_Paga_Recebe",
        "Preço Abertura",
        "Preço Mercado",
        "Resultado Prévio",
        "Bid Total",
        "Resultado Saindo Hoje",
        "% Saindo Hoje",
        "Cod Produto",
    ]

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
    file_assessores = st.file_uploader("📂 Base de Assessores", type=["xlsx", "xls", "csv"], key="file_assessores")

with col2:
    file_ops = st.file_uploader("📂 Planilha Padrão de Operações", type=["xlsx", "xls", "csv"], key="file_ops")

with col3:
    file_abertura = st.file_uploader("📂 Dash Preço de Abertura", type=["xlsx", "xls", "csv"], key="file_dash")


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



        try:
            df_resultado = processar_dados(df_assessores, df_ops)
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
