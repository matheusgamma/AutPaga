import streamlit as st
import pandas as pd
from io import BytesIO

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


def processar_dados(df_assessores: pd.DataFrame, df_ops: pd.DataFrame) -> pd.DataFrame:
    """
    Unifica operações multi-pernas da planilha padrão, cruza com a base de assessores
    e calcula Ref+Bid (R$) e % Saindo agora.
    """

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

    faltando_ass = cols_assessores_obrig - set(df_assessores.columns)
    faltando_ops = cols_ops_obrig - set(df_ops.columns)

    if faltando_ass:
        raise ValueError(f"Faltam colunas na base de assessores: {faltando_ass}")
    if faltando_ops:
        raise ValueError(f"Faltam colunas na planilha padrão: {faltando_ops}")

    df_ops = df_ops.copy()
    df_assessores = df_assessores.copy()

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

    # Renomear colunas
    df_merged = df_merged.rename(columns={
        "Nome": "Nome Cliente",
        "Bid(+)/Offer(-)": "Paga/Recebe",
        "Código do Produto": "Cod Produto",
    })

    # =========================
    # GARANTIR TIPOS NUMÉRICOS
    # =========================
    df_merged["Ref"] = pd.to_numeric(df_merged["Ref"], errors="coerce")
    df_merged["Paga/Recebe"] = pd.to_numeric(df_merged["Paga/Recebe"], errors="coerce")
    df_merged["Quantidade"] = pd.to_numeric(df_merged["Quantidade"], errors="coerce")
    df_merged["Preço Exercício"] = pd.to_numeric(df_merged["Preço Exercício"], errors="coerce")

    # =========================
    # Ref+Bid (valor financeiro total)
    # (Ref + Bid) * Quantidade
    # =========================
    df_merged["Ref+Bid_valor"] = (df_merged["Ref"] + df_merged["Paga/Recebe"]) * df_merged["Quantidade"]

    # Formatar Ref+Bid em R$
    df_merged["Ref+Bid"] = df_merged["Ref+Bid_valor"].apply(
        lambda x: f"R$ {x:,.2f}".replace(",", "X").replace(".", ",").replace("X", ".")
        if pd.notnull(x) else ""
    )

    # =========================
    # % Saindo agora
    # ((Ref + Bid) / Preço Exercício - 1) * 100
    # =========================
    base_preco = (df_merged["Ref"] + df_merged["Paga/Recebe"])
    df_merged["% Saindo agora"] = ((base_preco / df_merged["Preço Exercício"]) - 1) * 100

    df_merged["% Saindo agora"] = df_merged["% Saindo agora"].apply(
        lambda x: f"{x:.2f}%".replace(".", ",") if pd.notnull(x) else ""
    )

    # Classificação PAGA / RECEBE / NEUTRO com base em Paga/Recebe (soma dos bids)
    df_merged["Cliente_Paga_Recebe"] = df_merged["Paga/Recebe"].apply(
        lambda x: "PAGA" if x < 0 else ("RECEBE" if x > 0 else "NEUTRO")
    )

    # Colunas de saída
    colunas_saida = [
        "Data_Operação",
        "Conta_Cliente",
        "Assessor",
        "Nome Cliente",
        "Ativo",
        "Preço Exercício",
        "Quantidade",
        "Barreira Knock In",
        "Barreira Knock Out",
        "Direção da Barreira",
        "Fixing",
        "KnockInAtingido",
        "Estrutura",
        "Ref",
        "Paga/Recebe",
        "Cliente_Paga_Recebe",
        "Ref+Bid",
        "% Saindo agora",
        "Cod Produto",
    ]

    colunas_saida = [c for c in colunas_saida if c in df_merged.columns]

    df_final = df_merged[colunas_saida]

    return df_final




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

col1, col2 = st.columns(2)

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

if st.button("🚀 Processar"):
    if not file_assessores or not file_ops:
        st.warning("Envie as **duas** planilhas antes de processar.")
    else:
        df_assessores = carregar_arquivo(file_assessores)
        df_ops = carregar_arquivo(file_ops)

        if df_assessores is None or df_ops is None:
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
