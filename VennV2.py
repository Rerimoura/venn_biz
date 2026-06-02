import streamlit as st
import pandas as pd
import psycopg2
import matplotlib.pyplot as plt
from matplotlib_venn import venn2
import plotly.graph_objects as go
from datetime import datetime, timedelta
from io import BytesIO

st.set_page_config(page_title="Análise de Venda Cruzada", page_icon="📊", layout="wide")

# ── Banco de dados ────────────────────────────────────────────────────────────

@st.cache_resource
def get_connection():
    try:
        cfg = st.secrets["postgres"]
        return psycopg2.connect(
            host=cfg["host"], database=cfg["database"],
            user=cfg["user"], password=cfg["password"], port=cfg["port"]
        )
    except Exception as e:
        st.error(f"Erro ao conectar ao banco de dados: {e}")
        return None


@st.cache_data(ttl=600)
def load_data(_conn, data_inicio, data_fim):
    query = """
        SELECT v.cliente, v.mercadoria, v.data_emissao, v.valor_liq, v.quant,
               v.vendedor, c.cidade, c.raz_social, c.atividade, c.rede,
               m.descricao AS descricao_produto
        FROM vendas v
        JOIN clientes c ON v.cliente = c.cliente
        LEFT JOIN mercadorias m ON v.mercadoria = m.mercadoria
        WHERE v.data_emissao::date BETWEEN %s AND %s
          AND v.vendedor != 2
        ORDER BY v.data_emissao DESC
    """
    try:
        return pd.read_sql_query(query, _conn, params=(data_inicio, data_fim))
    except Exception as e:
        st.error(f"Erro ao carregar dados: {e}")
        return pd.DataFrame()

# ── Análise ───────────────────────────────────────────────────────────────────

def analisar_venda_cruzada(df, produto_a, produto_b):
    clientes_a = set(df[df['mercadoria'] == produto_a]['cliente'].unique())
    clientes_b = set(df[df['mercadoria'] == produto_b]['cliente'].unique())
    ambos      = clientes_a & clientes_b
    return {
        'clientes_a':    clientes_a,
        'clientes_b':    clientes_b,
        'apenas_a':      clientes_a - clientes_b,
        'apenas_b':      clientes_b - clientes_a,
        'ambos':         ambos,
        'total_a':       len(clientes_a),
        'total_b':       len(clientes_b),
        'count_apenas_a': len(clientes_a - clientes_b),
        'count_apenas_b': len(clientes_b - clientes_a),
        'count_ambos':    len(ambos),
    }

# ── Gráficos ──────────────────────────────────────────────────────────────────

def criar_diagrama_venn(resultado, produto_a, produto_b):
    fig, ax = plt.subplots(figsize=(12, 8))

    venn = venn2(
        subsets=(resultado['count_apenas_a'], resultado['count_apenas_b'], resultado['count_ambos']),
        set_labels=(produto_a, produto_b),
        ax=ax
    )

    estilos = {'10': '#4285F4', '01': '#EA4335', '11': '#9C27B0'}
    for pid, cor in estilos.items():
        patch = venn.get_patch_by_id(pid)
        if patch:
            patch.set_facecolor(cor)
            patch.set_alpha(0.65)
            patch.set_edgecolor('white')
            patch.set_linewidth(3)

    for text in venn.set_labels:
        text.set_fontsize(14)
        text.set_fontweight('bold')

    for text in venn.subset_labels:
        if text:
            text.set_fontsize(16)
            text.set_fontweight('bold')
            text.set_color('white')

    taxa = (resultado['count_ambos'] / resultado['total_a'] * 100) if resultado['total_a'] else 0
    total = resultado['count_apenas_a'] + resultado['count_apenas_b'] + resultado['count_ambos']

    stats = (
        f"Estatísticas:\n{'─'*22}\n"
        f"Total Clientes : {total}\n"
        f"Total Produto A: {resultado['total_a']}\n"
        f"Total Produto B: {resultado['total_b']}\n"
        f"Apenas A       : {resultado['count_apenas_a']}\n"
        f"Apenas B       : {resultado['count_apenas_b']}\n"
        f"Ambos          : {resultado['count_ambos']}\n"
        f"Taxa Conversão : {taxa:.1f}%\n{'─'*22}"
    )

    ax.text(0.02, 0.98, stats, transform=ax.transAxes, fontsize=11,
            verticalalignment='top', fontfamily='monospace',
            bbox=dict(boxstyle='round', facecolor='#f5f5f5', alpha=0.95,
                      edgecolor='#333', linewidth=2))

    ax.set_title('Análise de Venda Cruzada', fontsize=18, fontweight='bold', pad=20)
    plt.tight_layout()
    return fig


def criar_grafico_barras(resultado):
    fig = go.Figure(go.Bar(
        x=['Apenas Produto A', 'Ambos', 'Apenas Produto B'],
        y=[resultado['count_apenas_a'], resultado['count_ambos'], resultado['count_apenas_b']],
        marker_color=['#4285F4', '#9C27B0', '#EA4335'],
        text=[resultado['count_apenas_a'], resultado['count_ambos'], resultado['count_apenas_b']],
        textposition='auto',
        textfont=dict(size=14, color='white')
    ))
    fig.update_layout(
        title='Distribuição de Clientes',
        xaxis_title='Categoria', yaxis_title='Quantidade de Clientes',
        height=400, showlegend=False
    )
    return fig

# ── Tabelas detalhadas ────────────────────────────────────────────────────────

AGG_COLS = {
    'raz_social': 'first', 'cidade': 'first', 'atividade': 'first',
    'rede': 'first', 'vendedor': 'last', 'descricao_produto': 'first',
    'data_emissao': 'max', 'quant': 'sum'
}
RENAME_BASE = ['Cliente', 'Razão Social', 'Cidade', 'Atividade', 'Rede',
               'Último Vendedor', 'Produto', 'Última Compra', 'Qtd Total']


def tabela_clientes(df, clientes, mercadoria):
    return (
        df[df['cliente'].isin(clientes) & (df['mercadoria'] == mercadoria)]
        .groupby('cliente')
        .agg(AGG_COLS)
        .reset_index()
        .rename(columns=dict(zip(
            ['cliente'] + list(AGG_COLS.keys()), RENAME_BASE
        )))
        .sort_values('Última Compra', ascending=False)
    )


def tabela_ambos(df, clientes, produto_a, produto_b):
    desc = lambda p: (
        df[df['cliente'].isin(clientes) & (df['mercadoria'] == p)]
        .groupby('cliente')['descricao_produto'].first()
    )
    agg_cols_sem_desc = {k: v for k, v in AGG_COLS.items() if k != 'descricao_produto'}

    base = (
        df[df['cliente'].isin(clientes)]
        .groupby('cliente')
        .agg(agg_cols_sem_desc)
        .reset_index()
    )
    base['produtos'] = desc(produto_a).reindex(base['cliente'].values).values \
                     + ' | ' \
                     + desc(produto_b).reindex(base['cliente'].values).values

    rename = ['Cliente', 'Razão Social', 'Cidade', 'Atividade', 'Rede',
              'Último Vendedor', 'Última Compra', 'Qtd Total', 'Produtos']
    base.columns = rename
    return base.sort_values('Última Compra', ascending=False)


def excel_download(df, sheet_name, filename):
    buf = BytesIO()
    with pd.ExcelWriter(buf, engine='openpyxl') as w:
        df.to_excel(w, index=False, sheet_name=sheet_name)
    buf.seek(0)
    st.download_button("📥 Download Excel", data=buf, file_name=filename,
                       mime='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet')

# ── App principal ─────────────────────────────────────────────────────────────

def main():
    st.title("📊 Análise de Venda Cruzada")
    st.markdown("---")

    conn = get_connection()
    if conn is None:
        st.error("⚠️ Não foi possível conectar ao banco de dados.")
        return

    # Sidebar — filtros de período
    st.sidebar.header("🔍 Filtros")
    st.sidebar.subheader("📅 Período")

    hoje = datetime.now().date()
    col1, col2 = st.sidebar.columns(2)
    data_inicio = col1.date_input("Início", value=hoje - timedelta(days=90), max_value=hoje)
    data_fim    = col2.date_input("Fim",    value=hoje, min_value=data_inicio)

    with st.spinner("Carregando dados..."):
        df = load_data(conn, data_inicio, data_fim)

    if df.empty:
        st.warning("⚠️ Nenhum dado encontrado para o período selecionado.")
        return

    st.sidebar.success(f"✅ {len(df):,} registros carregados")

    # Sidebar — seleção de produtos
    st.sidebar.subheader("🛍️ Produtos")
    produtos = sorted(df['mercadoria'].unique())
    produto_a = st.sidebar.selectbox("Produto A", produtos, index=0)
    produto_b = st.sidebar.selectbox("Produto B", produtos, index=min(1, len(produtos) - 1))

    # Sidebar — filtros adicionais
    st.sidebar.subheader("🎯 Filtros Adicionais")

    def multiselect_filtro(label, coluna, placeholder='Todas'):
        opcoes = [placeholder] + sorted(df[coluna].dropna().unique().tolist())
        return st.sidebar.multiselect(label, opcoes, default=[placeholder])

    cidade_sel    = multiselect_filtro("Cidade",    'cidade',    'Todas')
    vendedor_sel  = multiselect_filtro("Vendedor",  'vendedor',  'Todos')
    atividade_sel = multiselect_filtro("Atividade", 'atividade', 'Todas')
    rede_sel      = multiselect_filtro("Rede",      'rede',      'Todas')

    # Aplicar filtros
    df_f = df.copy()
    filtros = [
        (cidade_sel,    'cidade',    'Todas'),
        (vendedor_sel,  'vendedor',  'Todos'),
        (atividade_sel, 'atividade', 'Todas'),
        (rede_sel,      'rede',      'Todas'),
    ]
    for sel, col, placeholder in filtros:
        if placeholder not in sel and sel:
            df_f = df_f[df_f[col].isin(sel)]

    if produto_a == produto_b:
        st.warning("⚠️ Selecione produtos diferentes para Produto A e Produto B.")
        return

    resultado = analisar_venda_cruzada(df_f, produto_a, produto_b)

    # Métricas
    st.subheader("📈 Resumo")
    taxa = (resultado['count_ambos'] / resultado['total_a'] * 100) if resultado['total_a'] else 0

    c1, c2, c3, c4 = st.columns(4)
    c1.metric(f"🔵 {str(produto_a)[:30]}", resultado['total_a'])
    c2.metric(f"🔴 {str(produto_b)[:30]}", resultado['total_b'])
    c3.metric("🟣 Compraram Ambos",        resultado['count_ambos'])
    c4.metric("📊 Taxa de Conversão",      f"{taxa:.1f}%")

    st.markdown("---")

    # Tabs
    tab1, tab2, tab3 = st.tabs(["📊 Diagrama de Venn", "📈 Gráfico de Barras", "📋 Tabelas Detalhadas"])

    with tab1:
        st.subheader("Diagrama de Venn")
        st.pyplot(criar_diagrama_venn(resultado, produto_a, produto_b))

    with tab2:
        st.subheader("Gráfico de Barras")
        st.plotly_chart(criar_grafico_barras(resultado), use_container_width=True)

    with tab3:
        data_str = datetime.now().strftime("%Y%m%d")

        secoes = [
            ("🔵 Clientes que compraram apenas Produto A", resultado['apenas_a'], produto_a,
             lambda c, p: tabela_clientes(df_f, c, p), f"clientes_apenas_A_{data_str}.xlsx", "Apenas Produto A"),
            ("🔴 Clientes que compraram apenas Produto B", resultado['apenas_b'], produto_b,
             lambda c, p: tabela_clientes(df_f, c, p), f"clientes_apenas_B_{data_str}.xlsx", "Apenas Produto B"),
        ]

        for titulo, clientes, prod, fn_tabela, filename, sheet in secoes:
            st.markdown(f"### {titulo}")
            if clientes:
                tbl = fn_tabela(clientes, prod)
                st.dataframe(tbl, use_container_width=True)
                excel_download(tbl, sheet, filename)
            else:
                st.info("Nenhum cliente encontrado nesta categoria.")
            st.markdown("---")

        st.markdown("### 🟣 Clientes que compraram AMBOS os produtos")
        if resultado['ambos']:
            tbl = tabela_ambos(df_f, resultado['ambos'], produto_a, produto_b)
            st.dataframe(tbl, use_container_width=True)
            excel_download(tbl, "Ambos Produtos", f"clientes_ambos_{data_str}.xlsx")
        else:
            st.info("Nenhum cliente encontrado nesta categoria.")

    st.markdown("---")
    st.caption(f"📅 Período: {data_inicio.strftime('%d/%m/%Y')} a {data_fim.strftime('%d/%m/%Y')}")
    st.caption(f"📊 Total de registros: {len(df_f):,}")


if __name__ == "__main__":
    main()