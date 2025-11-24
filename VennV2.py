import streamlit as st
import pandas as pd
import psycopg2
from psycopg2 import sql
import matplotlib.pyplot as plt
from matplotlib_venn import venn2
import plotly.express as px
import plotly.graph_objects as go
from datetime import datetime, timedelta

# Configuração da página
st.set_page_config(
    page_title="Análise de Venda Cruzada",
    page_icon="📊",
    layout="wide"
)

# Função para conectar ao PostgreSQL
@st.cache_resource
def get_connection():
    """Estabelece conexão com PostgreSQL"""
    try:
        # Usando st.secrets para credenciais
        db_config = st.secrets["postgres"]
        
        conn = psycopg2.connect(
            host=db_config["host"],
            database=db_config["database"],
            user=db_config["user"],
            password=db_config["password"],
            port=db_config["port"]
        )
        return conn
    except Exception as e:
        st.error(f"Erro ao conectar ao banco de dados: {e}")
        return None

# Função para carregar dados
@st.cache_data(ttl=600)  # Cache por 10 minutos
def load_data(_conn, data_inicio, data_fim):
    """Carrega dados de vendas do PostgreSQL"""
    query = """
    SELECT 
        v.cliente,
        v.mercadoria,
        v.data_emissao,
        v.valor_liq,
        v.quant,
        v.vendedor,
        c.cidade,
        c.raz_social
    FROM vendas v
    inner join clientes c
        on v.cliente = c.cliente
    WHERE data_emissao BETWEEN %s AND %s
    ORDER BY data_emissao desc;
    """
    
    try:
        df = pd.read_sql_query(query, _conn, params=(data_inicio, data_fim))
        return df
    except Exception as e:
        st.error(f"Erro ao carregar dados: {e}")
        return pd.DataFrame()

# Função para obter lista de produtos
@st.cache_data(ttl=600)
def get_produtos(_conn):
    """Retorna lista única de produtos"""
    query = """
    SELECT DISTINCT mercadoria 
    FROM vendas
    ORDER BY mercadoria
    """
    try:
        df = pd.read_sql_query(query, _conn)
        return df['mercadoria'].tolist()
    except Exception as e:
        st.error(f"Erro ao carregar produtos: {e}")
        return []

# Função para obter lista de cidades
@st.cache_data(ttl=600)
def get_cidades(_conn):
    """Retorna lista única de cidades"""
    query = """
    SELECT DISTINCT c.cidade 
    FROM vendas v
    inner join clientes c
        on v.cliente = c.cliente 
    WHERE cidade IS NOT null and UF = 'MG'
    ORDER BY cidade
    """
    try:
        df = pd.read_sql_query(query, _conn)
        return df['cidade'].tolist()
    except Exception as e:
        return []

# Função para obter lista de vendedores
@st.cache_data(ttl=600)
def get_vendedores(_conn):
    """Retorna lista única de vendedores"""
    query = """
    SELECT DISTINCT vendedor 
    FROM vendas
    WHERE vendedor IS NOT NULL
    ORDER BY vendedor
    """
    try:
        df = pd.read_sql_query(query, _conn)
        return df['vendedor'].tolist()
    except Exception as e:
        return []

# Função para análise de venda  cruzada
def analisar_venda_cruzada(df, produto_a, produto_b):
    """Analisa venda cruzada entre dois produtos"""
    
    # Clientes que compraram produto A
    clientes_a = set(df[df['mercadoria'] == produto_a]['cliente'].unique())
    
    # Clientes que compraram produto B
    clientes_b = set(df[df['mercadoria'] == produto_b]['cliente'].unique())
    
    # Clientes exclusivos de A
    apenas_a = clientes_a - clientes_b
    
    # Clientes exclusivos de B
    apenas_b = clientes_b - clientes_a
    
    # Clientes que compraram ambos
    ambos = clientes_a & clientes_b
    
    return {
        'clientes_a': clientes_a,
        'clientes_b': clientes_b,
        'apenas_a': apenas_a,
        'apenas_b': apenas_b,
        'ambos': ambos,
        'total_a': len(clientes_a),
        'total_b': len(clientes_b),
        'count_apenas_a': len(apenas_a),
        'count_apenas_b': len(apenas_b),
        'count_ambos': len(ambos)
    }

# Função para criar diagrama de Venn
def criar_diagrama_venn(resultado, produto_a, produto_b):
    """Cria diagrama de Venn com matplotlib"""
    
    fig, ax = plt.subplots(figsize=(12, 8))
    
    # Criar diagrama
    venn = venn2(
        subsets=(
            resultado['count_apenas_a'],
            resultado['count_apenas_b'],
            resultado['count_ambos']
        ),
        set_labels=(produto_a, produto_b),
        ax=ax
    )
    
    # Personalizar cores
    if venn.get_patch_by_id('10'):
        venn.get_patch_by_id('10').set_color('#4285F4')
        venn.get_patch_by_id('10').set_alpha(0.6)
        venn.get_patch_by_id('10').set_edgecolor('white')
        venn.get_patch_by_id('10').set_linewidth(3)
    
    if venn.get_patch_by_id('01'):
        venn.get_patch_by_id('01').set_color('#EA4335')
        venn.get_patch_by_id('01').set_alpha(0.6)
        venn.get_patch_by_id('01').set_edgecolor('white')
        venn.get_patch_by_id('01').set_linewidth(3)
    
    if venn.get_patch_by_id('11'):
        venn.get_patch_by_id('11').set_color('#9C27B0')
        venn.get_patch_by_id('11').set_alpha(0.7)
        venn.get_patch_by_id('11').set_edgecolor('white')
        venn.get_patch_by_id('11').set_linewidth(3)
    
    # Customizar textos
    for text in venn.set_labels:
        text.set_fontsize(14)
        text.set_fontweight('bold')
        text.set_color('#333')
    
    for text in venn.subset_labels:
        if text:
            text.set_fontsize(16)
            text.set_fontweight('bold')
            text.set_color('white')
    
    # Título
    plt.title('Análise de Venda Cruzada', fontsize=18, fontweight='bold', pad=20)
    
    # Calcular taxa de conversão
    taxa_conversao = (resultado['count_ambos'] / resultado['total_a'] * 100) if resultado['total_a'] > 0 else 0
    
    # Box com estatísticas
    total_geral = resultado['count_apenas_a'] + resultado['count_apenas_b'] + resultado['count_ambos']
    
    stats_text = f'''Estatísticas:
──────────────────────
Total Clientes: {total_geral}
Total Produto A: {resultado['total_a']}
Total Produto B: {resultado['total_b']}
Apenas A: {resultado['count_apenas_a']}
Apenas B: {resultado['count_apenas_b']}
Ambos: {resultado['count_ambos']}
Taxa Conversão: {taxa_conversao:.1f}%
──────────────────────'''
    
    plt.text(0.02, 0.98, stats_text, 
             transform=ax.transAxes,
             fontsize=11,
             verticalalignment='top',
             fontfamily='monospace',
             bbox=dict(boxstyle='round', 
                       facecolor='#f5f5f5', 
                       alpha=0.95,
                       edgecolor='#333',
                       linewidth=2))
    
    plt.tight_layout()
    return fig

# Função para criar gráfico de barras
def criar_grafico_barras(resultado):
    """Cria gráfico de barras com Plotly"""
    
    categorias = ['Apenas Produto A', 'Ambos', 'Apenas Produto B']
    valores = [
        resultado['count_apenas_a'],
        resultado['count_ambos'],
        resultado['count_apenas_b']
    ]
    cores = ['#4285F4', '#9C27B0', '#EA4335']
    
    fig = go.Figure(data=[
        go.Bar(
            x=categorias,
            y=valores,
            marker_color=cores,
            text=valores,
            textposition='auto',
            textfont=dict(size=14, color='white')
        )
    ])
    
    fig.update_layout(
        title='Distribuição de Clientes',
        xaxis_title='Categoria',
        yaxis_title='Quantidade de Clientes',
        height=400,
        showlegend=False
    )
    
    return fig

# Interface principal
def main():
    st.title("📊 Análise de Venda Cruzada")
    st.markdown("---")
    
    # Conectar ao banco
    conn = get_connection()
    
    if conn is None:
        st.error("⚠️ Não foi possível conectar ao banco de dados. Verifique as configurações.")
        st.info("💡 Edite as credenciais do banco na função `get_connection()` no código.")
        return
    
    # Sidebar - Filtros
    st.sidebar.header("🔍 Filtros")
    
    # Filtro de data
    st.sidebar.subheader("📅 Período")
    
    col1, col2 = st.sidebar.columns(2)
    
    # Data padrão: últimos 90 dias
    data_fim_default = datetime.now().date()
    data_inicio_default = data_fim_default - timedelta(days=90)
    
    with col1:
        data_inicio = st.date_input(
            "Data Início",
            value=data_inicio_default,
            max_value=data_fim_default
        )
    
    with col2:
        data_fim = st.date_input(
            "Data Fim",
            value=data_fim_default,
            min_value=data_inicio
        )
    
    # Carregar dados
    with st.spinner("Carregando dados..."):
        df = load_data(conn, data_inicio, data_fim)
    
    if df.empty:
        st.warning("⚠️ Nenhum dado encontrado para o período selecionado.")
        return
    
    st.sidebar.success(f"✅ {len(df)} registros carregados")
    
    # Filtro de produtos
    st.sidebar.subheader("🛍️ Produtos")
    
    produtos_disponiveis = sorted(df['mercadoria'].unique())
    
    produto_a = st.sidebar.selectbox(
        "Produto A",
        options=produtos_disponiveis,
        index=0 if len(produtos_disponiveis) > 0 else None
    )
    
    produto_b = st.sidebar.selectbox(
        "Produto B",
        options=produtos_disponiveis,
        index=1 if len(produtos_disponiveis) > 1 else 0
    )
    
    # Filtros adicionais
    st.sidebar.subheader("🎯 Filtros Adicionais")
    
    # Filtro de cidade
    cidades_disponiveis = ['Todas'] + sorted(df['cidade'].dropna().unique().tolist())
    cidade_selecionada = st.sidebar.multiselect(
        "Cidade",
        options=cidades_disponiveis,
        default=['Todas']
    )
    
    # Filtro de vendedor
    vendedores_disponiveis = ['Todos'] + sorted(df['vendedor'].dropna().unique().tolist())
    vendedor_selecionado = st.sidebar.multiselect(
        "Vendedor",
        options=vendedores_disponiveis,
        default=['Todos']
    )
    
    # Aplicar filtros
    df_filtrado = df.copy()
    
    if 'Todas' not in cidade_selecionada and len(cidade_selecionada) > 0:
        df_filtrado = df_filtrado[df_filtrado['cidade'].isin(cidade_selecionada)]
    
    if 'Todos' not in vendedor_selecionado and len(vendedor_selecionado) > 0:
        df_filtrado = df_filtrado[df_filtrado['vendedor'].isin(vendedor_selecionado)]
    
    # Validação
    if produto_a == produto_b:
        st.warning("⚠️ Por favor, selecione produtos diferentes para Produto A e Produto B.")
        return
    
    # Análise
    resultado = analisar_venda_cruzada(df_filtrado, produto_a, produto_b)
    
    # Métricas principais
    st.subheader("📈 Resumo")
    
    col1, col2, col3, col4 = st.columns(4)
    
    with col1:
        st.metric(
            label=f"🔵 Total {str(produto_a)[:30]}...",
            value=resultado['total_a'],
            delta=None
        )
    
    with col2:
        st.metric(
            label=f"🔴 Total {str(produto_b)[:30]}...",
            value=resultado['total_b'],
            delta=None
        )
    
    with col3:
        st.metric(
            label="🟣 Compraram Ambos",
            value=resultado['count_ambos'],
            delta=None
        )
    
    with col4:
        taxa_conversao = (resultado['count_ambos'] / resultado['total_a'] * 100) if resultado['total_a'] > 0 else 0
        st.metric(
            label="📊 Taxa de Conversão",
            value=f"{taxa_conversao:.1f}%",
            delta=None
        )
    
    st.markdown("---")
    
    # Visualizações
    tab1, tab2, tab3 = st.tabs(["📊 Diagrama de Venn", "📈 Gráfico de Barras", "📋 Tabelas Detalhadas"])
    
    with tab1:
        st.subheader("Diagrama de Venn")
        fig_venn = criar_diagrama_venn(resultado, produto_a, produto_b)
        st.pyplot(fig_venn)
    
    with tab2:
        st.subheader("Gráfico de Barras")
        fig_barras = criar_grafico_barras(resultado)
        st.plotly_chart(fig_barras, use_container_width=True)
    
    with tab3:
        st.subheader("Detalhamento de Clientes")
        
        # Tabela: Compraram A e não B
        st.markdown("### 🔵 Clientes que compraram apenas Produto A")
        if len(resultado['apenas_a']) > 0:
            df_apenas_a = df_filtrado[
                df_filtrado['cliente'].isin(resultado['apenas_a']) &
                (df_filtrado['mercadoria'] == produto_a)
            ].groupby('cliente').agg({
                'raz_social': 'first',
                'data_emissao': 'max',
                'quant': 'sum'
            }).reset_index()
            
            df_apenas_a.columns = ['Cliente', 'Razão Social', 'Última Compra', 'Qtd Total']
            df_apenas_a = df_apenas_a.sort_values('Última Compra', ascending=False)
            
            st.dataframe(df_apenas_a, use_container_width=True)
            
            # Botão de download
            csv = df_apenas_a.to_csv(index=False).encode('utf-8')
            st.download_button(
                label="⬇️ Download CSV",
                data=csv,
                file_name=f'clientes_apenas_A_{datetime.now().strftime("%Y%m%d")}.csv',
                mime='text/csv'
            )
        else:
            st.info("Nenhum cliente encontrado nesta categoria.")
        
        st.markdown("---")
        
        # Tabela: Compraram B e não A
        st.markdown("### 🔴 Clientes que compraram apenas Produto B")
        if len(resultado['apenas_b']) > 0:
            df_apenas_b = df_filtrado[
                df_filtrado['cliente'].isin(resultado['apenas_b']) &
                (df_filtrado['mercadoria'] == produto_b)
            ].groupby('cliente').agg({
                'raz_social': 'first',
                'data_emissao': 'max',
                'quant': 'sum'
            }).reset_index()
            
            df_apenas_b.columns = ['Cliente', 'Razão Social', 'Última Compra', 'Qtd Total']
            df_apenas_b = df_apenas_b.sort_values('Última Compra', ascending=False)
            
            st.dataframe(df_apenas_b, use_container_width=True)
            
            csv = df_apenas_b.to_csv(index=False).encode('utf-8')
            st.download_button(
                label="⬇️ Download CSV",
                data=csv,
                file_name=f'clientes_apenas_B_{datetime.now().strftime("%Y%m%d")}.csv',
                mime='text/csv'
            )
        else:
            st.info("Nenhum cliente encontrado nesta categoria.")
        
        st.markdown("---")
        
        # Tabela: Compraram Ambos
        st.markdown("### 🟣 Clientes que compraram AMBOS os produtos")
        if len(resultado['ambos']) > 0:
            df_ambos = df_filtrado[
                df_filtrado['cliente'].isin(resultado['ambos'])
            ].groupby('cliente').agg({
                'raz_social': 'first',
                'data_emissao': 'max',
                'quant': 'sum'
            }).reset_index()
            
            df_ambos.columns = ['Cliente', 'Razão Social', 'Última Compra', 'Qtd Total']
            df_ambos = df_ambos.sort_values('Última Compra', ascending=False)
            
            st.dataframe(df_ambos, use_container_width=True)
            
            csv = df_ambos.to_csv(index=False).encode('utf-8')
            st.download_button(
                label="⬇️ Download CSV",
                data=csv,
                file_name=f'clientes_ambos_{datetime.now().strftime("%Y%m%d")}.csv',
                mime='text/csv'
            )
        else:
            st.info("Nenhum cliente encontrado nesta categoria.")
    
    # Rodapé
    st.markdown("---")
    st.caption(f"📅 Período analisado: {data_inicio.strftime('%d/%m/%Y')} a {data_fim.strftime('%d/%m/%Y')}")
    st.caption(f"📊 Total de registros: {len(df_filtrado):,}")

if __name__ == "__main__":
    main()