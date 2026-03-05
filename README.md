# Análise de Venda Cruzada 📊

Este projeto é uma aplicação web interativa desenvolvida com [Streamlit](https://streamlit.io/) e Python, focada na **Análise de Venda Cruzada (Cross-Selling)** entre produtos. O principal objetivo é visualizar e entender o comportamento de compra dos clientes, identificando oportunidades de negócio através da análise de produtos comprados em conjunto ou isoladamente.

## 🚀 Funcionalidades

- **Dashboard Interativo:** Interface amigável e limpa para facilitar a análise de dados e obtenção de insights.
- **Integração com PostgreSQL:** Conexão robusta e direta com banco de dados relacional para carregamento eficiente de dados de vendas (clientes, mercadorias, vendas reais e vendedores).
- **Filtros Dinâmicos e Granulares:** Filtre os resultados da análise com precisão através de diversos parâmetros ajustáveis:
  - 📅 Período de emissão
  - 🛍️ Produtos A e B (foco principal do cruzamento)
  - 🏙️ Cidades
  - 👤 Vendedores
  - 🏢 Atividades / Ramos de Atuação
  - 🏪 Redes de clientes
- **Visualização de Dados Avançada:**
  - 📊 **Diagrama de Venn:** Representação visual direta (via `matplotlib-venn`) da interseção e exclusividade de clientes que compraram o produto A, o produto B ou ambos.
  - 📈 **Gráfico de Barras:** Análise quantitativa interativa da distribuição de clientes, usando `plotly`.
- **Estatísticas e Métricas KPI:** Cálculo automático em tempo real de métricas críticas, incluindo a contagem de clientes por perfil e a **Taxa de Conversão**.
- **Relatórios Detalhados e Exportação:** 
  - Visualização rápida em tabelas agrupando os clientes específicos de cada categoria (Apenas Produto A, Apenas Produto B e Ambos os Produtos).
  - 📥 **Exportação para Excel (.xlsx):** Recurso nativo para baixar o relatório final para manipulação no Excel, já contemplando as principais informações analíticas de cada cliente.

## 🛠️ Tecnologias Utilizadas

- **[Python 3.x](https://www.python.org/):** Linguagem de programação central.
- **[Streamlit](https://streamlit.io/):** Framework para construção do web app, renderização de componentes e roteamento.
- **[Pandas](https://pandas.pydata.org/):** Para manipulação analítica, limpeza e transformação estruturada dos dados via DataFrames.
- **[Psycopg2](https://pypi.org/project/psycopg2/):** Adaptador para comunicação eficiente com o PostgreSQL.
- **[Matplotlib](https://matplotlib.org/) & [Matplotlib-Venn](https://pypi.org/project/matplotlib-venn/):** Criação do Diagrama de Venn e personalização de marcações e texto em gráficos vetoriais.
- **[Plotly](https://plotly.com/python/):** Criação de gráficos interativos com responsividade.
- **[OpenPyXL](https://openpyxl.readthedocs.io/):** Motor de escrita e exportação por trás das rotinas do Pandas para formato Excel.

## ⚙️ Pré-requisitos & Instalação

1. Clone o seu repositório ou faça o download dos arquivos-fonte do projeto.
2. Certifique-se de ter o [Python](https://www.python.org/downloads/) devidamente instalado em sua máquina.
3. Instale as bibliotecas necessárias executando o comando abaixo pelo terminal:
   ```bash
   pip install streamlit pandas psycopg2-binary matplotlib matplotlib-venn plotly openpyxl
   ```

## 🔐 Configuração do Banco de Dados

Esta aplicação utiliza a funcionalidade nativa e segura de gerenciamento de segredos do Streamlit (`st.secrets`) para configurar a conexão com o PostgreSQL, evitando expor credenciais no código-fonte.

Na raiz do seu projeto local, você deve criar um diretório `.streamlit` e dentro dele um arquivo `secrets.toml`, com a seguinte estrutura:

```toml
[postgres]
host = "seu_host_ou_ip"
database = "nome_do_banco"
user = "seo_usuario"
password = "sua_senha_secreta"
port = "5432"
```

*Nota: Em ambiente de produção no Streamlit Community Cloud ou similar, configure os "Secrets" diretamente nas configurações do dashboard (web).*

## ▶️ Como Executar o App Localmente

No seu terminal ou prompt de comando (Ex: PowerShell, CMD), navegue até a pasta que contém o script `VennV2.py` e digite:

```bash
streamlit run VennV2.py
```

O ambiente do Streamlit compilará a aplicação e abrirá automaticamente no navegador padrão (costuma ser a rota `http://localhost:8501`).

## 👨‍💻 Casos de Uso

Este projeto foi desenhado sob medida para apoiar Analistas de Inteligência de Mercado (BI), Gestores Comerciais e Supervisores de Vendas na descoberta de _insights_ cruciais acerca de aderência a combos de produtos, ajudando na implantação de campanhas de descontos e metas promocionais casadas.
