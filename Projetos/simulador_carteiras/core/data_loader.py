"""
Módulo de carregamento de dados financeiros.

Centraliza a leitura de preços de múltiplas fontes:
- ANBIMA (parquet do market_data) → índices da família IMA
- BCB API → SELIC, CDI, USD/BRL
- yfinance → ações e ETFs

Conversão automática para BRL: tickers do yfinance que NÃO terminam em .SA
são tratados como USD e convertidos usando a cotação do dólar.

Uso típico:
    from core.data_loader import carregar_precos
    
    df = carregar_precos(
        tickers=['SELIC', 'IMA-B', 'DIVO11.SA', 'GLD'],
        data_inicio=date(2014, 1, 1),
        data_fim=date.today()
    )
"""

# ============================================================
# BLOCO 1 - IMPORTS NECESSÁRIOS
# ============================================================

# Imports bibliotecas nativas
from datetime import date, datetime
from pathlib import Path

# Imports de bibliotecas externas
import pandas as pd
import yfinance as yf
import requests

# Imports de módulos do próprio projeto
from core.config import(
    ANBIMA_PARQUET,
    CACHE_DIR,
    BCB_CODIGOS,
)

# ============================================================
# BLOCO 2 - CLASSIFICAÇÃO DE TICKERS POR FONTE
# ============================================================

def _classificar_ticker(ticker: str) -> str:
    """
    Identifica a fonte de dados de um ticker.
    
    Args:
        ticker: Identificador do ativo (ex: 'IMA-B', 'DIVO11.SA', 'SELIC')
    
    Returns:
        'bcb', 'anbima' ou 'yfinance'
    
    Examples:
        >>> _classificar_ticker('SELIC')
        'bcb'
        >>> _classificar_ticker('IMA-B')
        'anbima'
        >>> _classificar_ticker('DIVO11.SA')
        'yfinance'
    """

    if ticker in BCB_CODIGOS:
        return 'bcb'
    
    if 'IMA' in ticker or 'IRF' in ticker:
        return 'anbima'
    
    # Default: yfinance (cobre ações .SA, ETFs estrangeiros, etc)
    return 'yfinance'

# ============================================================
# BLOCO 3 - LEITOR PARQUET ANBIMA (FROM MARKET_DATA)
# ============================================================

def _ler_anbima(indices: list[str]) -> pd.DataFrame:
    """
    Lê os índices da família IMA do parquet do market_data.
    
    Transforma o formato long (uma linha por índice/data) para wide
    (uma coluna por índice), pronto para análise de séries temporais.
    
    Args:
        indices: Lista de nomes dos índices (ex: ['IMA-B', 'IMA-B 5'])
    
    Returns:
        DataFrame com 'data' como índice e cada índice como coluna,
        valores = numero_indice.
    
    Raises:
        FileNotFoundError: Se o parquet não existir (rodar market_data primeiro).
    """

    if not ANBIMA_PARQUET.exists():
        raise FileNotFoundError(
            f"Parquet ANBIMA não encontrado: {ANBIMA_PARQUET}\n"
            f"Rode primeiro: python -m scripts.update_anbima no projeto market_data"
        )

    # Lê o parquet inteiro
    df = pd.read_parquet(ANBIMA_PARQUET)

    # Filtra apenas os índices solicitados
    df = df[df['indice'].isin(indices)] # quando passamos um Series como filtro do df, o pandas mantém só as linhas True, isin() gera uma lista de True/False
    
    # Pivota: linhas = data, colunas = indices, valores = numero_indice
    df_wide = df.pivot(index="data", columns="indice", values='numero_indice')

    # Validacao de dados
    encontrados = set(df_wide.columns)
    pedidos = set(indices)
    faltantes = pedidos - encontrados

    if faltantes:
        print(f" Índices não encontrados no parquet: {faltantes}")
        print(f"   Disponíveis: {sorted(encontrados)}")

    return df_wide

# ============================================================
# BLOCO 4 - BAIXAR SERIES TEMPORAIS DO BCB VIA API PUBLICA
# ============================================================

def _baixar_bcb(codigo: int, nome: str, data_inicio: date, data_fim: date) -> pd.Series:
    """
    Baixa uma série temporal do Sistema Gerenciador de Séries (SGS) do BCB.
    
    Args:
        codigo: Código da série no SGS (ex: 11 para Selic, 12 para CDI)
        nome: Nome amigável do ticker (ex: 'SELIC') - vira o nome da Series
        data_inicio: Data inicial da série
        data_fim: Data final da série
    
    Returns:
        Series com índice = data e valores = taxa/cotação.
        IMPORTANTE: Para SELIC/CDI, retorna a TAXA DIÁRIA EM %, não índice acumulado.
    
    Raises:
        requests.HTTPError: Se a API do BCB retornar erro.
    """
    
    # Monta URL no formato esperado pelo BCB
    url = (
        f"https://api.bcb.gov.br/dados/serie/bcdata.sgs.{codigo}/dados?formato=json"
        f"&dataInicial={data_inicio.strftime('%d/%m/%Y')}"
        f"&dataFinal={data_fim.strftime('%d/%m/%Y')}"
    )
    
    print(f"🌐 Baixando {nome} do BCB (código {codigo})...")

    # Faz a requisição
    response = requests.get(url, timeout=30)
    response.raise_for_status() # erro se status != 200

    # Parse do JSON
    dados = response.json()

    if not dados:
         raise ValueError(f"BCB Retornou vazio para o código {codigo}. Verifique datas")
    
    # Converte em dataframe temporário para processamento de colunas
    df = pd.DataFrame(dados)

    # Processa colunas
    df['data'] = pd.to_datetime(df['data'], format='%d/%m/%Y')
    df['valor'] = pd.to_numeric(df['valor'])

    # Cria Series Final (com índice = data, valores = valor)

    serie = pd.Series(
        data=df['valor'].values, # usamos o .values para o pandas extrair o array puro da coluna toda
        index=df['data'],
        name=nome,
    )

    print(f" ✅ {len(serie)} registros de {serie.index.min().date()} até {serie.index.max().date()}")

    return serie

# ============================================================
# BLOCO 5 - BAIXAR PREÇOS DO YFINANCE, COM CACHE LOCAL 
# ============================================================

# Caminho fixo do cache (definido em runtime via config)
_CACHE_YFINANCE = CACHE_DIR / "cache_yfinance.parquet"

def _cache_yfinance_valido() -> bool:
     """verifica se o cache existe e foi gerado hoje."""
     if not _CACHE_YFINANCE.exists():
        return False
     
     mtime = _CACHE_YFINANCE.stat().st_mtime
     data_mod = datetime.fromtimestamp(mtime).date() # retorna a data de modificação do arquivo, em timestamp Unix
     return data_mod == date.today()

def _ler_cache_yfinance(tickers: list[str]) -> pd.DataFrame | None:
    """
    Tenta ler o cache do yfinance.

    Retorna:
    DataFrame se cache é válido E contém todos os tickers solicitados
    None caso contrário
    """
    
    if not _cache_yfinance_valido():
        return None

    df_cache = pd.read_parquet(_CACHE_YFINANCE)

    # Verifica se TODOS os tickers solicitados estão no cache
    tickers_no_cache = set(df_cache.columns)
    if not set(tickers).issubset(tickers_no_cache): # A.issubset(B) -> retorna True se todos os elementos de A estão em B
        return None # Falta algum ticker, precisa rebaixar
    
    print(f"💾 Cache yfinance válido (gerado hoje), usando.")
    return df_cache[tickers]

def _baixar_yfinance(tickers: list[str], data_inicio: date, data_fim: date) -> pd.DataFrame:
    """
    Baixa preços de fechamento de ações/ETFs via yfinance.
    
    Implementa cache simples: se já baixou hoje e tem todos os tickers,
    usa o cache. Senão, baixa de novo e atualiza.
    
    Args:
        tickers: Lista de tickers (ex: ['DIVO11.SA', 'GLD'])
        data_inicio: Data inicial
        data_fim: Data final
    
    Returns:
        DataFrame com colunas = tickers, valores = preço de fechamento ajustado.
    """

    # Tenta usar cache primeiro
    df_cache = _ler_cache_yfinance(tickers)
    if df_cache is not None:
        return df_cache
    
    # Cache inválido ou incompleto: baixa de novo
    print(f"🌐 Baixando {len(tickers)} tickers do yfinance: {tickers}")

    df = yf.download(
        tickers,
        start=data_inicio,
        end=data_fim,
        auto_adjust=True, # Ajusta por splits e dividendos
        progress=False # Esconde a barra de progresso
    )

    # yfinance retorna multi-index se mais de 1 ticker for listado; achataremos para close
    if len(tickers) > 1:
        df = df['Close'] # retorna Series (uma coluna)
    else:
        df = df[['Close']] # retorna dataframe (uma coluna)
        df.columns = tickers # renomeia coluna pro nome do ticker

    # remove linhas totalmente vazias (feriados em todos os mercados)
    df = df.dropna(how='all')

    # Validação de dados dos tickers
    tickers_vazios = df.columns[df.isna().all()].tolist()

    if tickers_vazios:
        print(f"⚠️  \nATENÇÃO: tickers sem dados no Yahoo: {tickers_vazios}")
        print(f"   Possíveis causas: ticker digitado incorretamente, ativo deslistado, ou sem cobertura no período.")
        print(f"   Verifique a grafia ou substitua o ticker.\n")

    # salva no cache para proxima execucao
    _CACHE_YFINANCE.parent.mkdir(parents=True, exist_ok=True)
    df.to_parquet(_CACHE_YFINANCE)
    print(f"💾 Cache atualizado em {_CACHE_YFINANCE.name}")
    
    return df

# ============================================================
# BLOCO 6 - CONVERTER SÉRIE DE PREÇOS EM USD EM BRL 
# ============================================================

def _converter_para_brl(serie_usd: pd.Series, cotacao_brl: pd.Series) -> pd.Series:
    """
    Converte uma série de preços de USD para BRL.
    
    Usa forward fill na cotação para preencher datas onde o ativo USD
    negociou mas a cotação BCB não está disponível (ex: feriados brasileiros).
    
    Args:
        serie_usd: Série de preços em USD (índice = data)
        cotacao_brl: Série de cotação USD/BRL do BCB (índice = data)
    
    Returns:
        Série de preços em BRL.
    """

    # alinha a cotacao com as datas da serie USD, usando ffill
    cotacao_alinhada = cotacao_brl.reindex(serie_usd.index, method='ffill')

    # multiplicacao elemento a elemento
    serie_brl = serie_usd * cotacao_alinhada 

    # mantem nome original
    serie_brl.name = serie_usd.name # quando multiplicamos series, o pandas as vezes apaga o nome, portanto setamos explicitamente

    return serie_brl

# ============================================================
# BLOCO 7 - FUNÇÃO PÚBLICA - ORQUESTRADOR 
# ============================================================

def carregar_precos(
        tickers: list[str],
        data_inicio: date,
        data_fim: date,
) -> pd.DataFrame:
   
    """
    Carrega preços de uma lista de tickers, de múltiplas fontes, em BRL.
    
    Pipeline:
    1. Classifica cada ticker pela fonte (anbima/bcb/yfinance)
    2. Busca dados em cada fonte
    3. Converte tickers USD para BRL (yfinance sem .SA)
    4. Junta tudo num DataFrame alinhado
    5. Aplica forward fill nas datas faltantes
    
    Args:
        tickers: Lista de tickers de qualquer fonte
                (ex: ['SELIC', 'IMA-B', 'DIVO11.SA', 'GLD'])
        data_inicio: Data inicial do histórico
        data_fim: Data final do histórico
    
    Returns:
        DataFrame com:
        - Índice: data
        - Colunas: cada ticker solicitado
        - Valores: preços/níveis em BRL, alinhados com ffill
    
    Examples:
        >>> from datetime import date
        >>> df = carregar_precos(
        ...     tickers=['SELIC', 'IMA-B', 'GLD'],
        ...     data_inicio=date(2020, 1, 1),
        ...     data_fim=date(2020, 12, 31),
        ... )
        >>> df.columns.tolist()
        ['SELIC', 'IMA-B', 'GLD']
    """
    print("=" * 60)
    print(f"📥 CARREGANDO {len(tickers)} TICKERS")
    print("=" * 60)
    
    # ============================================================
    # FASE 1: Classificar tickers por fonte
    # ============================================================

    tickers_anbima = [t for t in tickers if _classificar_ticker(t) == 'anbima']
    tickers_bcb = [t for t in tickers if _classificar_ticker(t) == 'bcb']
    tickers_yfinance = [t for t in tickers if _classificar_ticker(t) == 'yfinance']
   
    print(f"\n📊 Origem dos dados:")
    print(f"   ANBIMA:   {tickers_anbima}")
    print(f"   BCB:      {tickers_bcb}")
    print(f"   yfinance: {tickers_yfinance}")

    # ============================================================
    # FASE 2: Buscar dados de cada fonte
    # ============================================================
   
    pedacos = [] # vai acumular dataframes/series pra juntar depois

    # ANBIMA
    if tickers_anbima:
        print(f"\n[ANBIMA]")
        df_anbima = _ler_anbima(tickers_anbima)
        # Filtra pelo intervalo de datas
        df_anbima = df_anbima.loc[data_inicio:data_fim]
        pedacos.append(df_anbima)
        print(f"🌐 Baixando dados de {len(tickers_anbima)} ativo(s)...")
        print(f"✅ {len(df_anbima)} registros encontrados, de {df_anbima.index.min().date()} até {df_anbima.index.max().date()}")
    
    # BCB
    series_bcb = {} # cache local para reusar USD_BRL na conversão
    if tickers_bcb:
       print(f"\n[BCB]")
       for ticker in tickers_bcb:
           codigo = BCB_CODIGOS[ticker]
           serie = _baixar_bcb(codigo, ticker, data_inicio, data_fim)
           series_bcb[ticker] = serie
           pedacos.append(serie)
    
    # yfinance
    df_yf = None
    if tickers_yfinance:
       print(f"\n[YFINANCE]")
       df_yf = _baixar_yfinance(tickers_yfinance, data_inicio, data_fim)

    # ============================================================
    # FASE 3: Aplicar a conversão USD->BRL onde necessário
    # ============================================================

    if df_yf is not None:
       # Detecta tickers sem .SA (assumidos como USD)
       tickers_usd = [t for t in tickers_yfinance if not t.endswith('.SA')]

    if tickers_usd:
        print(f"\n[CONVERSÃO USD → BRL]")
        print(f"   Tickers em USD: {tickers_usd}")

        # garante que USD_BRL está disponível
        if 'USD_BRL' in series_bcb:
            cotacao = series_bcb['USD_BRL']
        else:
            # Não foi pedido pelo usuário, mas precisamos baixar
            cotacao = _baixar_bcb(
                BCB_CODIGOS['USD_BRL'], 'USD_BRL', data_inicio, data_fim
            )
        
        # Converte cada ticker USD
        for ticker in tickers_usd:
            print(f"   🔄 Convertendo {ticker}...")
            df_yf[ticker] = _converter_para_brl(df_yf[ticker], cotacao)
    
    pedacos.append(df_yf)

    # ============================================================
    # FASE 4: Juntar tudo num DataFrame
    # ============================================================
    if not pedacos:
        raise ValueError("Nenhum dado carregado. Verifique os tickers.")
    
    print(f"\n[CONSOLIDAÇÃO]")
    df_final = pd.concat(pedacos, axis=1)

    # Garante a ordem das colunas conforme o usuário pediu
    df_final = df_final[tickers]

    # ============================================================
    # FASE 5: Aplicar ffill e retornar
    # ============================================================

    df_final = df_final.ffill()

    print(f"\n✅ DataFrame final: {df_final.shape[0]} linhas × {df_final.shape[1]} colunas")
    print(f"   De {df_final.index.min().date()} até {df_final.index.max().date()}")
    print("=" * 60)
    
    return df_final