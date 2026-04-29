"""
Processamento de dados da ANBIMA Data.

Lê os arquivos XLS baixados manualmente do site data.anbima.com.br
e converte para um DataFrame padronizado, pronto para ser
consolidado na base local.
"""

from pathlib import Path
import pandas as pd

# ============================================================
# CONSTANTES
# ============================================================

# Nome da Aba do arquivo XLS que contém os dados
SHEET_NAME = "Historico"

# Padronização do nome das colunas
COLUNAS_MAPEAMENTO = {
    'Índice': 'indice',
    'Data de Referência': 'data',
    'Número Índice': 'numero_indice',
    'Variação Diária (%)': 'variacao_diaria',
    'Variação no Mês (%)': 'variacao_mes',
    'Variação no Ano (%)': 'variacao_ano',
    'Variação 12 Meses (%)': 'variacao_12m',
    'Variação 24 Meses (%)': 'variacao_24m',
    'Duration (d.u.)': 'duration',
    'PMR': 'pmr',
}

# ============================================================
# FUNÇÕES INTERNAS
# ============================================================

# Função que irá checar se o arquivo XLS tem as colunas esperadas
def _validar_colunas(df: pd.DataFrame) -> None:
    """
    Essa função irá verificar se o arquivo XLS contém todas as colunas que queremos.
    Caso contrário, irá apresentar uma mensagem dizendo sobre as colunas faltantes, mas não interrompe o processamento.
    Funções que começam com underscore (_) são funções privadas/internas ao módulo (a função é só para uso interno aqui dentro do módulo)  
    """

    colunas_esperadas = set(COLUNAS_MAPEAMENTO.keys()) # A funcao set faz com que os valores de colunas_mapeamento sejam valores unicos e com operacoes matematicas
    colunas_recebidas = set(df.columns)

    faltantes = colunas_esperadas - colunas_recebidas # essa operacao fara com que encontremos o valor que esta faltando (valor = nome coluna)
    inesperadas = colunas_recebidas - colunas_esperadas

    if faltantes:
        print(f"⚠️  Colunas esperadas que NÃO vieram: {faltantes}")
    
    if inesperadas:
        print(f"ℹ️  Colunas novas detectadas: {inesperadas}")
        print("   Considere adicioná-las ao COLUNAS_MAPEAMENTO se forem úteis...")

# Função que fará correção de bugs contidos no arquivo XLS
def _normalizar_indice(nome: str) -> str:
    """
    Normaliza o nome do índice removendo espaços extras.
    """

    # Remove espaços ao redor de hífens
    return nome.replace(' - ', '-').strip() # comando que substitui espaço-hifen-espaço por somente o hife, .strip() remove espacos no inicio e fim da string

# ============================================================
# FUNÇÃO PRINCIPAL (API PÚBLICA)
# ============================================================

def processar_xls(caminho_xls: Path) -> pd.DataFrame:
    """
    Lê e padroniza o XLS do ANBIMA Data.

    Args:
        caminho_xls: Caminho do arquivo XLS baixado manualmente

    Returns:
        DataFrame com colunas padronizadas, índices normalizados e linhas inválidas removidas.
    """

    print(f"📂 Lendo {caminho_xls.name}...") # caminho_xls.name mostra apenas o nome do arquivo, sem apresentar o caminho completo
    df = pd.read_excel(caminho_xls, sheet_name=SHEET_NAME)
    print(f"   {len(df)} linhas brutas carregadas")

    # Valida a estrutura (avisa mas não interrompe)
    _validar_colunas(df) # esse comando chama a funcao privada _validar_colunas, que irá nos avisar se houver alguma diferença

    # Renomeia apenas colunas que existem (proteção contra colunas faltantes)
    colunas_para_renomear = {
        original: novo
        for original, novo in COLUNAS_MAPEAMENTO.items()
        if original in df.columns
    }
    df = df.rename(columns=colunas_para_renomear)

    if 'indice' in df.columns:
        antes = df['indice'].nunique() # df['indice'].nunique() Conta quantos valores únicos a coluna tem
        df['indice'] = df['indice'].apply(_normalizar_indice) # df['indice'].apply(_normalizar_indice) aplica uma função a cada elemento da coluna 
        depois = df['indice'].nunique()

        if antes != depois: # se o numero de elementos unicos de antes for diferente do num. elementos de depois, mostraremos isso ao usuario
            print(f"🔧 Normalização de índices: {antes} variações → {depois} únicos")
    
    if 'data' in df.columns: # se 'data' estiver nas colunas do df transformaremos essa coluna em tipo datetime 
        df['data'] = pd.to_datetime(df['data'])

    # Remove as linhas que nao contiverem numero_indice (linhas numero_indice com NA) 
    if 'numero_indice' in df.columns:
        antes = len(df)
        df = df.dropna(subset=['numero_indice']) # usamos subset para dropar apenas as linhas que tiverem numero_indice = NA
        removidas = antes - len(df)
    
        if removidas > 0:
            print(f"🗑️  {removidas} linhas removidas (sem 'numero_indice')")

    # Ordena por data e índice
    df = df.sort_values(['data', 'indice']).reset_index(drop=True)
    print(f"✅ Processamento concluído: {len(df)} linhas finais")

    return df
