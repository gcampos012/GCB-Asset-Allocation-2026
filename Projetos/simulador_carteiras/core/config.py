# ============================================================
# BLOCO 1 - IMPORTS E DECRIÇÃO
# ============================================================

"""
Configurações centralizadas do simulador de carteiras.

Define caminhos, constantes e parâmetros padrão usados em todo o projeto.
Se algo mudar (estrutura de pastas, códigos do BCB), edite apenas este arquivo.
"""

from pathlib import Path

# ============================================================
# BLOCO 2 - CAMINHOS DO PROJETO (simulador_carteiras)
# ============================================================

# Raiz do simulador (calculada a partir deste arquivo) -> nossa pasta simulador_carteiras
PROJECT_ROOT = Path(__file__).parent.parent

# Pastas internas
DATA_DIR = PROJECT_ROOT/"data"
OUTPUTS_DIR = PROJECT_ROOT/"outputs"
NOTEBOOKS_DIR = PROJECT_ROOT/"notebooks"

# Cache do data loader
CACHE_DIR = DATA_DIR

# ============================================================
# BLOCO 3 - CAMINHOS EXTERNOS (market_data)
# ============================================================

# Raiz do market_data (calculada a partir deste arquivo)
MARKET_DATA_ROOT = PROJECT_ROOT.parent/"market_data"

# Parquet consolidado da família IMA
ANBIMA_PARQUET = MARKET_DATA_ROOT/"data"/"anbima"/"ima_familia.parquet"

# ============================================================
# BLOCO 4 - CAMINHOS EXTERNOS (market_data)
# ============================================================

#Mapeamento de tickers amigáveis para códigos numéricos do SGS-BCB
# Referência: https://www3.bcb.gov.br/sgspub/
BCB_CODIGOS = {
    'SELIC':   11,    # Taxa Selic - meta
    'CDI':     12,    # CDI - taxa diária
    'USD_BRL': 1,     # Dólar comercial (PTAX venda)
    'IPCA': 433,      # IPCA   
}

# ============================================================
# BLOCO 5 - DEFAULTS DE SIMULAÇÃO
# ============================================================

# Número de dias úteis de um ano (base 252)
DIAS_UTEIS_ANO = 252

# Número de simulações de Monte Carlo (Markowitz)
N_SIMULACOES_MARKOWITZ = 100_000

# Para o Monte Carlo de cenários futuros (a definir nas próximas etapas)
N_SIMULACOES_CENARIOS = 10_000
N_DIAS_PROJECAO = 252  # 1 ano útil

# ============================================================
# BLOCO 6 - GARANTIA DE EXISTÊNCIA DAS PASTAS
# ============================================================

# Cria pastas que o simulador precisa pra escrever arquivos
for diretorio in [DATA_DIR, OUTPUTS_DIR]:
    diretorio.mkdir(parents=True, exist_ok=True)