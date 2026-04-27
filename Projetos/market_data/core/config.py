# %%
"""
Configurações centralizadas do projeto market_data.

Esse módulo define todos os caminhos, URLs e constantes usadas pelo projeto. Se algo mudar (estrutura de pastas, URLs, índices), basta editar ESSE arquivo.
"""

from pathlib import Path

# ============================================================
# CAMINHOS DO PROJETO (pasta raiz)
# ============================================================

PROJECT_ROOT = Path(__file__).parent.parent

# __file__ é uma variavel especial python que contem o caminho deste arquivo (config.py)
# path(__file__) transforma esse caminho em um objeto path 
# .parent sobe um nível na árvore de pastas
# .parent.parent sobe dois níveis 

DATA_DIR = PROJECT_ROOT / "data"
LOGS_DIR = PROJECT_ROOT / "logs"
NOTEBOOKS_DIR = PROJECT_ROOT / "notebooks"

# ============================================================
# CAMINHOS POR FONTE DE DADOS
# ============================================================

ANBIMA_DIR = DATA_DIR / "anbima"
ANBIMA_DOWNLOADS_DIR = ANBIMA_DIR / "downloads"
ANBIMA_BASE_FILE = ANBIMA_DIR / "ima_familia.parquet"

# (no futuro, quando adicionar BCB)
# BCB_DIR = DATA_DIR / "bcb"
# BCB_BASE_FILE = BCB_DIR / "selic.parquet"

# ============================================================
# CONFIGURAÇÕES DA ANBIMA
# ============================================================

#URL base do ANBIMA data
ANBIMA_DATA_URL = "https://data.anbima.com.br"

# indices da familia ima que queremos rastrear
INDICES_IMA = [
    "IMA-B",
    "IMA-B 5",
    "IMA-B 5+",
    "IMA-B 5 P2",
    "IRF-M",
    "IRF-M P2"
]

# data de inicio do historico do ima 
IMA_DATA_INICIO = "2001-12-04"

# ============================================================
# CONFIGURAÇÕES GERAIS
# ============================================================

# Formato padrao para datas em strings
DATE_FORMAT = "%Y-%m-%d"

# Timezone do brasil
TIMEZONE = "America/Sao_Paulo"

# ============================================================
# GARANTIA DE EXISTÊNCIA DAS PASTAS
# ============================================================

for diretorio in [DATA_DIR, LOGS_DIR, ANBIMA_DIR, ANBIMA_DOWNLOADS_DIR]:
    diretorio.mkdir(parents=True, exist_ok=True)

