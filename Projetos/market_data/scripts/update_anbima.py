# %%
"""
Script de atualização da base de dados da ANBIMA.

Pipeline: 
1. Carregar base existente
2. Processa o XLS mais recente em data/anbima/downloads
3. Consolida (sem duplicar) com a base
4. Salva o resultado

Uso:
    python -m scipts.update_anbima
"""

import  pandas as pd
from pathlib import Path
from core.config import ANBIMA_DOWNLOADS_DIR, ANBIMA_BASE_FILE # Importamos os caminhos definidos em config.py
from core.storage import carregar_base, consolidar, salvar_base # Importamos as funções do módulo storage que iremos utilizar
from sources.anbima import processar_xls # Importamos a função anbima.py para fazer o processamento 

def _listar_xls(pasta: Path) -> list[Path]:
    """
    Lista todos os arquivos XLS da pasta downloads.

    Retorna a lista ordenada alfabeticamente para a reprodutibilidade
    """

    xls_files = sorted(pasta.glob("*.xlsx"))

    if not xls_files:
        raise FileNotFoundError(
            f"Nenhum arquivo .xlsx foi encontrado em {pasta}.\n"
            f"Baixa os arquivos XLSX do ANBIMA DATA e coloque dentro das pasta {pasta.name}.\n"
        )

    return xls_files

def main() -> None:
    """
    Executa o pipeline completo de atualização da base ANBIMA.
    """

    print("="*60+"\n🚀 ATUALIZAÇÃO DA BASE ANBIMA\n"+"="*60)

    # 1. Lista TODOS os XLS da pasta
    print("\n[1/4] Localizando XLS...")
    arquivos_xlsx = _listar_xls(ANBIMA_DOWNLOADS_DIR)
    print(f"   {len(arquivos_xlsx)} arquivo(s) encontrado(s):")
    for arq in arquivos_xlsx:
        print(f"   -{arq.name}")

    # 2. Carrega base existente
    print("\n[2/4] Carregando base atual...")
    base_atual = carregar_base(ANBIMA_BASE_FILE)

    # 3. Processa TODOS os XLS e junta
    print("\n[3/4] Processando os XLS...")
    dataframes_novos = []
    for arq in arquivos_xlsx:
        print(f"  - {arq.name} -")
        df = processar_xls(arq)
        dataframes_novos.append(df)

    # Concatena tudo num único DataFrame
    dados_novos = pd.concat(dataframes_novos, ignore_index=True)
    print(f"\n    Total combinado: {len(dados_novos)} linhas.")

    # 4. Consolida e salva
    print("\n [4/4] Consolidando e salvando...")
    base_atualizada = consolidar(base_atual, dados_novos)
    salvar_base(base_atualizada, ANBIMA_BASE_FILE)

    print("="*60+"\n✅ ATUALIZAÇÃO CONCLUÍDA\n"+"="*60)

if __name__ == "__main__":
    main()
# esse codigo acima diz "só rode main() se este arquivo for o ponto de entrada, nao se ele estiver sendo somente importado"


    