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

from pathlib import Path
from core.config import ANBIMA_DOWNLOADS_DIR, ANBIMA_BASE_FILE # Importamos os caminhos definidos em config.py
from core.storage import carregar_base, consolidar, salvar_base # Importamos as funções do módulo storage que iremos utilizar
from sources.anbima import processar_xls # Importamos a função anbima.py para fazer o processamento 

def _encontrar_xls_mais_recente(pasta:Path) -> Path: # Essa função recebe um path e retorna um path
    """
    Encontra o arquivo XLS mais recentemente modificado na pasta.

    Útil quando a pasta downloads/ acumula vários downloads ao longo do tempo. 
    """

    xls_files = list(pasta.glob("*.xlsx")) # criamos uma lista com os arquivos dentro da pasta. .glob() faz com que a busca seja por padrão de arquivo

    if not xls_files:
        raise FileNotFoundError(
            f"Nenhum arquivo .xlsx encontrado em {pasta}.\n"
            f"Baixe um XLS do ANBIMA data e coloque na pasta :)\n"
        )
    
    # Ordena por data de modificação (mais recente primeiro)
    mais_recente = max(xls_files, key=lambda f: f.stat().st_mtime) # código que pega o arquivo com a data mais recente (data máxima) 
    # max() -> função que retorna o maior item, mas ao passarmos key=funcao ele aplica a funcao para todos os itens e os compara
    # lambda -> funcao anonima, é util quando quremos fazer uma função pequena em um único lugar
    # a letra f representa a variavel xls_files, que é do tipo path. Portanto, quando chamamos f.stat() estamos chamando path.stat()
    # path.stat() retorna informações do arquivo, já path.stat().st_mtime retorna a data da última modificação 

    return mais_recente

def main() -> None:
    """
    Executa o pipeline completo de atualização da base ANBIMA.
    """

    print("="*60+"\n🚀 ATUALIZAÇÃO DA BASE ANBIMA\n"+"="*60)

    # 1. Encontra o XLS mais recente
    print("\n[1/4] Localizando XLS...")
    caminho_xls = _encontrar_xls_mais_recente(ANBIMA_DOWNLOADS_DIR)
    
    # 2. Carrega base existente
    print("\n[2/4] Carregando base atual...")
    base_atual = carregar_base(ANBIMA_BASE_FILE)

    # 3. Processa o XLS
    print("\n[3/4] Processando o XLS...")
    dados_novos = processar_xls(caminho_xls)

    # 4. Consolida e salva
    print("\n [4/4] Consolidando e salvando...")
    base_atualizada = consolidar(base_atual, dados_novos)
    salvar_base(base_atualizada, ANBIMA_BASE_FILE)

    print("="*60+"\n✅ ATUALIZAÇÃO CONCLUÍDA\n"+"="*60)

if __name__ == "__main__":
    main()
# esse codigo acima diz "só rode main() se este arquivo for o ponto de entrada, nao se ele estiver sendo somente importado"


    