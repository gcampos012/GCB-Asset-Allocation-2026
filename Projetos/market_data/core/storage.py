"""
O Storage é o bibliotecário do projeto. Ele somente gerencia a base de dados local. Suas atividades são:
- Carregar a base existente
- Salvar a base atualizada
- Consolidar dados antigos com dados novos (sem duplicar)

Neste módulo, usaremos funções, e cada função faz somente UMA coisa MUITO bem feita.
"""

from pathlib import Path
import pandas as pd


# ============================================================
# FUNCAO PARA CARREGAR BASE DE DADOS
# ============================================================

def carregar_base(caminho: Path) -> pd.DataFrame: # definimos uma funcao chamada carregar_base, que recebe um caminho do tipo Path e retorna um dataframe
    """
    Carrega a base historica do parquet.

    Se o arquivo ainda não existir, retorna um DF vazio.
    """
    if caminho.exists(): # path tem o método .exists() - que retorna true se o arquivo existe, false se não
        df = pd.read_parquet(caminho) # se o arquivo existir, o pandas le o arquivo parquet e devolve um df
        print(f"Base carregada: {len(df)} registros.") # imprime um feedback com o numero de registros
        if not df.empty: # se o dataframe nao estiver vazio, mostra o intervalo de datas
            print(f"   De {df['data'].min().date()} até {df['data'].max().date()}")
        return df
    else: # se não, apenas retorna um dataframe vazio
        print("Base ainda não existe. Será criada na primeira atualização.") 
        return pd.DataFrame()

# ============================================================
# FUNCAO PARA SALVAR DATAFRAME EM FORMATO PARQUET
# ============================================================

def salvar_base(df: pd.DataFrame, caminho: Path) -> None: # a função recebe um dataframe e um caminho, porém, não devolve nada, só executa a função (por isso o none)
    """
    Salva o dataframe no formato parquet, ordenando por data e índice antes de salvar, para garantir a consistência dos dados.
    """

    # Garante que a pasta existe
    caminho.parent.mkdir(parents=True, exist_ok=True)

    # Ordena para manter consistência
    df_ordenado = df.sort_values(['data', 'indice']).reset_index(drop=True) # drop=True reseta a numeracao das linhas (0,1,2,3)

    # Salva 
    df_ordenado.to_parquet(caminho, index=False) # salvamos o dataframe em parquet, index=false diz "não salvar o indice numerico do dataframe"

    print(f"💾 Base salva: {len(df_ordenado)} registros em {caminho.name}")

# ============================================================
# FUNCAO PARA CONSOLIDAR OS DATAFRAMES
# ============================================================

def consolidar(base_atual: pd.DataFrame, dados_novos: pd.DataFrame) -> pd.DataFrame:
    """
    Junta as bases removendo as duplicatas. Em caso de conflito (mesma data e índice), mantém os dados novos.
    """
    if base_atual.empty:
        print(f"➕ Base inicial: {len(dados_novos)} registros.")
        return dados_novos
    
    # Concatena os dois dataframes
    consolidado = pd.concat([base_atual, dados_novos], ignore_index=True)

    # Remove as duplicatas (data e índice), mantendo a versao mais recente
    consolidado = consolidado.drop_duplicates(
        subset=['data', 'indice'], # drop_duplicates irá considerar valores duplicados quando data E indice forem iguais
        keep='last' # quando encontrar duplicata, mantém a última 
   )
    
    # Reseta o indice
    consolidado = consolidado.reset_index(drop=True)

    # Calcula quantos registros foram adicionados
    registros_novos = len(consolidado) - len(base_atual)
    print(f"➕ {registros_novos} novos registros adicionados")
    print(f"   Total na base: {len(consolidado)} registros")

    return consolidado