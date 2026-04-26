# %%
import pandas as pd
import os

# %%
tabela = pd.read_csv("../precotaxatesourodireto.csv", sep=";")
tabela.tail(10)

# %%
print(tabela.head(10))


# %%
titulos = pd.Series(tabela["Tipo Titulo"])
type(titulos)

# %%
# DataFrames
titulo_e_taxa = pd.DataFrame(tabela[["Tipo Titulo", "Taxa Compra Manha"]])
titulo_e_taxa

# %%
type(titulo_e_taxa)


# %% 3) Navegando pelos dados

# Informações Básicas
# Tipos de colunas
# Navegação em linhas e colunas
# Renomeando colunas


# %% 4) Filtrando dados

# Condições Lógidas


# %% 5) Transformações e remoções

# Criação de novas colunas
# Ordenação

# %% Replace
 

# %% Conversão de tipos 
#tabela["PU Base Manha"] = tabela["PU Base Manha"].str.replace(",", ".").astype(float)
tabela["Data Vencimento"]


# %% Conversão de str para datas
pd.to_datetime(tabela["Data Vencimento"], format="%d/%m/%Y")
tabela["Data Vencimento"] = pd.to_datetime(tabela["Data Vencimento"], format="%d/%m/%Y")
tabela["Data Vencimento"]

# %%
type(tabela["Data Vencimento"])