# %%
# Importação dos pacotes
import pandas as pd
import yfinance as yf
import matplotlib.pyplot as plt
import numpy as np
import datetime as dt
from datetime import date
from dateutil.relativedelta import relativedelta

# %%
# Coletando o DF variação da selic
hoje = dt.date.today()
dez_anos_atras = hoje - relativedelta(years=10) 

url = (
    f"https://api.bcb.gov.br/dados/serie/bcdata.sgs.11/dados?formato=json"
    f"&dataInicial={dez_anos_atras.strftime('%d/%m/%Y')}"
    f"&dataFinal={hoje.strftime('%d/%m/%Y')}"
)
df = pd.read_json(url)

# %%
df["data"] = pd.to_datetime(df["data"], format="%d/%m/%Y")
df["valor"] = pd.to_numeric(df["valor"])
df.set_index("data", inplace=True)
df["SELIC"] = ((1 + df["valor"] / 100).cumprod()-1)*100
df["SALDO_ACUM_SELIC"] = 10000 * (df["SELIC"] / 100 + 1)

# %%
# Ativos brasileiros negociados na B3
tickers_brl = ["DIVO11.SA", "IMAB11.SA", "B5P211.SA"]
tickers = tickers_brl

#%%
# Coleta os dados (todos em BRL, sem conversão de moeda)
dados = yf.download(tickers, start=dez_anos_atras, end=hoje)["Close"]
dados["SELIC"] = df["SALDO_ACUM_SELIC"].reindex(dados.index, method="ffill")
dados = dados.dropna()
dados_pct = (dados / dados.iloc[0] - 1) * 100
dados_pct

# %%
# Plota o gráfico
dados_pct.plot(title="Rentabilidade", figsize=(12, 5))
plt.ylabel("Retorno Acumulado (%)")
plt.tight_layout()
plt.show()

# %%
# FRONTEIRA EFICIENTE MARKOWITZ
# 1) Calcular os retornos diários
retornos = dados.pct_change().dropna()
retornos

#%%
# 2) Parâmetros fundamentais
# Retorno esperado -> retorno diário médio * 252)
retorno_medio = retornos.mean() * 252

# Matriz de covariância
cov_matrix = retornos.cov() * 252

# Taxa livre de risco atual (Selic Atual)
taxa_livre_risco = (1 + df["valor"].iloc[-1] / 100) ** 252 - 1  # CDI anualizado - ajuste conforme necessário

# %%
# 3) Estabelecer o Número de portfólios aleatórios
n_ativos = len(retornos.columns)
n_simulacoes = 100000

# Lista vazia para anexar os resultados
resultados = []

# Loop que preenchera todas as simulacoes 
for i in range(n_simulacoes):
    pesos = np.random.random(n_ativos) # objeto que cria pesos randômicos a cada execução
    pesos /= pesos.sum() # objeto que normaliza a randomização para termos pesos = 100% 
    retorno_portfolio = pesos @ retorno_medio # Calcula o retorno do portfolio
    risco_portfolio = np.sqrt(pesos @ cov_matrix.values @ pesos) # Calcula o risco do portfolio
    sharpe = (retorno_portfolio - taxa_livre_risco) / risco_portfolio # Calcula o sharpe do portfolio
    resultados.append([retorno_portfolio, risco_portfolio, sharpe, pesos]) # Adiciona todos os dados no DF resultados

# %%
# 4) Encontrar os portfólios ótimos
df_portfolios = pd.DataFrame(resultados, columns=["Retorno", "Risco", "Sharpe", "Pesos"])

# %%
# 5) Identifica a linha (o portfolio) de menor risco (menor desvio padrão)
idx_min_var = df_portfolios["Risco"].idxmin()

# identifica a linha (o portfolio) de menor risco (menor desvio padrão)
idx_max_var = df_portfolios["Risco"].idxmax()

# Identifica a linha (o portfolio) com maior sharpe (máxima relação retorno / risco)
idx_max_sharpe = df_portfolios["Sharpe"].idxmax()

# Identifica a linha (o portfolio) com maior sharpe (máxima relação retorno / risco)
idx_min_sharpe = df_portfolios["Sharpe"].idxmin()

# Portfolio moderado: melhor Sharpe entre mínima variância e portfólio de mercado
risco_min = df_portfolios.loc[idx_min_var, "Risco"]

# Portfolio agressivo: melhor Sharpe entre mínima variância e portfólio de mercado
risco_max = df_portfolios.loc[idx_max_var, "Risco"]

# Portfolio de mercado: 
risco_mercado = df_portfolios.loc[idx_max_sharpe, "Risco"]

# Portfolio péssimo de mercado:
risco_mercado_pessimo = df_portfolios.loc[idx_min_sharpe, "Risco"]

# %%
# 6) Definindo e selecionando o melhor portfolio considerando o sharpe  
df_mercado = df_portfolios[
    (df_portfolios["Risco"] >= risco_min) &
    (df_portfolios["Risco"] <= risco_mercado)
]
idx_mercado = df_mercado["Sharpe"].idxmax()

# %%
# 7) Calcular a Capital Line Market do portfolio moderado
rm = df_portfolios.loc[idx_mercado, "Retorno"] # rm (Return max) recebe o valor do retorno do portfolio de maior sharpe
sm = df_portfolios.loc[idx_mercado, "Risco"] # sm (Sigma Max) recebe o valor do risco do portfólio de maior sharpe

sigma_cml = np.linspace(0, df_portfolios["Risco"].max() * 1.1, 300)
retorno_cml = taxa_livre_risco + ((rm - taxa_livre_risco) / sm) * sigma_cml

# %%
# 8) Configura os parâmetros do gráfico a ser plotado
plt.figure(figsize=(12, 6))
scatter = plt.scatter(
    df_portfolios["Risco"], df_portfolios["Retorno"],
    c=df_portfolios["Sharpe"], cmap="viridis", alpha=0.5, s=5
)
plt.colorbar(scatter, label="Sharpe Ratio")

plt.plot(sigma_cml, retorno_cml, color="black", linewidth=1.5,
         linestyle="--", label="Linha de Mercado de Capitais (CML)")
plt.scatter(0, taxa_livre_risco,
            color="black", s=80, zorder=5, label=f"CDI - Taxa Livre de Risco ({taxa_livre_risco*100:.1f}%)")
plt.scatter(*df_portfolios.loc[idx_min_var, ["Risco", "Retorno"]],
            color="blue", s=100, zorder=5, label="Mínima Variância")
plt.scatter(*df_portfolios.loc[idx_mercado, ["Risco", "Retorno"]],
            color="red", s=100, zorder=5, label="Portfólio de Mercado (Máx. Sharpe)")

plt.xlabel("Risco (Desvio Padrão Anualizado)")
plt.ylabel("Retorno Esperado Anualizado")
plt.title("Fronteira Eficiente - Mercado Brasileiro (BRL)")
plt.legend()
plt.tight_layout()
plt.show()

# %%
# 9) Exibe a composicao dos portfolios
lista_ativos = retornos.columns.tolist()

print("=== Portfólio de Máximo Sharpe ===")
for ativo, peso in zip(lista_ativos, df_portfolios.loc[idx_max_sharpe, "Pesos"]):
    print(f"  {ativo}: {peso*100:.1f}%")
print(f"  Retorno esperado: {df_portfolios.loc[idx_max_sharpe, 'Retorno']*100:.1f}%")
print(f"  Risco:            {df_portfolios.loc[idx_max_sharpe, 'Risco']*100:.1f}%")
print(f"  Sharpe:           {df_portfolios.loc[idx_max_sharpe, 'Sharpe']:.2f}")

print("\n=== Portfólio de Mínima Variância ===")
for ativo, peso in zip(lista_ativos, df_portfolios.loc[idx_min_var, "Pesos"]):
    print(f"  {ativo}: {peso*100:.1f}%")
print(f"  Retorno esperado: {df_portfolios.loc[idx_min_var, 'Retorno']*100:.1f}%")
print(f"  Risco:            {df_portfolios.loc[idx_min_var, 'Risco']*100:.1f}%")
print(f"  Sharpe:           {df_portfolios.loc[idx_min_var, 'Sharpe']:.2f}")

# %%
# 10) Resultado do Portfolio de Mínima Variância (menor risco)
alocacao_min_var = []
for ativo, peso in zip(lista_ativos, df_portfolios.loc[idx_min_var, "Pesos"]):
    alocacao_min_var.append(peso * 100000)
#alocacao_min_var
df_min_var = alocacao_min_var * (1 + dados_pct / 100)
df_min_var["Portfolio"] = df_min_var.sum(axis=1)
df_min_var_pct =(df_min_var / df_min_var.iloc[0] -1) * 100
df_min_var_pct

# %%
# 11) Plota o gráfico
df_min_var_pct[["SELIC", "Portfolio"]].plot(title="Patrimônio Acumulado", figsize=(12, 5))
plt.xlabel("Data")
plt.ylabel("Retorno Acumulado (BRL)")
plt.tight_layout()
plt.show()

# %%
# 12) Exibe o saldo do Portfolio de Mínima variância 
print("=== Portfólio Conservador ===\n")
for ativo, saldo in zip(lista_ativos, df_min_var.iloc[-1]):
    print(f"  {ativo}: R$ {saldo:.2f}")
print(f"\n  Patrimônio Atual: R$ {df_min_var["Portfolio"].iloc[-1]:.2f}")

# %%
# 13) Resultado do Portfolio de Máximo Sharpe (maior sharpe)
alocacao_max_sharpe = []
for ativo, peso in zip(lista_ativos, df_portfolios.loc[idx_max_sharpe, "Pesos"]):
    alocacao_max_sharpe.append(peso * 100000)
#alocacao_min_var
df_max_sharpe = alocacao_max_sharpe * (1 + dados_pct / 100)
df_max_sharpe["Portfolio"] = df_max_sharpe.sum(axis=1)
df_max_sharpe_pct =(df_max_sharpe / df_max_sharpe.iloc[0] -1) * 100
df_max_sharpe_pct

# %%
# 14) Plota o gráfico
df_max_sharpe_pct[["SELIC", "Portfolio"]].plot(title="Patrimônio Acumulado", figsize=(12, 5))
plt.xlabel("Data")
plt.ylabel("Retorno Acumulado (BRL)")
plt.tight_layout()
plt.show()

# %%
# 15) Exibe o saldo do Portfolio de Mínima variância 
print("=== Portfólio de Maior Sharpe ===\n")
for ativo, saldo in zip(lista_ativos, df_max_sharpe.iloc[-1]):
    print(f"  {ativo}: R$ {saldo:.2f}")
print(f"\n  Patrimônio Atual: R$ {df_max_sharpe["Portfolio"].iloc[-1]:.2f}")
# %%
