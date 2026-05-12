"""
Orquestrador: roda backtest histórico em janelas rolantes (6m, 12m, 24m, 36m).

Diferença pro run_backtest_brl:
- Esse roda em janelas CURTAS pra avaliar performance recente
- Markowitz é calculado UMA VEZ com a janela completa (pesos atemporais)
- Aplica esses pesos em cada janela curta

Uso:
    python -m scripts.run_backtest_rolante
    python -m scripts.run_backtest_rolante --config carteiras/configs/balanceada.json
"""

import argparse
from datetime import date

from dateutil.relativedelta import relativedelta

from carteiras.carteira_brl import load_carteira, get_tickers
from core.data_loader import carregar_precos
from analysis.markowitz import calcular_fronteira_eficiente
from analysis.backtest import executar_backtest


CARTEIRA_DEFAULT = "carteiras/configs/balanceada.json"

# Janelas a analisar (em meses)
JANELAS_MESES = [6, 12, 24, 36]

# Estratégias a testar em cada janela
ESTRATEGIAS = ['buy_and_hold', 'anual']

VALOR_INICIAL = 100.0


def main() -> None:
    """Pipeline de backtest rolante."""
    
    # ========================================================
    # 0. PARSEAR ARGUMENTOS
    # ========================================================
    parser = argparse.ArgumentParser(
        description="Roda backtest histórico em janelas rolantes (6m, 12m, 24m, 36m).",
    )
    parser.add_argument(
        "--config",
        type=str,
        default=CARTEIRA_DEFAULT,
        help=f"Caminho do JSON de configuração (default: {CARTEIRA_DEFAULT})",
    )
    args = parser.parse_args()
    
    # ========================================================
    # 1. CARREGAR CARTEIRA E DADOS
    # ========================================================
    carteira = load_carteira(args.config)
    
    print("\n" + "█" * 60)
    print(f"█ BACKTEST ROLANTE — {carteira['nome'].upper()}")
    print("█" * 60)
    print(f"📂 Config: {args.config}")
    print(f"📅 Janelas: {JANELAS_MESES} meses")
    print(f"🎯 Estratégias: {ESTRATEGIAS}")
    
    ativos = get_tickers(carteira)
    benchmark = carteira['benchmark']
    
    data_fim = date.today()
    
    precos = carregar_precos(
        tickers=ativos + [benchmark],
        data_inicio=carteira['data_inicio_default'],
        data_fim=data_fim,
    )
    
    # ========================================================
    # 2. RODAR MARKOWITZ NA JANELA COMPLETA
    # ========================================================
    # Calcula CDI anualizado na janela total dos ativos
    janela_inicio_total = precos[ativos].dropna().index.min()
    serie_cdi = precos[benchmark].loc[janela_inicio_total:].dropna()
    n_anos = len(serie_cdi) / 252
    cdi_anual_total = (
        serie_cdi.iloc[-1] / serie_cdi.iloc[0]
    ) ** (1/n_anos) - 1
    
    print(f"\n💰 CDI anualizado (janela total): {cdi_anual_total:.2%}")
    
    # Markowitz global (uma vez)
    resultado_markowitz = calcular_fronteira_eficiente(
        precos=precos[ativos],
        taxa_livre_anual=cdi_anual_total,
        seed=42,
    )
    
    pesos_max_sharpe = resultado_markowitz['max_sharpe']['pesos']
    pesos_min_var = resultado_markowitz['min_variancia']['pesos']
    
    # ========================================================
    # 3. RODA BACKTEST EM CADA JANELA
    # ========================================================
    resultados = {}  # estrutura: {janela: {carteira: {estrategia: resultado}}}
    
    for janela_meses in JANELAS_MESES:
        data_inicio_janela = data_fim - relativedelta(months=janela_meses)
        
        print(f"\n   🔍 Janela de {janela_meses}m: {data_inicio_janela} → {data_fim}")
        
        # Recorta preços nessa janela
        precos_janela = precos.loc[data_inicio_janela:]
        
        # Calcula CDI anualizado nessa janela específica (pra retorno equivalente)
        serie_cdi_janela = precos_janela[benchmark].dropna()
        n_anos_janela = len(serie_cdi_janela) / 252
        cdi_anual_janela = (
            serie_cdi_janela.iloc[-1] / serie_cdi_janela.iloc[0]
        ) ** (1/n_anos_janela) - 1
        
        resultados[janela_meses] = {
            'cdi_anual': cdi_anual_janela,
            'Max Sharpe': {},
            'Min Variância': {},
            'CDI': {},
        }
        
        # Roda Max Sharpe nas 2 estratégias
        for estrategia in ESTRATEGIAS:
            resultados[janela_meses]['Max Sharpe'][estrategia] = executar_backtest(
                precos=precos_janela[ativos],
                pesos=pesos_max_sharpe,
                estrategia=estrategia,
                valor_inicial=VALOR_INICIAL,
            )
        
        # Roda Min Variância nas 2 estratégias
        for estrategia in ESTRATEGIAS:
            resultados[janela_meses]['Min Variância'][estrategia] = executar_backtest(
                precos=precos_janela[ativos],
                pesos=pesos_min_var,
                estrategia=estrategia,
                valor_inicial=VALOR_INICIAL,
            )
        
        # CDI (sempre buy_and_hold)
        resultados[janela_meses]['CDI']['buy_and_hold'] = executar_backtest(
            precos=precos_janela[[benchmark]],
            pesos={benchmark: 1.0},
            estrategia='buy_and_hold',
            valor_inicial=VALOR_INICIAL,
        )
    
    # ========================================================
    # 4. IMPRIMIR TABELA COMPARATIVA
    # ========================================================
    _imprimir_tabela_rolante(resultados)
    
    print(f"\n✅ Backtest rolante concluído.\n")


def _imprimir_tabela_rolante(resultados: dict) -> None:
    """Imprime a tabela comparativa das janelas rolantes."""
    print("\n" + "═" * 110)
    print("📋 TABELA COMPARATIVA: JANELAS ROLANTES")
    print("═" * 110)
    
    # Cabeçalho com janelas
    janelas = sorted(resultados.keys())
    cabecalho = f"{'Carteira':<15} {'Estratégia':<14} "
    for j in janelas:
        cabecalho += f"{f'{j}m':>20} "
    print(cabecalho)
    print("─" * 110)
    
    # Linhas: cada combinação (carteira + estratégia)
    combinacoes = [
        ('Max Sharpe', 'buy_and_hold'),
        ('Max Sharpe', 'anual'),
        ('Min Variância', 'buy_and_hold'),
        ('Min Variância', 'anual'),
        ('CDI', 'buy_and_hold'),
    ]
    
    for carteira_nome, estrategia in combinacoes:
        # Pula combinações inexistentes (ex: CDI + anual)
        if estrategia not in resultados[janelas[0]][carteira_nome]:
            continue
        
        linha = f"{carteira_nome:<15} {estrategia:<14} "
        
        for j in janelas:
            resultado = resultados[j][carteira_nome][estrategia]
            cdi_janela = resultados[j]['cdi_anual']
            
            # Retorno acumulado da janela (valor_final - 100)
            valor_final = resultado['valores'].iloc[-1]
            retorno_acumulado = (valor_final / VALOR_INICIAL) - 1
            
            # Retorno anualizado
            retorno_anualizado = resultado['retorno_anualizado']
            
            # Spread sobre o CDI (anualizado)
            spread = retorno_anualizado - cdi_janela
            sinal = '+' if spread >= 0 else '-'
            
            # Formato: "+12.1% (CDI+2.7%)"
            celula = f"{retorno_acumulado:+.1%} (CDI{sinal}{abs(spread):.1%})"
            linha += f"{celula:>20} "
        
        print(linha)
    
    print("═" * 110)
    
    # CDI anualizado por janela (referência)
    print(f"\n💡 CDI anualizado por janela:")
    for j in janelas:
        print(f"   {j}m: {resultados[j]['cdi_anual']:.2%}")
    
    print(f"\n📊 Como ler:")
    print(f"   Retorno acumulado (CDI±X%): retorno realizado na janela e spread anualizado sobre CDI")
    print(f"   BH = Buy and Hold | Anual = rebalanceamento anual")


if __name__ == "__main__":
    main()