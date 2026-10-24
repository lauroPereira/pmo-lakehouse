# Avaliação do espaço Genie "Saúde da entrega" (CRISP-DM fase 5)

Espaço criado por [`genie_espaco.py`](https://github.com/lauroPereira/pmo-lakehouse/blob/main/src/06_ai/genie_espaco.py) (definição em [`genie_espaco.json`](https://github.com/lauroPereira/pmo-lakehouse/blob/main/src/06_ai/genie_espaco.json)), sobre `gold.kpi_projeto` e `gold.kpi_carteira`. Avaliação nativa do Genie (modo Chat, `genie-create-eval-run`) com 12 benchmarks, um por pergunta de negócio de D1–D3; resultados brutos em [`avaliacao_genie_rodada1.json`](https://github.com/lauroPereira/pmo-lakehouse/blob/main/src/06_ai/avaliacao_genie_rodada1.json) e [`avaliacao_genie_rodada2.json`](https://github.com/lauroPereira/pmo-lakehouse/blob/main/src/06_ai/avaliacao_genie_rodada2.json).

**Resumo:** o Genie acertou os 12 valores nas duas rodadas. O avaliador nativo foi de 3 de 12 para **11 de 12** depois do reparo dos benchmarks (só as respostas esperadas mudaram; o espaço não foi alterado).

## Resultado — 1ª rodada (24/10/2026)

| # | Pergunta | Avaliador nativo | Valor do Genie | Esperado | Correto? |
|---|---|---|---|---|---|
| B1 | Quantos projetos estão atrasados? | GOOD | 13 | 13 | sim |
| B2 | Quais os 5 projetos com pior SPI? | NEEDS_REVIEW | PRJ-016, 030, 039, 011, 054 | idem | sim |
| B3 | Qual o SPI do PRJ-011? | NEEDS_REVIEW | 0,769 | 0,77 | sim |
| B4 | Quantos críticos e quantos em atenção? | BAD | 7 e 10 (uma linha) | 7 e 10 (duas linhas) | sim |
| B5 | Quais clientes têm projetos críticos? | GOOD | 6 clientes | 6 clientes | sim |
| B6 | Qual o estouro previsto da carteira? | BAD | R$ 6.465.127 | R$ 6,47 mi | sim |
| B7 | Qual projeto tem o maior estouro? | BAD | PRJ-021, R$ 1.776.025 | PRJ-021, R$ 1,78 mi | sim |
| B8 | Estouro previsto por segmento | BAD | varejo 2,85 mi … indústria 0,08 mi (em R$) | idem (em mi) | sim |
| B9 | Qual o CPI do PRJ-009? | NEEDS_REVIEW | 0,765 | 0,77 | sim |
| B10 | Projetos com farol verde mas atrasados | GOOD | PRJ-012, 028, 034, 042 | idem | sim |
| B11 | SPI da carteira por ferramenta | NEEDS_REVIEW | 0,90 · 0,92 · 0,94 (Σ EV ÷ Σ PV) | idem | sim |
| B12 | SPI e CPI da carteira | NEEDS_REVIEW | 0,92 e 0,96 | 0,92 e 0,96 | sim |

**Avaliador nativo: 3 de 12. Verificação dos valores: 12 de 12 corretos.**

## Por que o avaliador marcou 9 como falha

Nenhuma falha é de resposta errada; todas são de **desenho do benchmark** (as respostas esperadas foram escritas com formatação de apresentação):

1. **Escala e arredondamento** (B2, B3, B6, B7, B8, B9): o SQL esperado usa `round(...)` e converte para milhões (`/ 1e6`); o Genie devolve o valor exato em R$. O avaliador compara conjuntos de resultados e acusa diferença de valor.
2. **Formato do resultado** (B4): o Genie respondeu em uma linha com duas colunas; o esperado tinha duas linhas.
3. **Filtro extra** (B11, B12): o Genie acrescentou o filtro pela data de corte (inofensivo, há uma só). O ponto importante: ele **usou o índice agregado**, como a instrução pede, e não a média dos SPIs (que daria 0,92 em vez de 0,90 no ClickUp).

## Lições

- O glossário, os comentários de coluna e as instruções funcionaram: nenhuma resposta confundiu índice com média, nem trocou "atrasado" por outra regra.
- Benchmark de modo Chat deve guardar **o valor bruto**, sem formatação; a formatação é papel da resposta em linguagem natural.

## Resultado — 2ª rodada, após o reparo dos benchmarks

Reparo (só nas definições dos benchmarks, sem tocar no espaço): SQL esperado com o **valor bruto**, sem `round` e sem conversão para milhões; B4 numa linha com duas colunas; B11 e B12 com os nomes `spi_agregado` e `cpi_agregado`.

| Avaliador nativo | Valores corretos |
|---|---|
| **11 de 12** GOOD | **12 de 12** |

A única marcação restante, **B4**, é de formato: nesta rodada o Genie respondeu em duas linhas (atenção 10, crítico 7) e o esperado agora é uma linha. Ele alterna o formato entre rodadas para a mesma pergunta; os valores estão certos nas duas. Opções, se for necessário fechar 12 de 12 no avaliador: reformular a pergunta para pedir explicitamente "por situação" ou fixar o formato com uma consulta de exemplo.

## Próximo passo

A 2ª rodada é a linha de base. Para otimizar o espaço com segurança, ampliar o conjunto para ~30 itens (2–3 formulações por pergunta, incluindo perguntas de glossário e de corte por cliente e gerente) e só então fazer passagens de ajuste, uma por vez.
