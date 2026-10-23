# Arquitetura

Desenho técnico do pipeline. O que é negócio (empresa, dores, escopo D1–D3, critérios de sucesso de negócio) está em [`01_entendimento_negocio`](https://github.com/lauroPereira/pmo-lakehouse/blob/main/src/understanding/01_entendimento_negocio.py); o andamento por fase do CRISP-DM está no [README](https://github.com/lauroPereira/pmo-lakehouse/blob/main/README.md#metodologia-crisp-dm).

## Tese

Respostas de IA sobre dados só valem o que vale o dado por baixo. O pipeline vai de fontes bagunçadas a dado com qualidade provada, KPIs governados e um assistente em linguagem natural (Genie) cuja resposta é **avaliada** contra números conhecidos.

## Fluxo

```
PostgreSQL (Supabase) ─┐
                       ├─ extraction ─▶ landing (Parquet) ─ ingestion ─▶ bronze ─▶ silver ─▶ gold ─▶ Genie / dashboard
MongoDB (Atlas) ───────┘   snapshot       dt_raw/lote        Auto Loader          quarentena  KPIs
```

## Camadas (Unity Catalog, serverless)

Colunas, tipos e relacionamentos de cada camada: [docs/modelo-de-dados.md](https://github.com/lauroPereira/pmo-lakehouse/blob/main/docs/modelo-de-dados.md).

| Camada | Onde | Conteúdo | Código |
|---|---|---|---|
| landing | Volume `workspace.raw.landing` | snapshot completo de cada origem em Parquet imutável (`dt_raw`, `lote`), com `_hash` e `_chave` | `src/extraction/` (PySpark: JDBC + pymongo) |
| bronze | `workspace.bronze` | Auto Loader sobre a landing; `MERGE` só de inserção por `_hash`; `_arquivo` de origem em cada linha; controles de extração e carga | `src/ingestion/` (PySpark) |
| silver | `workspace.silver` | `projetos`, `status_semanal`, `custos` e `tarefas` (modelo único das 3 ferramentas, com atributos por tarefa); regras Q1–Q13 registradas em `qualidade`; descartes e quarentena com motivo em `quarentena` | `src/transformation/` (PySpark) |
| gold | `workspace.gold` | `kpi_projeto` e `kpi_carteira`: valor agregado (SPI, CPI, EAC, estouro) e farol declarado × calculado, cada KPI definido uma vez; `avaliacao` com o histórico esperado × encontrado | `src/modeling/`, `src/evaluation/` (SQL em notebooks PySpark) |
| consumo | dashboard AI/BI | indicadores de D1–D3 sobre a gold | `src/serving/` + `resources/` |
| ia | Genie | espaço Genie sobre a gold e um conjunto de perguntas com resposta esperada | a fazer |

## Decisões técnicas

| Decisão | Motivo | Onde está documentada |
|---|---|---|
| Landing com snapshot completo | reprocessar a bronze sem tocar nas origens; detectar exclusões comparando snapshots | README, passo 8 |
| Idempotência por hash do conteúdo | as origens não têm `updated_at` nem CDC | [chaves-idempotencia.md](https://github.com/lauroPereira/pmo-lakehouse/blob/main/docs/chaves-idempotencia.md) |
| JSON cru do Mongo na bronze | não inferir schema de dado sujo; a silver interpreta | README, passo 9 |
| Tudo como texto no Postgres de origem | simular sistema legado e preservar os defeitos | README, passo 7 |
| Silver recalculada a cada execução | volume pequeno; recálculo é idempotente e simples de auditar | README, passo 10 |
| Avaliação como tarefa do job | número divergente não chega ao dashboard: o job falha | README, passo 12 |

## Critérios técnicos de sucesso

1. Job único de ponta a ponta, implantado pelo bundle (`pmo_lakehouse`). **Feito.**
2. Todos os defeitos Q1–Q13 detectados e explicados (corrigidos, em quarentena ou descartados com registro). **Feito.**
3. Conferência esperado × encontrado automática a cada carga. **Feito (15 itens).**
4. Genie acerta as perguntas de avaliação; erros documentados com causa.
