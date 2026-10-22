# Chaves de idempotência e marcas d'água da bronze

Como as chaves e as colunas de data da bronze foram escolhidas, e o que o perfilamento mostrou. O teste de unicidade é reproduzível na seção 1.1 de [`02_entendimento_dados`](https://github.com/lauroPereira/pmo-lakehouse/blob/main/src/understanding/02_entendimento_dados.py). A análise das colunas de marca d'água foi feita quando a extração era incremental ([commit 08079f7](https://github.com/lauroPereira/pmo-lakehouse/tree/08079f7)) e fica aqui como registro.

## Método

1. **Candidata = identificador de negócio** de cada tabela (coluna `*_id`; para tabelas semanais, o par projeto + data; no timesheet, colaborador + projeto + semana).
2. **Teste de unicidade:** `count(*)` contra `count(distinct chave)`. Uma chave só é adotada como `_chave` natural se não houver duplicadas, ou se as duplicadas forem um defeito já conhecido, tratado na silver.
3. **Marca d'água (avaliada e não usada):** a extração acabou sendo snapshot completo, então não há leitura incremental; a análise abaixo fica como base para tabelas grandes, onde a marca d'água volta a valer. Critério: só em tabelas de fatos (acrescentam linhas com o tempo). A coluna de data precisa ter zero nulos e **formato ISO** (`AAAA-MM-DD`), para a comparação textual `data >= '2026-09-18'` ser correta.
4. **Mongo:** o `_id` (ObjectId) foi descartado como chave, porque `carregar_origens` apaga e reinsere os documentos e cada recarga gera `_id`s novos. Usa-se uma chave de negócio extraída do documento.
5. **A idempotência em si não depende da chave natural**: ela vem do `_hash` (SHA-256 do conteúdo). A `_chave` natural serve para a silver escolher a versão vigente entre versões do mesmo registro.

## Resultado: unicidade

| Tabela | Chave | Linhas | Chaves distintas | Duplicadas |
|---|---|---|---|---|
| `pg_pmo_projetos` | `projeto_id` | 60 | 60 | 0 |
| `pg_pmo_status_semanal` | `projeto_id` + `data_status` | 3.911 | 3.911 | 0 |
| `pg_rh_colaboradores` | `colaborador_id` | 523 | 520 | **3** |
| `pg_financeiro_custos` | `lancamento_id` | 23.640 | 23.640 | 0 |
| `pg_timesheet_apontamentos` | `colaborador_id` + `projeto_id` + `semana_inicio` | 30.883 | 30.883 | 0 |
| `pg_compras_entregas_fornecedor` | `entrega_id` | 640 | 640 | 0 |
| `mongo_clickup_listas` | `list.id` | 25 | 25 | 0 |
| `mongo_msproject_projetos` | `_arquivo` | 15 | 15 | 0 |
| `mongo_excel_cronogramas` | `_arquivo` + `_linha` | 1.663 | 1.663 | 0 |

### O único caso com duplicadas: `colaborador_id`

São 3 ids (COL0102, COL0257, COL0433), cada um com 2 linhas **com conteúdo diferente** (2 versões distintas por hash). A diferença é a caixa do nome: `Renata Monteiro` e `RENATA MONTEIRO`, e assim por diante. Consequências:
- A bronze guarda as duas versões, como deve (dado como chegou).
- A chave natural sozinha não escolhe a versão vigente: a silver precisa **normalizar o nome** antes de deduplicar por `colaborador_id`. Fica como regra de qualidade da silver, com os 3 casos como valor esperado.

## Resultado: colunas candidatas a marca d'água (não usadas no snapshot completo)

| Tabela | Coluna | Nulos | Fora do ISO | Mínimo | Máximo |
|---|---|---|---|---|---|
| `pg_pmo_status_semanal` | `data_status` | 0 | 0 | 2024-12-06 | 2026-10-16 |
| `pg_financeiro_custos` | `data` | 0 | 0 | 2024-12-09 | 2026-10-16 |
| `pg_timesheet_apontamentos` | `semana_inicio` | 0 | 0 | 2024-12-02 | 2026-10-12 |
| `pg_compras_entregas_fornecedor` | `data_pedido` | 0 | 0 | 2025-01-21 | 2026-10-16 |

As quatro colunas atendem aos critérios. Para `entregas`, a escolha foi `data_pedido` (sempre preenchida) em vez de `data_entrega`, que fica vazia enquanto a entrega está aberta. Se a extração virar incremental, uma janela de 28 dias reler as últimas semanas para pegar lançamentos retroativos; o `MERGE` por hash garante que reler não duplica.

## Fora do escopo

- **Exclusões na origem** não são aplicadas na bronze (só insere), mas ficam detectáveis comparando as chaves de dois snapshots da landing.
