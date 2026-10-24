# Chaves de idempotência e marcas d'água da bronze

Como as chaves e as colunas de data da bronze foram escolhidas, e o que o perfilamento mostrou. O teste de unicidade é reproduzível na seção 1.1 de [`02_entendimento_dados`](https://github.com/lauroPereira/pmo-lakehouse/blob/main/src/00_understanding/02_entendimento_dados.py).

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


## Fora do escopo

- **Exclusões na origem** não são aplicadas na bronze (só insere), mas ficam detectáveis comparando as chaves de dois snapshots da landing.
