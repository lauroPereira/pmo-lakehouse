# Modelo de dados por camada

Diagramas entidade-relacionamento de cada camada do lakehouse, gerados a partir do `information_schema` do Unity Catalog (catálogo `workspace`). Os diagramas usam [Mermaid](https://mermaid.js.org/), que o GitHub renderiza; no visualizador de Markdown do Databricks eles aparecem como texto.

Convenções:
- Colunas começando com `_` são **metadados técnicos** (rastreio, idempotência, auditoria), não dados de negócio.
- Os relacionamentos são **lógicos**: o Delta não impõe chave estrangeira. A integridade é verificada pelo pipeline (quarentena na silver e avaliação na gold).
- Na landing e na bronze, todas as colunas de negócio do Postgres são `string`, como vieram da origem; a tipagem acontece na silver.

## Visão geral

```mermaid
flowchart LR
    subgraph Origens
        PG[(PostgreSQL<br/>Supabase)]
        MG[(MongoDB<br/>Atlas)]
    end
    subgraph Landing["landing (Volume, Parquet)"]
        L[snapshot completo<br/>dt_raw / lote]
    end
    subgraph Bronze
        B[9 tabelas de origem<br/>+ 2 de controle]
    end
    subgraph Silver
        S[projetos · status_semanal<br/>custos · tarefas<br/>+ qualidade · quarentena]
    end
    subgraph Gold
        G[kpi_projeto · kpi_carteira<br/>+ avaliacao]
    end
    PG --> L
    MG --> L
    L --> B --> S --> G
```

---

## Landing (`/Volumes/workspace/raw/landing`)

Arquivos Parquet imutáveis, um diretório por origem e por snapshot: `<fonte>/<tabela>/dt_raw=AAAA-MM-DD/lote_<id>/part-*.parquet`. Cada arquivo tem as colunas da origem mais quatro colunas técnicas acrescentadas na extração.

| Coluna técnica | Tipo | Significado |
|---|---|---|
| `_hash` | string | SHA-256 do conteúdo da linha (no Mongo, do documento sem `_id` e `_carregado_em`) |
| `_chave` | string | chave natural do registro (ver [chaves-idempotencia.md](https://github.com/lauroPereira/pmo-lakehouse/blob/main/docs/chaves-idempotencia.md)) |
| `_extraido_em` | timestamp | instante da extração (desempata snapshots do mesmo dia) |
| `_lote_id` | string | identificador do snapshot |
| `dt_raw` | date | partição do diretório: data da extração |

O modelo lógico da landing é o modelo da origem:

```mermaid
erDiagram
    PMO_PROJETOS ||--o{ PMO_STATUS_SEMANAL : "projeto_id"
    PMO_PROJETOS ||--o{ FINANCEIRO_CUSTOS : "projeto_id"
    PMO_PROJETOS ||--o{ TIMESHEET_APONTAMENTOS : "projeto_id"
    PMO_PROJETOS ||--o{ COMPRAS_ENTREGAS_FORNECEDOR : "projeto_id"
    RH_COLABORADORES ||--o{ TIMESHEET_APONTAMENTOS : "colaborador_id"
    PMO_PROJETOS ||--o| MONGO_CLICKUP_LISTAS : "codigo_ferramenta = list.id"
    PMO_PROJETOS ||--o| MONGO_MSPROJECT_PROJETOS : "PRJ-xxx no nome do arquivo"
    PMO_PROJETOS ||--o{ MONGO_EXCEL_CRONOGRAMAS : "PRJ-xxx no nome do arquivo"

    PMO_PROJETOS {
        string projeto_id PK
        string nome_projeto
        string cliente
        string segmento
        string ferramenta
        string codigo_ferramenta
        string valor_contrato
        string orcamento_custo
        string data_inicio
        string data_fim_baseline
        string status
        string gerente_projeto
    }
    PMO_STATUS_SEMANAL {
        string projeto_id PK, FK
        string data_status PK
        string gerente
        string farol
        string pct_concluido
        string texto_status
    }
    FINANCEIRO_CUSTOS {
        string lancamento_id PK
        string projeto_id FK
        string data
        string categoria
        string descricao
        string valor
        string centro_custo
    }
    TIMESHEET_APONTAMENTOS {
        string colaborador_id PK, FK
        string projeto_id PK, FK
        string semana_inicio PK
        string horas_disponiveis
        string horas_apontadas
    }
    COMPRAS_ENTREGAS_FORNECEDOR {
        string entrega_id PK
        string projeto_id FK
        string fornecedor
        string item
        string valor
        string data_pedido
        string data_prometida
        string data_entrega
        string prazo_contratado_dias
        string status
    }
    RH_COLABORADORES {
        string colaborador_id PK "não é única: 3 duplicados"
        string nome
        string papel
        string senioridade
        string custo_hora
        string data_admissao
        string data_desligamento
    }
    MONGO_CLICKUP_LISTAS {
        string _chave PK "list.id"
        string documento "JSON: list, tasks[]"
    }
    MONGO_MSPROJECT_PROJETOS {
        string _chave PK "nome do arquivo XML"
        string documento "JSON: projeto.Tasks.Task[]"
    }
    MONGO_EXCEL_CRONOGRAMAS {
        string _chave PK "arquivo | linha"
        string documento "JSON: uma linha da planilha"
    }
```

---

## Bronze (`workspace.bronze`)

Mesmas entidades e relacionamentos da landing, com o nome `pg_<schema>_<tabela>` ou `mongo_<coleção>`, as colunas de negócio idênticas (`string`) e os metadados abaixo em todas as tabelas. Guarda **versões**: linha alterada na origem entra como linha nova; a versão vigente é a de `_extraido_em` mais recente por `_chave`.

| Coluna técnica | Tipo | Origem |
|---|---|---|
| `_hash`, `_chave`, `_extraido_em`, `_lote_id`, `dt_raw` | — | vêm da landing |
| `_arquivo` | string | Parquet que originou a linha (`_metadata.file_path` do Auto Loader) |
| `_carregado_em` | timestamp | instante da carga na bronze |
| `_origem` | string | nome da tabela bronze |
| `_rescued_data` | string | coluna de resgate do Auto Loader: dado que não coube no schema (vazia hoje) |

Tabelas de controle da carga:

```mermaid
erDiagram
    CONTROLE_EXTRACAO {
        string origem
        string pasta_landing
        long linhas_lidas
        long linhas_gravadas
        boolean confere "lidas = gravadas"
        timestamp _extraido_em
        string _lote_id
    }
    CONTROLE_CARGA {
        string tabela_bronze
        long arquivos_novos
        long linhas_antes
        long linhas_depois
        long linhas_inseridas
        long hashes_ausentes
        boolean confere "nenhum hash da landing faltando"
        timestamp _carregado_em
        string _lote_id
    }
```

---

## Silver (`workspace.silver`)

Só as fontes do escopo D1–D3, tipadas, limpas e integradas. As três ferramentas de cronograma viram uma entidade só, `tarefas`.

```mermaid
erDiagram
    PROJETOS ||--o{ STATUS_SEMANAL : "projeto_id"
    PROJETOS ||--o{ CUSTOS : "projeto_id"
    PROJETOS ||--o{ TAREFAS : "projeto_id"

    PROJETOS {
        string projeto_id PK
        string nome_projeto
        string cliente
        string segmento
        string tipo_projeto
        string ferramenta "excel | clickup | msproject"
        string codigo_ferramenta
        boolean sem_codigo_ferramenta
        decimal valor_contrato
        decimal orcamento_custo "BAC"
        date data_inicio
        date data_fim_baseline
        string status "em_andamento | concluido"
        string gerente
    }
    STATUS_SEMANAL {
        string projeto_id PK, FK
        date data_status PK
        string gerente
        string farol "verde | amarelo | vermelho"
        int pct_concluido_declarado
        string texto_status
        boolean texto_vazio
    }
    CUSTOS {
        string lancamento_id PK
        string projeto_id FK
        date data
        string categoria
        string descricao
        string centro_custo
        decimal valor
        boolean estorno
    }
    TAREFAS {
        string tarefa_id PK
        string projeto_id FK
        string ferramenta
        string tarefa
        string fase
        string responsavel
        string status_tarefa
        date inicio_baseline
        date fim_baseline
        date inicio_real
        date fim_real
        double horas_previstas
        double pct_concluido "0 a 100"
        boolean pct_acima_de_100_na_origem
        date data_corte
        int duracao_baseline_dias
        double fracao_planejada "0 a 1"
        double pct_planejado
        double horas_agregadas "horas × % concluído"
        double horas_planejadas "horas × fração planejada"
        double desvio_pp
        boolean atrasada
        boolean concluida
    }
```

Todas as tabelas acima têm `_chave` (chave natural da bronze, exceto `tarefas`) e `_processado_em`. Tabelas de qualidade, ligadas às demais pelo nome da tabela:

```mermaid
erDiagram
    QUALIDADE {
        string tabela "projetos | status_semanal | custos | tarefas"
        string regra "Q1 a Q13"
        string descricao
        string tratamento "corrigido | sinalizado | descartado | quarentena"
        long ocorrencias
        timestamp _processado_em
        string _lote
    }
    QUARENTENA {
        string tabela
        string chave "chave do registro na tabela"
        string motivo
        string tratamento "descartado | quarentena"
        string registro "JSON do registro original"
        timestamp _processado_em
        string _lote
    }
```

---

## Gold (`workspace.gold`)

```mermaid
erDiagram
    SILVER_PROJETOS ||--o| KPI_PROJETO : "projeto_id (só em andamento)"
    KPI_PROJETO }o--|| KPI_CARTEIRA : "agregado na data de corte"

    SILVER_PROJETOS {
        string projeto_id PK
    }
    KPI_PROJETO {
        string projeto_id PK, FK
        string cliente
        string segmento
        string ferramenta
        string gerente
        date data_corte
        long tarefas
        long tarefas_atrasadas
        double bac "orçamento"
        double pv "valor planejado"
        double ev "valor agregado"
        double ac "custo real"
        double pct_concluido_calculado
        double pct_planejado
        double spi "EV / PV"
        double cpi "EV / AC"
        double sv "EV - PV"
        double cv "EV - AC"
        double eac "BAC / CPI"
        double etc "EAC - AC"
        double vac "BAC - EAC"
        double estouro_previsto "max(0, EAC - BAC)"
        string situacao "crítico | atenção | ok"
        string farol_declarado
        int pct_concluido_declarado
        string farol_calculado
        boolean melancia
    }
    KPI_CARTEIRA {
        date data_corte PK
        long projetos_em_andamento
        double orcamento_total
        double custo_real_total
        double estouro_previsto_total
        long projetos_criticos
        long projetos_em_atencao
        long projetos_melancia
        string maior_estouro_projeto
        double maior_estouro_valor
    }
    AVALIACAO {
        string escopo "D1 | D2 | D3 | RNF"
        string item
        string esperado
        string encontrado
        boolean confere
        timestamp _executado_em
    }
```

`avaliacao` não se relaciona por chave: guarda, a cada execução do job, a conferência esperado × encontrado sobre a silver e a gold.
