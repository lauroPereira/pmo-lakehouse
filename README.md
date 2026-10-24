# pmo-lakehouse

Saúde da entrega numa fábrica de software: lakehouse em Databricks com camada de qualidade de dados.

> 🚧 Em construção (outubro de 2026).

## O problema

Uma fábrica de software com 60 projetos de clientes controla cada um numa ferramenta diferente (Excel, ClickUp, MS Project). A diretoria não consegue dizer **quais projetos estão atrasados, quanto o atraso vai custar e em quais o status reportado não bate com a realidade**. Este é o escopo do primeiro ciclo; ociosidade de pessoas, atraso de fornecedores e previsão de risco ficam no backlog. O entendimento completo do negócio está em [`01_entendimento_negocio`](https://github.com/lauroPereira/pmo-lakehouse/blob/main/src/00_understanding/01_entendimento_negocio.py).

## Metodologia: CRISP-DM

O projeto segue o CRISP-DM: tudo parte da dor do negócio, e cada fase deixa um artefato. A arquitetura medalhão é o desenho técnico; o CRISP-DM é a condução do projeto.

| Fase | Artefato | Status |
|---|---|---|
| 1. Entendimento do negócio | [`01_entendimento_negocio`](https://github.com/lauroPereira/pmo-lakehouse/blob/main/src/00_understanding/01_entendimento_negocio.py): empresa, pessoas, sistemas, dores, escopo D1–D3 e backlog | Feito |
| 2. Entendimento dos dados | [`02_entendimento_dados`](https://github.com/lauroPereira/pmo-lakehouse/blob/main/src/00_understanding/02_entendimento_dados.py): perfil das fontes, 13 defeitos medidos (Q1–Q13) e 11 de 11 evidências da fase 1 confirmadas e [src/00_understanding/chaves-idempotencia.md](https://github.com/lauroPereira/pmo-lakehouse/blob/main/src/00_understanding/chaves-idempotencia.md) | Feito |
| 3. Preparação dos dados | Landing e bronze; silver [`silver_cadastros`](https://github.com/lauroPereira/pmo-lakehouse/blob/main/src/03_transformation/silver_cadastros.py) e [`silver_tarefas`](https://github.com/lauroPereira/pmo-lakehouse/blob/main/src/03_transformation/silver_tarefas.py): limpeza, tipos, wrangling, integração das 3 ferramentas e atributos por tarefa, com quarentena e registro de qualidade | Feito |
| 4. Modelagem | [`gold_kpis`](https://github.com/lauroPereira/pmo-lakehouse/blob/main/src/04_modeling/gold_kpis.py): valor agregado (SPI, CPI, EAC, estouro) e farol calculado, por projeto e carteira | Feito |
| 5. Avaliação | [`avaliacao`](https://github.com/lauroPereira/pmo-lakehouse/blob/main/src/05_evaluation/avaliacao.py): 15 itens esperado × encontrado a cada execução (15 de 15); perguntas de avaliação do Genie a fazer | Em andamento |
| 6. Implantação | Job [`pmo_lakehouse`](https://github.com/lauroPereira/pmo-lakehouse/blob/main/resources/pmo_lakehouse.job.yml) e dashboard [`saude_da_entrega`](https://github.com/lauroPereira/pmo-lakehouse/blob/main/src/serving/saude_da_entrega.lvdash.json) no bundle (feitos); assistente em linguagem natural (Genie) a fazer | Em andamento |

### Registro de iterações

O CRISP-DM é cíclico; estas foram as voltas até aqui.

1. **Dados antes do negócio.** O projeto começou explorando os dados da imersão e montando a ingestão. Reancorei no entendimento do negócio (fase 1) e reduzi o escopo às 3 dores principais, para entregar com mais qualidade.
2. **Bronze: snapshot → incremental → landing.** A primeira bronze copiava tudo a cada carga. O perfilamento (fase 2) mostrou chaves naturais únicas, e a carga virou incremental por hash. Depois, para poder reprocessar sem tocar nas origens, adotei uma landing zone com snapshots completos em Parquet e Auto Loader.
3. **Qualidade como requisito não funcional.** O negócio não pediu, mas o perfilamento encontrou defeitos (por exemplo, colaboradores duplicados que diferem só na caixa do nome). A qualidade virou requisito proposto pela equipe de dados.

## Arquitetura

Medalhão no Databricks Free Edition (Unity Catalog, serverless): `landing` (snapshots em Parquet) → `bronze` (dado como chegou) → `silver` (limpo e integrado, com regras de qualidade e quarentena) → `gold` (KPIs para a diretoria: SPI, CPI, estouro previsto).

## Dados

O modelo de dados de cada camada (landing, bronze, silver e gold), em diagramas entidade-relacionamento, está em [src/00_understanding/modelo-de-dados.md](https://github.com/lauroPereira/pmo-lakehouse/blob/main/src/00_understanding/modelo-de-dados.md).

Sintéticos, de uma empresa fictícia (Vértice Tecnologia) fornecida pela imersão. Nenhum dado real de clientes ou empregadores.

## Origem e créditos

O caso e os dados sintéticos da Vértice Tecnologia vêm da imersão gratuita **"Databricks + Claude Code: projeto de dados do zero"**, do canal Jornada de Dados no YouTube (Luciano Vasconcelos), usada aqui como referência. Os dados brutos do curso não estão neste repositório. A narrativa de negócio (pessoas, sistemas e dores) é uma simulação minha sobre esse caso fictício. Arquitetura, pipeline, regras de qualidade e análises são meus; o desenho está em [src/00_understanding/arquitetura.md](https://github.com/lauroPereira/pmo-lakehouse/blob/main/src/00_understanding/arquitetura.md).

## Organização do código

| Pasta | Papel |
|---|---|
| `src/00_understanding/` | Fases 1 e 2 do CRISP-DM: entendimento do negócio e dos dados |
| `src/sources/` | Simula os sistemas de origem (não faz parte do pipeline) |
| `src/01_extraction/` | Origem → landing (Parquet) |
| `src/02_ingestion/` | Landing → bronze (Auto Loader) |
| `src/03_transformation/` | Bronze → silver (fase 3: preparação dos dados) |
| `src/04_modeling/` | Silver → gold (fase 4: modelo de valor agregado) |
| `src/05_evaluation/` | Esperado × encontrado (fase 5) |
| `src/serving/` | Dashboard AI/BI sobre a gold (fase 6) |
| `resources/` | Job e dashboard do bundle (fase 6) |
| `src/diagnostics/` | Utilitários fora do fluxo (teste de conectividade) |

## Passo a passo da criação do projeto

Registro do que foi executado, em ordem.

1. **Instalação do Databricks CLI** (Windows)
   ```bash
   winget search databricks
   winget install Databricks.DatabricksCLI
   databricks --version
   ```
2. **Inicialização do projeto**
   ```bash
   databricks bundle init
   databricks aitools install --agents claude-code --scope project --skills-only --experimental
   ```
   O `bundle init` gerou o esqueleto (`databricks.yml`, `resources/`, `src/`, `tests/`). O `aitools install` instalou as skills do Databricks para o Claude Code, só neste projeto.

3. **Origens externas** (substituem os arquivos do curso como fonte)
   - **Postgres no Supabase** (plano gratuito): criar o projeto e copiar os dados do **Session pooler** em *Connect*. A conexão direta do Supabase é só IPv6 e não funciona a partir do serverless; o usuário do pooler tem a forma `postgres.<ref>`.
   - **MongoDB Atlas M0** (conta pessoal): criar o cluster e um usuário de banco. Em *Network Access*, liberar `0.0.0.0/0`, porque o IP de saída do serverless da Free Edition não é fixo.
4. **Segredos no Databricks** (escopo `pmo`). Nenhuma credencial entra no repositório; o código lê tudo com `dbutils.secrets.get("pmo", chave)`. Criados pelo CLI, com o valor digitado no prompt (o valor nunca aparece em comando, log ou commit):
   ```powershell
   databricks secrets create-scope pmo --profile <perfil>
   databricks secrets put-secret pmo pg_host --profile <perfil>
   ```
   O `put-secret` foi repetido para cada chave:

   | Chave | Conteúdo |
   |---|---|
   | `pg_host`, `pg_port`, `pg_db` | host, porta e banco do Session pooler do Supabase |
   | `pg_user`, `pg_password` | usuário (`postgres.<ref>`) e senha |
   | `mongo_user`, `mongo_password` | usuário e senha do Atlas |
   | `mongo_host` | host do cluster, sem esquema (`<cluster>.<id>.mongodb.net`) |

   Conferir sem expor valores: `databricks secrets list-secrets pmo --profile <perfil>`.
5. **Teste de conectividade**: o notebook [`src/diagnostics/teste_conectividade.py`](https://github.com/lauroPereira/pmo-lakehouse/blob/main/src/diagnostics/teste_conectividade.py) testa TCP e login no Postgres, `ping` no Mongo e dois hosts de controle. Foi importado no workspace e executado como run serverless (`databricks workspace import` + `databricks jobs submit`). **Resultado: a Free Edition alcança as duas origens.**

6. **Dados brutos no Volume** (fora do git). Os arquivos do curso sobem para `workspace.raw.material`, que a carga lê:
   ```powershell
   databricks schemas create raw workspace --profile <perfil>
   databricks volumes create workspace raw material MANAGED --profile <perfil>
   databricks fs cp <pasta-dados>\clickup dbfs:/Volumes/workspace/raw/material/clickup --recursive --profile <perfil>
   ```
   (repetido para `excel`, `msproject`, `pmo`, `rh`, `financeiro`, `timesheet` e `compras`).
   Também são criados os volumes `workspace.raw.landing` (snapshots em Parquet) e `workspace.raw.checkpoints` (estado do Auto Loader), com `databricks volumes create workspace raw <nome> MANAGED`.
7. **Carga das origens**: o notebook [`src/sources/carregar_origens.py`](https://github.com/lauroPereira/pmo-lakehouse/blob/main/src/sources/carregar_origens.py) lê o Volume e popula as duas origens usando os segredos do passo 4. É idempotente (recria tabelas e esvazia coleções) e **não limpa nada**: os defeitos de dado ficam na origem, para a bronze e a silver tratarem.

   | Origem | Destino | Conteúdo | Registros |
   |---|---|---|---|
   | Postgres | `pmo.projetos` | projetos | 60 |
   | Postgres | `pmo.status_semanal` | status semanal com farol | 3.911 |
   | Postgres | `rh.colaboradores` | colaboradores | 523 |
   | Postgres | `financeiro.custos` | lançamentos de custo | 23.640 |
   | Postgres | `timesheet.apontamentos` | horas disponíveis × apontadas | 30.883 |
   | Postgres | `compras.entregas_fornecedor` | entregas de fornecedores | 640 |
   | Mongo `vertice` | `clickup_listas` | um export de lista por documento | 25 |
   | Mongo `vertice` | `msproject_projetos` | um XML por documento | 15 |
   | Mongo `vertice` | `excel_cronogramas` | uma linha de cronograma por documento | 1.663 |

   Conferência: as contagens do Postgres batem com as linhas dos CSVs (sem o cabeçalho). Todas as colunas do Postgres são `text` de propósito (sistema legado sem tipos), e cada documento do Mongo leva `_arquivo` e `_carregado_em` para rastreio. Para ver as tabelas no painel do Supabase, troque o seletor de schema (padrão `public`) para `pmo`, `rh`, `financeiro`, `timesheet` ou `compras`. Escolha de desenho: ClickUp, MS Project e Excel ficam juntos no Mongo porque são as três "ferramentas de gerente" do caso, cada uma com um modelo diferente.

8. **Landing zone** ([`src/01_extraction/extrair_para_landing.py`](https://github.com/lauroPereira/pmo-lakehouse/blob/main/src/01_extraction/extrair_para_landing.py)): lê cada origem **por inteiro** e grava um **snapshot completo** em Parquet no Volume `workspace.raw.landing`, em `<fonte>/<tabela>/dt_raw=AAAA-MM-DD/lote_<id>/`. Cada execução é uma fotografia da origem naquele momento, mesmo quando nada mudou; os arquivos são imutáveis.
   - Postgres via JDBC e Mongo via pymongo (o conector Spark do Mongo não está disponível no serverless).
   - Cada linha leva `_hash` (SHA-256 do conteúdo; no Mongo, do documento sem `_id` e `_carregado_em`, que mudam a cada recarga), `_chave` (chave natural), `_extraido_em` e `_lote_id`.
   - `bronze._controle_extracao` compara linhas lidas na origem × linhas gravadas na landing.
   - **Por que snapshot completo:** a landing vira uma cópia auditável e reprocessável, sem depender de marca d'água; exclusões na origem passam a ser detectáveis comparando as chaves de dois snapshots. O custo é armazenamento (cada snapshot repete a origem, poucos MB aqui); para tabelas grandes eu voltaria ao incremental ou CDC.
9. **Bronze** ([`src/02_ingestion/ingestao_bronze.py`](https://github.com/lauroPereira/pmo-lakehouse/blob/main/src/02_ingestion/ingestao_bronze.py)): lê a landing com **Auto Loader** (`cloudFiles`, só arquivos novos, via checkpoint) e grava em `workspace.bronze` (`pg_<schema>_<tabela>`, `mongo_<coleção>`). O notebook é organizado em 7 etapas (configuração, fontes declaradas, leitura, enriquecimento, gravação, conferência e registro), uma função por etapa; incluir uma fonte nova é acrescentar uma linha na lista de fontes.
   - Cada linha leva `_arquivo` (o Parquet que a originou), `dt_raw`, `_origem`, `_carregado_em`, `_hash`, `_chave`, `_extraido_em` e `_lote_id`.
   - **Idempotência em dois níveis:** por arquivo (checkpoint do Auto Loader) e por conteúdo (`MERGE` só de inserção por `_hash`). Ler um snapshot repetido não duplica nada; linha alterada entra como nova versão, com histórico. A silver escolhe a versão vigente por `_chave`.
   - **Chaves naturais:** método e resultados em [src/00_understanding/chaves-idempotencia.md](https://github.com/lauroPereira/pmo-lakehouse/blob/main/src/00_understanding/chaves-idempotencia.md), reproduzíveis na seção 1.1 de [`02_entendimento_dados`](https://github.com/lauroPereira/pmo-lakehouse/blob/main/src/00_understanding/02_entendimento_dados.py). Únicas em todas as tabelas, exceto `colaborador_id` (3 duplicados que diferem só na caixa do nome; seguem na bronze e a silver normaliza).
   - `bronze._controle_carga`: arquivos novos, linhas inseridas e a conferência de que todo hash da landing está na bronze; o notebook falha se algum faltar.

   **Evidência:**

   | Execução | Linhas inseridas na bronze | Observação |
   |---|---|---|
   | 1ª extração + bronze | 61.360 | 9 de 9 tabelas conferem |
   | 2ª extração, sem mudança na origem | 0 | 1 arquivo novo por tabela, nenhuma linha nova |
   | 3ª, após [`simular_mudancas.py`](https://github.com/lauroPereira/pmo-lakehouse/blob/main/src/sources/simular_mudancas.py) | 5 | 2 custos novos, 1 projeto alterado, 1 linha Excel nova, 1 lista ClickUp alterada; cada linha aponta para o `_arquivo` do snapshot que a trouxe |
   | Replay: bronze apagada e reconstruída só da landing | 61.365 | mesma contagem, sem tocar nas origens |

   Depois da simulação, landing, bronze e checkpoints foram apagados e refeitos com as origens restauradas, para não deixar linhas simuladas.

10. **Silver** (fase 3 do CRISP-DM), recalculada a cada execução a partir da versão vigente da bronze:
    - Organização dos notebooks: como na bronze, cada um tem etapas nomeadas e declara o que muda em listas — `Preparacao` e `Regra` em `silver_cadastros`, `Ferramenta` em `silver_tarefas`, `Etapa` em `gold_kpis` e `Checagem` em `avaliacao`. Incluir uma regra, uma ferramenta, um KPI ou uma checagem é acrescentar um item na lista.
    - [`silver_cadastros`](https://github.com/lauroPereira/pmo-lakehouse/blob/main/src/03_transformation/silver_cadastros.py): `silver.projetos` (ferramenta normalizada, datas nos 2 formatos), `silver.status_semanal` (farol normalizado e validado) e `silver.custos` (vírgula decimal corrigida, estorno sinalizado).
    - [`silver_tarefas`](https://github.com/lauroPereira/pmo-lakehouse/blob/main/src/03_transformation/silver_tarefas.py): `silver.tarefas`, o modelo único das 3 ferramentas (4.917 tarefas). Limpeza (TOTAL e tarefas-resumo descartadas), padronização de tipos (datas, % e horas em vários formatos), wrangling (JSON aberto; a fase do MS Project herdada da tarefa-resumo), integração (ClickUp ligado ao cadastro pelo código ou pelo nome) e atributos por tarefa (fração planejada, horas agregadas e planejadas, desvio, atraso).
    - Qualidade: cada regra Q1–Q13 grava quantas linhas corrigiu, sinalizou, descartou ou pôs em quarentena em `silver.qualidade`; o que foi descartado ou ficou em quarentena vai para `silver.quarentena` com o motivo e o registro original.
11. **Gold** (fase 4): [`gold_kpis`](https://github.com/lauroPereira/pmo-lakehouse/blob/main/src/04_modeling/gold_kpis.py) cria `gold.kpi_projeto` (BAC, PV, EV, AC, SPI, CPI, EAC, VAC, estouro, situação, farol declarado × calculado, melancia) e `gold.kpi_carteira`.
12. **Avaliação** (fase 5): [`avaliacao`](https://github.com/lauroPereira/pmo-lakehouse/blob/main/src/05_evaluation/avaliacao.py) confere 15 itens (D1, D2, D3 e o requisito de qualidade) contra os valores de referência, grava `gold.avaliacao` e falha o job se algum divergir.
13. **Job** (fase 6): [`resources/pmo_lakehouse.job.yml`](https://github.com/lauroPereira/pmo-lakehouse/blob/main/resources/pmo_lakehouse.job.yml) encadeia extração → bronze → silver_cadastros → silver_tarefas → gold → avaliação, no serverless, agendado para dias úteis às 6h (pausado no alvo `dev`).

14. **Dashboard** (fase 6): [`saude_da_entrega.lvdash.json`](https://github.com/lauroPereira/pmo-lakehouse/blob/main/src/serving/saude_da_entrega.lvdash.json), implantado pelo bundle ([`resources/saude_da_entrega.dashboard.yml`](https://github.com/lauroPereira/pmo-lakehouse/blob/main/resources/saude_da_entrega.dashboard.yml)) no SQL warehouse serverless. Quatro indicadores (projetos em andamento, críticos, estouro previsto e melancias), os 10 maiores estouros (D2), situação calculada × farol do gerente (D3) e a tabela de projetos com SPI e CPI coloridos (D1).

### Armadilhas encontradas

- **Git Bash apaga as barras da URI.** Gravar `mongodb+srv://...` como segredo pelo Git Bash perdeu o `//` e a `/` final (o `pymongo` acusou "Reserved characters such as ':'"). Solução: guardar usuário, senha e host em segredos separados, sem barras, e montar a URI no código com `quote_plus`.
- **Atlas sem liberação de IP** não devolve erro de rede: o handshake TLS cai com `tlsv1 alert internal error`. Resolvido ao liberar `0.0.0.0/0`.
- **Supabase direto = IPv6**; usar o Session pooler.
- **Links relativos quebram no workspace.** No deploy do bundle, notebooks `.py` perdem a extensão e o visualizador de Markdown do Databricks resolve links relativos a partir da raiz do host (404). Os links dos `.md` apontam para o GitHub, que funciona nos dois lugares.
- **`bundle deploy` não desfaz edições feitas no workspace.** Ele só envia o que mudou localmente; para sobrescrever uma edição feita direto na pasta do bundle, use `databricks bundle sync --full`. Edite sempre no repositório.
- **Contadores dentro do `foreachBatch` não voltam no serverless** (a função roda no servidor). A contagem de arquivos processados vem de `cloud_files_state(checkpoint)`.
- **ANSI ligado no serverless:** `cast` inválido e divisão por zero geram erro em vez de nulo; a silver usa `try_cast`, `try_to_timestamp` e `try_divide`.
- **Soma de `double` varia no último dígito entre execuções** (a ordem de soma muda): `gold.kpi_carteira` pode trazer `1.6100521125E8` ou `1.6100521125000003E8`. Comparações de regressão devem usar tolerância ou arredondar.
- **`collect_list` não garante ordem**, mesmo depois de um `order by` na subconsulta; a avaliação monta textos com agregações explícitas.
- **`pymongo[srv]`** não existe como extra na versão 4.10; basta `pymongo` (traz `dnspython`).

## Como rodar

Projeto no formato Declarative Automation Bundle (`databricks.yml`). Requer o Databricks CLI autenticado e as origens e segredos dos passos 3 e 4 (e, uma vez, `sources/carregar_origens` para popular as origens).

```bash
databricks bundle validate
databricks bundle deploy -t dev
databricks bundle run pmo_lakehouse -t dev
```

O job roda extração → bronze → silver → gold → avaliação; o deploy também publica o dashboard "Saúde da entrega". Os notebooks `understanding/01_entendimento_negocio` e `understanding/02_entendimento_dados` documentam as fases 1 e 2 e podem ser abertos a qualquer momento.
