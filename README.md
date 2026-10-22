# pmo-lakehouse

Saúde da entrega numa fábrica de software: lakehouse em Databricks com camada de qualidade de dados.

> 🚧 Em construção (outubro de 2026).

## O problema

Uma fábrica de software com 60 projetos de clientes controla cada um numa ferramenta diferente (Excel, ClickUp, MS Project). A diretoria não consegue dizer **quais projetos estão atrasados, quanto o atraso vai custar e em quais o status reportado não bate com a realidade**. Este é o escopo do primeiro ciclo; ociosidade de pessoas, atraso de fornecedores e previsão de risco ficam no backlog. O entendimento completo do negócio está em [`01_entendimento_negocio`](src/understanding/01_entendimento_negocio.py).

## Metodologia: CRISP-DM

O projeto segue o CRISP-DM: tudo parte da dor do negócio, e cada fase deixa um artefato. A arquitetura medalhão é o desenho técnico; o CRISP-DM é a condução do projeto.

| Fase | Artefato | Status |
|---|---|---|
| 1. Entendimento do negócio | [`01_entendimento_negocio`](src/understanding/01_entendimento_negocio.py): empresa, pessoas, sistemas, dores, escopo D1–D3 e backlog | Feito |
| 2. Entendimento dos dados | [`perfil_chaves`](src/understanding/perfil_chaves.py) e [docs/chaves-idempotencia.md](docs/chaves-idempotencia.md); confirmação das evidências da fase 1 | Em andamento |
| 3. Preparação dos dados | Landing, bronze (feitas) e silver com quarentena | Em andamento |
| 4. Modelagem | Gold: valor agregado (SPI, CPI, EAC) e regra do farol | A fazer |
| 5. Avaliação | Critérios de sucesso da fase 1; perguntas de avaliação do Genie | A fazer |
| 6. Implantação | Job no bundle, dashboard e assistente em linguagem natural | A fazer |

### Registro de iterações

O CRISP-DM é cíclico; estas foram as voltas até aqui.

1. **Dados antes do negócio.** O projeto começou explorando os dados da imersão e montando a ingestão. Reancorei no entendimento do negócio (fase 1) e reduzi o escopo às 3 dores principais, para entregar com mais qualidade.
2. **Bronze: snapshot → incremental → landing.** A primeira bronze copiava tudo a cada carga. O perfilamento (fase 2) mostrou chaves naturais únicas, e a carga virou incremental por hash. Depois, para poder reprocessar sem tocar nas origens, adotei uma landing zone com snapshots completos em Parquet e Auto Loader.
3. **Qualidade como requisito não funcional.** O negócio não pediu, mas o perfilamento encontrou defeitos (por exemplo, colaboradores duplicados que diferem só na caixa do nome). A qualidade virou requisito proposto pela equipe de dados.

## Arquitetura

Medalhão no Databricks Free Edition (Unity Catalog, serverless): `landing` (snapshots em Parquet) → `bronze` (dado como chegou) → `silver` (limpo e integrado, com regras de qualidade e quarentena) → `gold` (KPIs para a diretoria: SPI, CPI, estouro previsto).

## Dados

Sintéticos, de uma empresa fictícia (Vértice Tecnologia) fornecida pela imersão. Nenhum dado real de clientes ou empregadores.

## Origem e créditos

O caso e os dados sintéticos da Vértice Tecnologia vêm da imersão gratuita **"Databricks + Claude Code: projeto de dados do zero"**, do canal Jornada de Dados no YouTube (Luciano Vasconcelos), usada aqui como referência. Os dados brutos do curso não estão neste repositório. A narrativa de negócio (pessoas, sistemas e dores) é uma simulação minha sobre esse caso fictício. Arquitetura, pipeline, regras de qualidade e análises são meus; o desenho está em [docs/arquitetura.md](docs/arquitetura.md).

## Organização do código

| Pasta | Papel |
|---|---|
| `src/understanding/` | Fases 1 e 2 do CRISP-DM: entendimento do negócio e dos dados |
| `src/sources/` | Simula os sistemas de origem (não faz parte do pipeline) |
| `src/extraction/` | Origem → landing (Parquet) |
| `src/ingestion/` | Landing → bronze (Auto Loader) |
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
5. **Teste de conectividade**: o notebook [`src/diagnostics/teste_conectividade.py`](src/diagnostics/teste_conectividade.py) testa TCP e login no Postgres, `ping` no Mongo e dois hosts de controle. Foi importado no workspace e executado como run serverless (`databricks workspace import` + `databricks jobs submit`). **Resultado: a Free Edition alcança as duas origens.**

6. **Dados brutos no Volume** (fora do git). Os arquivos do curso sobem para `workspace.raw.material`, que a carga lê:
   ```powershell
   databricks schemas create raw workspace --profile <perfil>
   databricks volumes create workspace raw material MANAGED --profile <perfil>
   databricks fs cp <pasta-dados>\clickup dbfs:/Volumes/workspace/raw/material/clickup --recursive --profile <perfil>
   ```
   (repetido para `excel`, `msproject`, `pmo`, `rh`, `financeiro`, `timesheet` e `compras`).
   Também são criados os volumes `workspace.raw.landing` (snapshots em Parquet) e `workspace.raw.checkpoints` (estado do Auto Loader), com `databricks volumes create workspace raw <nome> MANAGED`.
7. **Carga das origens**: o notebook [`src/sources/carregar_origens.py`](src/sources/carregar_origens.py) lê o Volume e popula as duas origens usando os segredos do passo 4. É idempotente (recria tabelas e esvazia coleções) e **não limpa nada**: os defeitos de dado ficam na origem, para a bronze e a silver tratarem.

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

8. **Landing zone** ([`src/extraction/extrair_para_landing.py`](src/extraction/extrair_para_landing.py)): lê cada origem **por inteiro** e grava um **snapshot completo** em Parquet no Volume `workspace.raw.landing`, em `<fonte>/<tabela>/dt_raw=AAAA-MM-DD/lote_<id>/`. Cada execução é uma fotografia da origem naquele momento, mesmo quando nada mudou; os arquivos são imutáveis.
   - Postgres via JDBC e Mongo via pymongo (o conector Spark do Mongo não está disponível no serverless).
   - Cada linha leva `_hash` (SHA-256 do conteúdo; no Mongo, do documento sem `_id` e `_carregado_em`, que mudam a cada recarga), `_chave` (chave natural), `_extraido_em` e `_lote_id`.
   - `bronze._controle_extracao` compara linhas lidas na origem × linhas gravadas na landing.
   - **Por que snapshot completo:** a landing vira uma cópia auditável e reprocessável, sem depender de marca d'água; exclusões na origem passam a ser detectáveis comparando as chaves de dois snapshots. O custo é armazenamento (cada snapshot repete a origem, poucos MB aqui); para tabelas grandes eu voltaria ao incremental ou CDC.
9. **Bronze** ([`src/ingestion/ingestao_bronze.py`](src/ingestion/ingestao_bronze.py)): lê a landing com **Auto Loader** (`cloudFiles`, só arquivos novos, via checkpoint) e grava em `workspace.bronze` (`pg_<schema>_<tabela>`, `mongo_<coleção>`).
   - Cada linha leva `_arquivo` (o Parquet que a originou), `dt_raw`, `_origem`, `_carregado_em`, `_hash`, `_chave`, `_extraido_em` e `_lote_id`.
   - **Idempotência em dois níveis:** por arquivo (checkpoint do Auto Loader) e por conteúdo (`MERGE` só de inserção por `_hash`). Ler um snapshot repetido não duplica nada; linha alterada entra como nova versão, com histórico. A silver escolhe a versão vigente por `_chave`.
   - **Chaves naturais:** método e resultados em [docs/chaves-idempotencia.md](docs/chaves-idempotencia.md), reproduzíveis com [`perfil_chaves.py`](src/understanding/perfil_chaves.py). Únicas em todas as tabelas, exceto `colaborador_id` (3 duplicados que diferem só na caixa do nome; seguem na bronze e a silver normaliza).
   - `bronze._controle_carga`: arquivos novos, linhas inseridas e a conferência de que todo hash da landing está na bronze; o notebook falha se algum faltar.

   **Evidência:**

   | Execução | Linhas inseridas na bronze | Observação |
   |---|---|---|
   | 1ª extração + bronze | 61.360 | 9 de 9 tabelas conferem |
   | 2ª extração, sem mudança na origem | 0 | 1 arquivo novo por tabela, nenhuma linha nova |
   | 3ª, após [`simular_mudancas.py`](src/sources/simular_mudancas.py) | 5 | 2 custos novos, 1 projeto alterado, 1 linha Excel nova, 1 lista ClickUp alterada; cada linha aponta para o `_arquivo` do snapshot que a trouxe |
   | Replay: bronze apagada e reconstruída só da landing | 61.365 | mesma contagem, sem tocar nas origens |

   Depois da simulação, landing, bronze e checkpoints foram apagados e refeitos com as origens restauradas, para não deixar linhas simuladas.

### Armadilhas encontradas

- **Git Bash apaga as barras da URI.** Gravar `mongodb+srv://...` como segredo pelo Git Bash perdeu o `//` e a `/` final (o `pymongo` acusou "Reserved characters such as ':'"). Solução: guardar usuário, senha e host em segredos separados, sem barras, e montar a URI no código com `quote_plus`.
- **Atlas sem liberação de IP** não devolve erro de rede: o handshake TLS cai com `tlsv1 alert internal error`. Resolvido ao liberar `0.0.0.0/0`.
- **Supabase direto = IPv6**; usar o Session pooler.
- **`pymongo[srv]`** não existe como extra na versão 4.10; basta `pymongo` (traz `dnspython`).

## Como rodar

Projeto no formato Declarative Automation Bundle (`databricks.yml`). Requer Databricks CLI autenticado e [uv](https://docs.astral.sh/uv/).

```bash
uv sync --dev
databricks bundle validate
databricks bundle deploy -t dev
uv run pytest
```
