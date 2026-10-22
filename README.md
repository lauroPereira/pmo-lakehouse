# pmo-lakehouse

Saúde da entrega numa fábrica de software: lakehouse em Databricks com camada de qualidade de dados.

> 🚧 Em construção (outubro de 2026).

## O problema

Uma consultoria de software com 60 projetos de clientes controla cada um numa ferramenta diferente (Excel, ClickUp, MS Project). Ninguém sabe dizer quais projetos estão atrasados, quanto o atraso vai custar, se há gente ociosa enquanto outro projeto pede reforço, quais clientes priorizar e quais projetos têm multa contratual.

## Arquitetura

Medalhão no Databricks Free Edition (Unity Catalog, serverless): `bronze` (dado como chegou) → `silver` (limpo e integrado, com regras de qualidade e quarentena) → `gold` (KPIs para a diretoria: SPI, CPI, estouro previsto).

## Dados

Sintéticos, de uma empresa fictícia (Vértice Tecnologia) fornecida pela imersão. Nenhum dado real de clientes ou empregadores.

## Origem e créditos

O caso e os dados sintéticos da Vértice Tecnologia vêm da imersão gratuita **"Databricks + Claude Code: projeto de dados do zero"**, do canal Jornada de Dados no YouTube (Luciano Vasconcelos), usada aqui como referência. Os dados brutos do curso não estão neste repositório. Arquitetura, pipeline, regras de qualidade e análises são meus; o desenho está em [docs/arquitetura.md](docs/arquitetura.md).

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
5. **Teste de conectividade**: o notebook [`src/teste_conectividade.py`](src/teste_conectividade.py) testa TCP e login no Postgres, `ping` no Mongo e dois hosts de controle. Foi importado no workspace e executado como run serverless (`databricks workspace import` + `databricks jobs submit`). **Resultado: a Free Edition alcança as duas origens.**

6. **Dados brutos no Volume** (fora do git). Os arquivos do curso sobem para `workspace.raw.material`, que a carga lê:
   ```powershell
   databricks schemas create raw workspace --profile <perfil>
   databricks volumes create workspace raw material MANAGED --profile <perfil>
   databricks fs cp <pasta-dados>\clickup dbfs:/Volumes/workspace/raw/material/clickup --recursive --profile <perfil>
   ```
   (repetido para `excel`, `msproject`, `pmo`, `rh`, `financeiro`, `timesheet` e `compras`).
7. **Carga das origens**: o notebook [`src/origens/carregar_origens.py`](src/origens/carregar_origens.py) lê o Volume e popula as duas origens usando os segredos do passo 4. É idempotente (recria tabelas e esvazia coleções) e **não limpa nada**: os defeitos de dado ficam na origem, para a bronze e a silver tratarem.

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

   Conferência: as contagens do Postgres batem com as linhas dos CSVs (sem o cabeçalho). Todas as colunas do Postgres são `text` de propósito (sistema legado sem tipos), e cada documento do Mongo leva `_arquivo` e `_carregado_em` para rastreio. Escolha de desenho: ClickUp, MS Project e Excel ficam juntos no Mongo porque são as três "ferramentas de gerente" do caso, cada uma com um modelo diferente.

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
