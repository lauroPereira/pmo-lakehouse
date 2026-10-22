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

## Como rodar

Projeto no formato Declarative Automation Bundle (`databricks.yml`). Requer Databricks CLI autenticado e [uv](https://docs.astral.sh/uv/).

```bash
uv sync --dev
databricks bundle validate
databricks bundle deploy -t dev
uv run pytest
```
