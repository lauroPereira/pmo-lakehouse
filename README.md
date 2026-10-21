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

O projeto nasceu da imersão gratuita **"Databricks + Claude Code: projeto de dados do zero"**, do canal Jornada de Dados no YouTube (Luciano Vasconcelos). O caso, os dados e a sequência das aulas são da imersão.

O que fiz além do conteúdo das aulas fica registrado aqui conforme o projeto avança (veja também o [diário](diario.md)).
