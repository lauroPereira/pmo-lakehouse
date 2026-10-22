# Arquitetura (rascunho do bloco 1)

## Tese do case

Respostas de IA sobre dados só valem o que vale o dado por baixo. O projeto mostra o caminho inteiro: fontes bagunçadas → dado com qualidade provada → KPIs governados → assistente em linguagem natural (Genie) cuja resposta é **avaliada** contra números conhecidos.

## Fontes (sintéticas, Vértice Tecnologia)

Cronogramas em Excel, listas do ClickUp (JSON), MS Project, compras (CSV), contratos e manual (PDF, fora do escopo inicial). Dados brutos do curso não vão para o repositório.

## Camadas (Unity Catalog, serverless)

| Camada | Conteúdo | Linguagem |
|---|---|---|
| landing | snapshot completo de cada origem em Parquet imutável (`dt_raw`, `lote`), com hash e chave natural | PySpark (JDBC + pymongo) |
| bronze | Auto Loader sobre a landing; MERGE por hash (idempotente), `_arquivo` de origem em cada linha, controle de carga | PySpark |
| silver | modelo único de tarefas, tipos corrigidos, deduplicação; regras de qualidade com **quarentena e motivo** | PySpark |
| gold | SPI, CPI, estouro previsto, ociosidade × demanda, projetos com multa; cada KPI definido uma vez (metric views) | SQL |
| ia | espaço Genie sobre a gold + conjunto de perguntas com resposta esperada | Genie, Python |

## Qualidade de dados como produto

- Defeitos de taxa conhecida (duplicados, custo com vírgula, tarefas-resumo etc.) → o pipeline precisa reencontrá-los.
- Tabela **esperado × encontrado** roda a cada execução do job.
- Perguntas de avaliação do Genie: pergunta, SQL esperado, resposta esperada, resultado (acerto/erro).

## Critérios de sucesso

1. Job único de ponta a ponta, implantado via bundle (`pmo_lakehouse`).
2. Todos os defeitos conhecidos detectados e explicados na quarentena.
3. Genie acerta as perguntas de avaliação; erros documentados com causa.
4. README com decisões, números e o que é da imersão × o que é meu.

## Blocos restantes

2 Bronze · 3 Silver · 4 Gold · 5 Validação + job · 6 Camada de IA, dashboard e README. Prazo: 28/10.
