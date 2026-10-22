# Databricks notebook source
# MAGIC %md
# MAGIC # CRISP-DM · Fase 1 · Entendimento do negócio
# MAGIC
# MAGIC > **Cenário simulado.** A Vértice Tecnologia é uma empresa fictícia e os dados são sintéticos, vindos da imersão
# MAGIC > *"Databricks + Claude Code: projeto de dados do zero"* (canal Jornada de Dados, Luciano Vasconcelos). Conduzo o caso
# MAGIC > como conduziria num cliente real, seguindo o CRISP-DM: tudo parte da dor do negócio.
# MAGIC
# MAGIC Números marcados com **†** vêm do material da imersão e foram **confirmados nos nossos dados** na fase 2
# MAGIC (`02_entendimento_dados`, tabela esperado × encontrado: 11 de 11).

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. A empresa
# MAGIC
# MAGIC - Fábrica de software com cerca de **520 pessoas** e **60 projetos de clientes**, em 5 segmentos: varejo, financeiro,
# MAGIC   saúde, logística e indústria.
# MAGIC - **Modelo de receita:** projetos com valor de contrato fechado e orçamento de custo previsto. A margem depende de
# MAGIC   entregar no prazo e no custo.
# MAGIC - **Momento:** a diretoria percebe a margem caindo e projetos estourando, mas não consegue dizer quais, quanto e por quê.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Pessoas, jornadas e onde dói
# MAGIC
# MAGIC | Quem | O que faz | Sistema que usa | Onde dói |
# MAGIC |---|---|---|---|
# MAGIC | Diretoria | Decide prioridade de clientes e de reforço | Relatórios montados à mão | Não confia no status que recebe |
# MAGIC | PMO | Consolida o status semanal | Sistema corporativo + planilhas | Junta 3 ferramentas manualmente toda semana |
# MAGIC | Gerente de projeto | Planeja e acompanha tarefas | ClickUp, MS Project ou Excel (cada um o seu) | Reporta um farol subjetivo, sem métrica |
# MAGIC | Alocação e RH | Distribui pessoas entre projetos | Cadastro de colaboradores + timesheet | Não enxerga quem está ocioso |
# MAGIC | Financeiro | Lança e controla custos | Sistema corporativo | Descobre o estouro depois que aconteceu |
# MAGIC | Compras | Contrata fornecedores e acompanha entregas | Sistema corporativo + contratos em PDF | Atraso de fornecedor vira atraso de projeto sem aviso |

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Os sistemas
# MAGIC
# MAGIC - **Sistema corporativo (PostgreSQL):** módulos de PMO (projetos e status semanal), RH (colaboradores), financeiro
# MAGIC   (custos), timesheet (apontamentos) e compras (entregas de fornecedor). Legado, sem tipos fortes: tudo é texto.
# MAGIC - **Repositório de exports das ferramentas dos gerentes (MongoDB):** o time de integração despeja ali os exports de
# MAGIC   ClickUp (JSON), MS Project (XML) e Excel, cada um no formato da sua ferramenta. Ninguém os unifica.
# MAGIC - **Contratos de fornecedores (PDF):** prazos e cláusulas que hoje ninguém cruza com as entregas.
# MAGIC
# MAGIC Volume de cada sistema, como chegou ao lakehouse (camada bronze):

# COMMAND ----------

tabelas = {
    "Sistema corporativo · PMO · projetos": "pg_pmo_projetos",
    "Sistema corporativo · PMO · status semanal": "pg_pmo_status_semanal",
    "Sistema corporativo · RH · colaboradores": "pg_rh_colaboradores",
    "Sistema corporativo · financeiro · custos": "pg_financeiro_custos",
    "Sistema corporativo · timesheet · apontamentos": "pg_timesheet_apontamentos",
    "Sistema corporativo · compras · entregas de fornecedor": "pg_compras_entregas_fornecedor",
    "Exports dos gerentes · ClickUp (listas)": "mongo_clickup_listas",
    "Exports dos gerentes · MS Project (projetos)": "mongo_msproject_projetos",
    "Exports dos gerentes · Excel (linhas de cronograma)": "mongo_excel_cronogramas",
}
display(spark.createDataFrame(
    [(sistema, spark.table(f"workspace.bronze.{t}").count()) for sistema, t in tabelas.items()],
    "sistema string, registros long",
))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. As dores
# MAGIC
# MAGIC O que ouvimos do negócio, com a evidência que precisa ser confirmada nos dados:
# MAGIC
# MAGIC | # | Dor, na voz do negócio | Evidência a confirmar |
# MAGIC |---|---|---|
# MAGIC | D1 | "Não sei quais projetos estão atrasados." | Projetos espalhados em 3 ferramentas: 20 em Excel, 25 no ClickUp, 15 no MS Project |
# MAGIC | D2 | "Não sei quanto o atraso vai me custar." | Estouro previsto de R$ 6,47 mi† (custo projetado × orçamento) |
# MAGIC | D3 | "O farol que recebo não bate com a realidade." | Projetos com farol verde e SPI baixo, os "melancia": 4† |
# MAGIC | D4 | "Tenho gente parada enquanto outro projeto pede reforço." | Horas disponíveis × apontadas no timesheet |
# MAGIC | D5 | "Fornecedor atrasa e eu só descubro tarde." | Data prometida × data de entrega; prazos dos contratos |
# MAGIC | D6 | "Quero saber o que vai dar errado antes de dar errado." | Histórico semanal de status, custo e avanço |

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Escopo deste ciclo: do problema à métrica (D1–D3)
# MAGIC
# MAGIC As três primeiras dores formam um núcleo coeso (prazo, custo e confiança no status) e usam os mesmos dados.
# MAGIC Este ciclo do CRISP-DM aprofunda nelas.
# MAGIC
# MAGIC | Dor | Pergunta de negócio | KPI | Fontes | Critério de sucesso |
# MAGIC |---|---|---|---|---|
# MAGIC | D1 | Quais projetos estão atrasados, e de quais clientes? | SPI por projeto | ClickUp, MS Project, Excel, `pmo.projetos` | Todos os projetos em andamento com SPI calculado numa visão única |
# MAGIC | D2 | Quanto o atraso vai custar? | CPI, EAC, estouro previsto (R$) | `financeiro.custos`, `pmo.projetos` | Estouro por projeto, conferido contra o valor de referência |
# MAGIC | D3 | Em quais projetos o farol mente? | Farol declarado × SPI | `pmo.status_semanal` + SPI | Lista de divergências, com a regra explícita |
# MAGIC
# MAGIC ### Backlog (próximos ciclos)
# MAGIC
# MAGIC | Dor | Pergunta | Fontes, já na bronze |
# MAGIC |---|---|---|
# MAGIC | D4 | Quem está ocioso e onde falta gente? | `timesheet.apontamentos`, `rh.colaboradores` |
# MAGIC | D5 | Quais fornecedores atrasam, e em que projetos? | `compras.entregas_fornecedor`, contratos em PDF |
# MAGIC | D6 | Quais projetos têm risco de atrasar no próximo mês? (modelo preditivo) | Histórico semanal de status, custo e avanço |

# COMMAND ----------

# MAGIC %md
# MAGIC ### 5.1 As métricas: gerenciamento do valor agregado (PMBOK)
# MAGIC
# MAGIC Os KPIs de D1–D3 vêm do **gerenciamento do valor agregado** (*Earned Value Management*, EVM), a técnica do PMBOK/PMI
# MAGIC que compara, numa mesma unidade (R$), o que foi planejado, o que foi entregue e o que foi gasto.
# MAGIC
# MAGIC **As quatro medidas de base**
# MAGIC
# MAGIC | Sigla | Nome | Pergunta que responde | De onde vem neste caso |
# MAGIC |---|---|---|---|
# MAGIC | BAC | *Budget at Completion*, orçamento no término | Quanto o projeto deveria custar no total? | `orcamento_custo` em `pmo.projetos` |
# MAGIC | PV | *Planned Value*, valor planejado | Quanto do orçamento já deveria estar entregue hoje? | BAC × % planejado até hoje, pelas datas e horas de baseline das tarefas |
# MAGIC | EV | *Earned Value*, valor agregado | Quanto do orçamento já foi de fato entregue? | BAC × % concluído, ponderado pelas horas das tarefas |
# MAGIC | AC | *Actual Cost*, custo real | Quanto já foi gasto? | Soma de `financeiro.custos` do projeto até hoje |
# MAGIC
# MAGIC **Os índices e as projeções**
# MAGIC
# MAGIC | Métrica | Fórmula | Leitura |
# MAGIC |---|---|---|
# MAGIC | SV, variação de prazo | EV − PV | Negativa: entregou menos do que deveria até agora |
# MAGIC | CV, variação de custo | EV − AC | Negativa: gastou mais do que entregou |
# MAGIC | **SPI**, índice de desempenho de prazo | EV ÷ PV | **Abaixo de 1 é atraso.** 0,80 = entregou 80% do que deveria |
# MAGIC | **CPI**, índice de desempenho de custo | EV ÷ AC | **Abaixo de 1 é custo acima do previsto.** 0,90 = cada R$ 1,00 gasto entregou R$ 0,90 |
# MAGIC | **EAC**, estimativa no término | BAC ÷ CPI | Quanto o projeto vai custar se a eficiência de custo atual continuar |
# MAGIC | ETC, estimativa para terminar | EAC − AC | Quanto ainda falta gastar |
# MAGIC | **VAC**, variação no término | BAC − EAC | Negativa: **estouro previsto** (a resposta de D2, em R$) |
# MAGIC
# MAGIC **Exemplo.** Projeto com BAC de R$ 1.000.000. Hoje deveria ter entregue 50% (PV = R$ 500 mil), entregou 40%
# MAGIC (EV = R$ 400 mil) e gastou R$ 500 mil (AC). Então SPI = 0,80 (atrasado), CPI = 0,80 (caro),
# MAGIC EAC = R$ 1.250.000 e VAC = −R$ 250.000: se nada mudar, o projeto estoura em R$ 250 mil.
# MAGIC
# MAGIC **Decisões que ficam para as próximas fases**
# MAGIC - **EAC:** a fórmula BAC ÷ CPI supõe que o desempenho de custo atual continua. Há variantes (por exemplo, AC + (BAC − EV),
# MAGIC   quando o desvio é pontual); a escolha é registrada na fase 4 e conferida com o valor de referência na fase 5.
# MAGIC - **% concluído e % planejado:** cada ferramenta (ClickUp, MS Project, Excel) informa avanço e baseline de um jeito;
# MAGIC   a unificação na silver define a regra única, e as tarefas-resumo ficam fora da soma para não contar horas duas vezes.
# MAGIC - **Farol calculado (D3):** faixas de SPI que equivalem a verde, amarelo e vermelho. Uma referência comum é verde
# MAGIC   a partir de 0,95, amarelo entre 0,85 e 0,95 e vermelho abaixo de 0,85; é premissa a validar com o PMO.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Requisito não funcional: dado confiável
# MAGIC
# MAGIC O negócio não pediu, mas é condição para as respostas de D1–D3 valerem: **todo número entregue precisa ser rastreável
# MAGIC e todo defeito de dado precisa ser detectado e explicado**, em vez de corrigido em silêncio ou ignorado.
# MAGIC
# MAGIC - É a equipe de dados que propõe esse requisito, a partir do entendimento dos dados (fase 2).
# MAGIC - Defeitos esperados†: colaboradores duplicados, custos com vírgula decimal, tarefas-resumo misturadas às tarefas,
# MAGIC   datas em formatos mistos.
# MAGIC - Como será atendido: camada de qualidade com quarentena e motivo, e uma conferência "esperado × encontrado" a cada carga.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7. Premissas, restrições e riscos
# MAGIC
# MAGIC **Premissas**
# MAGIC - O baseline de cada projeto (datas e horas previstas) é confiável.
# MAGIC - O avanço informado nas ferramentas reflete o trabalho real.
# MAGIC
# MAGIC **Restrições**
# MAGIC - Acesso só de leitura às origens.
# MAGIC - Databricks Free Edition: serverless, sem conectores gerenciados.
# MAGIC - As origens não têm coluna de atualização (`updated_at`) nem CDC.
# MAGIC
# MAGIC **Riscos**
# MAGIC - A qualidade do dado contamina os KPIs, por isso a qualidade vem antes da análise.
# MAGIC - As 3 ferramentas modelam tarefas de formas diferentes; a unificação pode distorcer o SPI se não for explícita.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 8. Próximos passos do ciclo
# MAGIC
# MAGIC | Fase do CRISP-DM | O que entrega neste ciclo |
# MAGIC |---|---|
# MAGIC | 2. Entendimento dos dados | Confirma as evidências marcadas com †, perfila chaves e defeitos (`02_entendimento_dados`) |
# MAGIC | 3. Preparação dos dados | Silver: modelo único de tarefas, custos tipados, quarentena com motivo |
# MAGIC | 4. Modelagem | Gold: modelo de valor agregado (SPI, CPI, EAC) e regra do farol |
# MAGIC | 5. Avaliação | Critérios da seção 5 conferidos; perguntas de avaliação do assistente (Genie) |
# MAGIC | 6. Implantação | Job orquestrado no bundle, dashboard e assistente em linguagem natural |
