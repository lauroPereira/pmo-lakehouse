"""Espaço Genie "Saúde da entrega" (CRISP-DM fase 6: implantação; fase 5: avaliação do assistente).

Gera a definição do espaço (serialized_space) a partir das declarações abaixo e cria ou atualiza o espaço pelo
Databricks CLI. Roda localmente:

    python src/06_ai/genie_espaco.py gerar              # escreve genie_espaco.json
    python src/06_ai/genie_espaco.py publicar           # cria (1ª vez) ou atualiza o espaço
    python src/06_ai/genie_espaco.py avaliar            # roda os benchmarks e grava avaliacao_genie.json

Perfil do CLI em DATABRICKS_CONFIG_PROFILE (padrão: southsystem).
"""
import hashlib
import json
import os
import subprocess
import sys
import time
from pathlib import Path

AQUI = Path(__file__).resolve().parent
PERFIL = os.environ.get("DATABRICKS_CONFIG_PROFILE", "southsystem")
CATALOGO = "workspace"
KPI = f"{CATALOGO}.gold.kpi_projeto"
CARTEIRA = f"{CATALOGO}.gold.kpi_carteira"
TITULO = "Saúde da entrega (pmo-lakehouse)"
DESCRICAO = ("Responde à diretoria da Vértice Tecnologia (cenário simulado) sobre atraso (SPI), custo do atraso "
             "(CPI, EAC, estouro previsto em R$) e coerência do farol reportado pelos gerentes, para os projetos em "
             "andamento na data de corte do último status semanal. Fonte: camada gold do pipeline pmo-lakehouse.")
PASTA_WORKSPACE = "/Workspace/Users/lauro.pereira@southsystem.com.br/pmo_lakehouse_ai"
WAREHOUSE = "Serverless Starter Warehouse"
ARQ_ID = AQUI / "genie_espaco_id.txt"


def uid(texto: str) -> str:
    """ID estável (32 hex) derivado do texto: a definição regenerada não muda os IDs."""
    return hashlib.md5(texto.encode("utf-8")).hexdigest()


# ---------------------------------------------------------------- colunas
COLUNAS_KPI = [
    ("spi", ["Índice de prazo SPI = EV / PV; abaixo de 1 é atraso"], ["índice de prazo", "desempenho de prazo"], {}),
    ("cpi", ["Índice de custo CPI = EV / AC; abaixo de 1 é custo acima do previsto"], ["índice de custo", "desempenho de custo"], {}),
    ("estouro_previsto", ["Quanto o projeto vai custar além do orçamento, em R$ (0 se não estoura)"],
     ["estouro", "custo do atraso", "prejuízo previsto"], {}),
    ("eac", ["Custo final previsto (EAC = BAC / CPI), em R$"], ["custo final previsto", "estimativa no término"], {}),
    ("bac", ["Orçamento de custo do projeto (BAC), em R$"], ["orçamento"], {}),
    ("ac", ["Custo real lançado até a data de corte (AC), em R$"], ["custo real", "gasto"], {}),
    ("situacao", ["crítico se SPI ou CPI < 0,8; atenção se < 0,9; senão ok"], ["status", "saúde do projeto"],
     {"enable_format_assistance": True, "enable_entity_matching": True}),
    ("farol_declarado", ["Cor informada pelo gerente no status semanal"], ["farol do gerente", "status reportado"],
     {"enable_format_assistance": True, "enable_entity_matching": True}),
    ("farol_calculado", ["Cor pelo SPI: verde >= 0,9, amarelo >= 0,8, vermelho abaixo"], ["farol real"],
     {"enable_format_assistance": True, "enable_entity_matching": True}),
    ("melancia", ["Farol declarado verde, mas o farol calculado não é verde"], ["farol mentiroso", "verde por fora"], {}),
    ("cliente", [], [], {"enable_format_assistance": True, "enable_entity_matching": True}),
    ("segmento", [], [], {"enable_format_assistance": True, "enable_entity_matching": True}),
    ("ferramenta", [], ["ferramenta de cronograma"], {"enable_format_assistance": True, "enable_entity_matching": True}),
    ("gerente", [], ["gerente de projeto"], {"enable_format_assistance": True, "enable_entity_matching": True}),
]
OCULTAS_KPI = ["sv", "cv", "etc", "vac", "tarefas", "pct_concluido_declarado"]

# ---------------------------------------------------------------- snippets, exemplos, instruções
MEDIDAS = [
    ("spi_agregado", "SPI agregado", "sum(kpi_projeto.ev) / sum(kpi_projeto.pv)", ["SPI da carteira", "SPI do grupo"]),
    ("cpi_agregado", "CPI agregado", "sum(kpi_projeto.ev) / sum(kpi_projeto.ac)", ["CPI da carteira", "CPI do grupo"]),
]
FILTROS = [("Projeto atrasado", "kpi_projeto.spi < 0.9", ["atrasado", "atrasados", "fora do prazo"])]

EXEMPLOS = [
    ("Quais os 3 projetos de varejo com maior estouro previsto?",
     f"SELECT projeto_id, cliente, estouro_previsto FROM {KPI} WHERE segmento = 'varejo' "
     f"ORDER BY estouro_previsto DESC LIMIT 3"),
    ("Qual o SPI agregado por gerente?",
     f"SELECT gerente, sum(ev) / sum(pv) AS spi_agregado FROM {KPI} GROUP BY gerente ORDER BY spi_agregado"),
    ("Em quais projetos o farol do gerente difere do farol calculado?",
     f"SELECT projeto_id, cliente, farol_declarado, farol_calculado, spi FROM {KPI} "
     f"WHERE farol_declarado <> farol_calculado ORDER BY spi"),
]

INSTRUCOES = """## PURPOSE
- Saúde da entrega dos projetos em andamento da Vértice Tecnologia (cenário simulado) na data de corte, o último status semanal.
- Público: diretoria e PMO. Responda em português do Brasil.

## DISAMBIGUATION
- "Atrasado" sem outro critério significa SPI abaixo de 0,9.
- "Crítico" e "em atenção" seguem a coluna situacao.
- Para SPI ou CPI de um grupo de projetos (carteira, segmento, ferramenta, gerente, cliente), use o índice agregado (soma do EV dividida pela soma do PV ou do AC), nunca a média dos índices.
- Glossário, para quando perguntarem o significado de uma sigla:
- BAC: orçamento do projeto. PV: valor planejado até a data de corte. EV: valor agregado, o que foi entregue. AC: custo real.
- SPI = EV / PV, índice de prazo; abaixo de 1 é atraso. CPI = EV / AC, índice de custo; abaixo de 1 é custo acima do previsto.
- EAC = BAC / CPI, custo final previsto. Estouro previsto = EAC - BAC, quando positivo.
- Melancia: projeto com farol declarado verde e farol calculado diferente de verde.

## Instructions you must follow when providing summaries
- Valores em R$ com duas casas decimais, em milhões acima de R$ 1 milhão.
- Índices (SPI, CPI) com duas casas decimais.
- Cite a data de corte ao falar de números da carteira."""

PERGUNTAS_EXEMPLO = [
    "Quais projetos estão atrasados e de quais clientes?",
    "Quanto o atraso vai custar para a empresa?",
    "Em quais projetos o farol do gerente não bate com a realidade?",
    "O que é SPI?",
]

# ---------------------------------------------------------------- benchmarks (respostas validadas na gold)
BENCHMARKS = [
    # Valores brutos, sem round nem conversão para milhões: o avaliador (modo Chat) compara conjuntos de resultados.
    ("B1", "Quantos projetos estão atrasados?", f"SELECT count(*) AS projetos_atrasados FROM {KPI} WHERE spi < 0.9"),
    ("B2", "Quais os 5 projetos com pior SPI?", f"SELECT projeto_id, spi FROM {KPI} ORDER BY spi LIMIT 5"),
    ("B3", "Qual o SPI do PRJ-011?", f"SELECT spi FROM {KPI} WHERE projeto_id = 'PRJ-011'"),
    ("B4", "Quantos projetos estão em situação crítica e quantos em atenção?",
     f"SELECT count_if(situacao = 'crítico') AS projetos_criticos, count_if(situacao = 'atenção') AS projetos_em_atencao "
     f"FROM {KPI}"),
    ("B5", "Quais clientes têm projetos críticos?",
     f"SELECT DISTINCT cliente FROM {KPI} WHERE situacao = 'crítico' ORDER BY cliente"),
    ("B6", "Qual o estouro previsto da carteira?", f"SELECT estouro_previsto_total FROM {CARTEIRA}"),
    ("B7", "Qual projeto tem o maior estouro previsto?",
     f"SELECT projeto_id, estouro_previsto FROM {KPI} ORDER BY estouro_previsto DESC LIMIT 1"),
    ("B8", "Qual o estouro previsto por segmento?",
     f"SELECT segmento, sum(estouro_previsto) AS estouro_previsto FROM {KPI} GROUP BY segmento ORDER BY 2 DESC"),
    ("B9", "Qual o CPI do PRJ-009?", f"SELECT cpi FROM {KPI} WHERE projeto_id = 'PRJ-009'"),
    ("B10", "Quais projetos estão com farol verde mas atrasados?",
     f"SELECT projeto_id FROM {KPI} WHERE melancia ORDER BY projeto_id"),
    ("B11", "Qual o SPI da carteira por ferramenta?",
     f"SELECT ferramenta, sum(ev) / sum(pv) AS spi_agregado FROM {KPI} GROUP BY ferramenta ORDER BY ferramenta"),
    ("B12", "Qual o SPI e o CPI da carteira?",
     f"SELECT sum(ev) / sum(pv) AS spi_agregado, sum(ev) / sum(ac) AS cpi_agregado FROM {KPI}"),
]


def definicao() -> dict:
    colunas = []
    for nome, desc, sin, flags in COLUNAS_KPI:
        c = {"column_name": nome}
        if desc:
            c["description"] = desc
        if sin:
            c["synonyms"] = sin
        c.update(flags)
        colunas.append(c)
    colunas += [{"column_name": n, "exclude": True} for n in OCULTAS_KPI]
    por_id = lambda xs: sorted(xs, key=lambda x: x["id"])
    return {
        "version": 2,
        "config": {"sample_questions": por_id([{"id": uid("q:" + q), "question": [q]} for q in PERGUNTAS_EXEMPLO])},
        "data_sources": {"tables": sorted([
            {"identifier": CARTEIRA},
            {"identifier": KPI, "column_configs": sorted(colunas, key=lambda c: c["column_name"])},
        ], key=lambda t: t["identifier"])},
        "instructions": {
            "text_instructions": [{"id": uid("instrucoes"), "content": [INSTRUCOES]}],
            "example_question_sqls": por_id([{"id": uid("e:" + q), "question": [q], "sql": [s]} for q, s in EXEMPLOS]),
            "sql_snippets": {
                "measures": por_id([{"id": uid("m:" + a), "alias": a, "display_name": d, "sql": [s], "synonyms": sin}
                                    for a, d, s, sin in MEDIDAS]),
                "filters": por_id([{"id": uid("f:" + d), "display_name": d, "sql": [s], "synonyms": sin}
                                   for d, s, sin in FILTROS]),
            },
        },
        "benchmarks": {"questions": por_id([
            {"id": uid("b:" + cod), "question": [q], "answer": [{"format": "SQL", "content": [s]}]}
            for cod, q, s in BENCHMARKS])},
    }


def cli(*args, entrada=None) -> dict:
    r = subprocess.run(["databricks", *args, "--profile", PERFIL, "-o", "json"], capture_output=True, text=True,
                       encoding="utf-8", input=entrada, env={**os.environ, "MSYS_NO_PATHCONV": "1"})
    if r.returncode:
        sys.exit(f"Falhou: databricks {' '.join(args[:3])}\n{r.stderr}")
    return json.loads(r.stdout.lstrip("﻿")) if r.stdout.strip() else {}


def gerar():
    (AQUI / "genie_espaco.json").write_text(json.dumps(definicao(), ensure_ascii=False, indent=2), encoding="utf-8")
    print("genie_espaco.json gerado")


def publicar():
    gerar()
    serial = json.dumps(definicao(), ensure_ascii=False)
    if ARQ_ID.exists():
        espaco = ARQ_ID.read_text().strip()
        cli("genie", "update-space", espaco, "--json", json.dumps({"serialized_space": serial, "title": TITULO,
                                                                    "description": DESCRICAO}))
        print("Espaço atualizado:", espaco)
        return
    wh = next(w["id"] for w in cli("warehouses", "list") if w["name"] == WAREHOUSE)
    cli("workspace", "mkdirs", PASTA_WORKSPACE)
    r = cli("genie", "create-space", "--json", json.dumps({
        "warehouse_id": wh, "title": TITULO, "description": DESCRICAO,
        "parent_path": PASTA_WORKSPACE, "serialized_space": serial}))
    ARQ_ID.write_text(r["space_id"])
    print("Espaço criado:", r["space_id"])


def avaliar():
    espaco = ARQ_ID.read_text().strip()
    ids = {uid("b:" + cod): (cod, q) for cod, q, _ in BENCHMARKS}
    run = cli("genie", "genie-create-eval-run", espaco, "--json", json.dumps({"benchmark_question_ids": sorted(ids)}))
    run_id = run.get("eval_run_id") or run.get("id")
    print("Avaliação iniciada:", run_id)
    while True:
        st = cli("genie", "genie-get-eval-run", espaco, run_id)
        if st.get("eval_run_status") in ("DONE", "FAILED", "CANCELLED"):
            break
        print(f"  {st.get('num_done', 0)}/{st.get('num_questions', '?')}...")
        time.sleep(15)
    resultados = cli("genie", "genie-list-eval-results", espaco, run_id)
    lista = resultados.get("eval_results") or resultados.get("results") or resultados
    saida = []
    for r in lista:
        det = cli("genie", "genie-get-eval-result-details", espaco, run_id, r["result_id"])
        cod, q = ids.get(r.get("benchmark_question_id") or det.get("benchmark_question_id"), ("?", "?"))
        atual = (det.get("actual_response") or [{}])[0]
        saida.append({"codigo": cod, "pergunta": q, "avaliacao": r.get("assessment") or det.get("assessment"),
                      "motivos": r.get("assessment_reasons") or det.get("assessment_reasons"),
                      "sql_gerado": atual.get("response")})
    saida.sort(key=lambda x: int(x["codigo"][1:]) if x["codigo"][1:].isdigit() else 99)
    (AQUI / os.environ.get("SAIDA_AVALIACAO", "avaliacao_genie.json")).write_text(json.dumps({"eval_run_id": run_id, "status": st.get("eval_run_status"),
                                                           "resultados": saida}, ensure_ascii=False, indent=2),
                                               encoding="utf-8")
    for s in saida:
        print(s["codigo"], s["avaliacao"], "-", s["pergunta"])


if __name__ == "__main__":
    {"gerar": gerar, "publicar": publicar, "avaliar": avaliar}[sys.argv[1] if len(sys.argv) > 1 else "gerar"]()
