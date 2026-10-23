# Declarative Automation Bundles Project

This project uses Declarative Automation Bundles (DABs) for deployment. Add project-specific instructions below.

## For AI Agents: Use Databricks AI Tools

**BEFORE any other action, read the `databricks-core` skill.**

It sets you up to work with this project reliably: CLI authentication, profile
selection, data discovery, and the bundle deployment workflow. Without it,
results are often slower and less accurate.

If this skill is not available (Databricks AI Tools are not installed), you can install them for your coding agent in seconds:

```bash
databricks aitools install
```

If the CLI is not installed, see: https://docs.databricks.com/dev-tools/cli/install

---

## Project Instructions

- **Metodologia:** o projeto segue o CRISP-DM. Toda etapa nova cita a dor (D1–D3) e a fase que atende; o escopo do ciclo está em `src/understanding/01_entendimento_negocio.py`.
- **Organização:** `src/understanding` (fases 1 e 2), `src/sources` (simula as origens, fora do pipeline), `src/extraction` (origem → landing), `src/ingestion` (landing → bronze), `src/transformation` (silver), `src/modeling` (gold), `src/evaluation` (esperado × encontrado), `src/serving` (dashboard), `resources/` (job e dashboard do bundle).
- **Dados:** os dados brutos do curso nunca entram no repositório; credenciais só em Databricks Secrets (escopo `pmo`).
- **Workspace:** o código chega ao Databricks só por `databricks bundle deploy -t dev`; nada de cópias soltas no workspace.
- **Documentação:** links nos `.md` são absolutos do GitHub (links relativos quebram no visualizador do workspace).
- **Antes de cada push:** revisar no chat os arquivos alterados e se o incremento está documentado no README ou em `docs/`.
