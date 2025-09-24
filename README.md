
# detools v0.4.1 — biblioteca-first (com Azure Blob + Catálogo/Schema + Notebook)

Novidades:
- `io.azure_blob` (adlfs): leitura/escrita com URLs `abfs://container@account.dfs.core.windows.net/path`
- `utils.catalog`: tipos/validações simples (schema, required, casts, unique)
- `notebooks/Quickstart.ipynb`: exemplo end-to-end (auth Azure, REST, DataFrame, salvar no ADLS ou local, e carregar no SQL)

Instalação rápida:
```
conda env create -f environment.yml
conda activate detools
pip install -e .
cp .env.example .env
```
