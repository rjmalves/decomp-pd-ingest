# decomp-pd-ingest

Script para ingestão de dados operativos provenientes do modelo DECOMP como produtos de dados.

## Instalação

Execute o script de setup (cria virtualenv, instala dependências e cria symlink do entrypoint `decomp-pd-ingest`):

```bash
./setup.sh
```

Use `./setup.sh --force` para recriar o ambiente.

## Variáveis de ambiente (.env)

O script procura um arquivo `.env` no diretório atual (ou acima) ao executar o comando `decomp-pd-ingest`. Um arquivo de exemplo `.env.example` é fornecido. Caso `.env` não exista durante o `setup.sh`, ele será criado automaticamente a partir do exemplo.

Edite os valores conforme necessário:

```
BUCKET_NAME="my-bucket"
BUCKET_PREFIX="my-prefix"
SYNTHESIS_DIR="sintese"
```

## Uso

No diretório que contém a pasta definida em `SYNTHESIS_DIR` (padrão `./sintese` com arquivos `.parquet`), execute:

```bash
decomp-pd-ingest
```

O programa solicitará: competência (MM/AAAA), cenário e versão.

## Atualização

Para atualizar dependências após alterações em `pyproject.toml`:

```bash
source venv/bin/activate
pip install -U .
```

## Desenvolvimento

Instale dependências de desenvolvimento:

```bash
source venv/bin/activate
pip install .[dev]
```

Rodar lint (ruff):

```bash
ruff check .
```

## Licença

Veja `LICENSE`.
