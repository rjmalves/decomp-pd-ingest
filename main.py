import os
from datetime import datetime
from enum import Enum

import polars as pl
from boto3 import client
from dotenv import find_dotenv, load_dotenv


def main():
    # Carregamento de variáveis de ambiente com prioridade para diretório de instalação.
    # Ordem de busca:
    # 1. Variável DECOMP_PD_ENV_FILE (se definida e existente)
    # 2. Diretório de instalação (onde este arquivo main.py reside)
    # 3. Diretório de execução atual (.)
    # 4. Busca ascendente via find_dotenv
    install_dir = os.path.abspath(os.path.dirname(__file__))
    env_override = os.getenv("DECOMP_PD_ENV_FILE")
    candidate_paths = []
    if env_override:
        candidate_paths.append(env_override)
    candidate_paths.append(os.path.join(install_dir, ".env"))
    candidate_paths.append(os.path.join(os.getcwd(), ".env"))

    dotenv_path = None
    for p in candidate_paths:
        if p and os.path.isfile(p):
            dotenv_path = p
            break
    if not dotenv_path:
        found = find_dotenv(usecwd=True)
        if found:
            dotenv_path = found
    if dotenv_path:
        load_dotenv(dotenv_path=dotenv_path, override=True)
        print(f"Variáveis carregadas de: {dotenv_path}")
    else:
        print(
            "Aviso: arquivo .env não encontrado; usando variáveis já presentes no ambiente."
        )

    print("Script para upload da sintese do DECOMP para o Lake")
    # 1- Confere diretorio sintese/ a partir da chamada
    valida_diretorio_chamada()
    # 2- Pede dados de entrada do usuário e valida cada um:
    # 2.1- competencia (MM/AAAA)
    competencia = obtem_competencia()
    # 2.2- cenario (PROSPECTIVO_INFERIOR, PROSPECTIVO_SUPERIOR, OUTRO)
    cenario = obtem_cenario()
    # 2.3- versão (inteiro >= 0)
    versao = obtem_versao()
    # 3- Edita dataframes da síntese adicionando as colunas a mais
    atualiza_dataframes(competencia, cenario, versao)
    # 4- Faz upload para o s3
    upload_sintese_s3(competencia, cenario, versao)
    print("Upload da sintese do DECOMP feito com sucesso!")


class CenarioEstudo(Enum):
    PROSPECTIVO_INFERIOR = "PROSPECTIVO_INFERIOR"
    PROSPECTIVO_SUPERIOR = "PROSPECTIVO_SUPERIOR"
    PMO = "PMO"
    OUTRO = "OUTRO"

    @classmethod
    def factory(cls, val: str) -> "CenarioEstudo":
        for v in cls:
            if v.value == val:
                return v
        raise ValueError(f"Cenario {val} nao reconhecido")

    @classmethod
    def from_index(cls, index: int) -> "CenarioEstudo":
        index_map = {
            0: "PROSPECTIVO_INFERIOR",
            1: "PROSPECTIVO_SUPERIOR",
            2: "PMO",
            3: "OUTRO",
        }
        if index in index_map:
            return cls.factory(index_map[index])
        raise ValueError(f"Codigo {index} nao reconhecido")


def key_arquivo_s3(pref: str, arq: str) -> str:
    return f"{os.getenv('BUCKET_PREFIX')}/{pref}/{arq}"


def valida_diretorio_chamada():
    synthesis_dir = os.getenv("SYNTHESIS_DIR")
    conteudo = os.listdir(os.curdir)
    contem_sintese = synthesis_dir in conteudo
    diretorio_valido = os.path.isdir(synthesis_dir)
    if contem_sintese and diretorio_valido:
        print(f"Diretório de sintese (./{synthesis_dir}) encontrado")
    else:
        print(f"Diretório de sintese (./{synthesis_dir}) não encontrado")
        exit(1)


def obtem_competencia() -> datetime:
    competencia_str = input("Insira a competencia do estudo (MM/AAAA): ")
    try:
        competencia = datetime.strptime(competencia_str, "%m/%Y")
        return competencia
    except Exception:
        print(f"Erro ao processar competencia fornecida: {competencia_str}")


def obtem_cenario() -> CenarioEstudo:
    cenario_str = input(
        "Insira o cenario do estudo"
        + " (0 - PROSPECTIVO_INFERIOR, 1 - PROSPECTIVO_SUPERIOR, 2 - PMO, 3 - OUTRO): "
    )
    try:
        cenario = CenarioEstudo.from_index(int(cenario_str))
        return cenario
    except Exception as e:
        print(str(e))
        exit(1)


def obtem_versao() -> CenarioEstudo:
    versao_str = input("Insira a versao do estudo (numero inteiro >= 0): ")
    try:
        versao = int(versao_str)
        return versao
    except Exception:
        print(f"Erro ao processar a versao fornecida: {versao_str}")
        exit(1)


def atualiza_dataframes(
    competencia: datetime, cenario: CenarioEstudo, versao: int
):
    print("Iniciando atualizacao local dos arquivos...")
    print(f"Competencia: {competencia.isoformat()}")
    print(f"Cenario: {cenario.value}")
    print(f"Versao: {versao}")
    synthesis_dir = os.getenv("SYNTHESIS_DIR")
    arq_diretorio_sintese = os.listdir(synthesis_dir)
    dataframes_sintese = [a for a in arq_diretorio_sintese if ".parquet" in a]
    for arq_df in dataframes_sintese:
        try:
            caminho_df = os.path.join(synthesis_dir, arq_df)
            df = pl.read_parquet(caminho_df)
            dt_type = pl.Datetime
            dt_type.time_unit = "ns"
            df = df.with_columns(
                pl.lit(competencia, dtype=dt_type)
                .alias("competencia")
                .dt.replace_time_zone("UTC"),
                pl.lit(cenario.value).alias("cenario_estudo"),
                pl.lit(versao, dtype=pl.Int64).alias("revisao"),
            )
            df.write_parquet(caminho_df, compression="snappy")
        except Exception as e:
            print(f"Erro ao atualizar arquivo {arq_df}: {str(e)}")
            exit(1)


def upload_sintese_s3(
    competencia: datetime, cenario: CenarioEstudo, versao: int
):
    print("Iniciando upload dos arquivos...")
    synthesis_dir = os.getenv("SYNTHESIS_DIR")
    s3 = client("s3")
    arq_diretorio_sintese = os.listdir(synthesis_dir)
    dataframes_sintese = [a for a in arq_diretorio_sintese if ".parquet" in a]
    pref = competencia.strftime("%Y_%m") + "_" + cenario.value + f"_rv{versao}"
    for arq_df in dataframes_sintese:
        try:
            caminho_df = os.path.join(synthesis_dir, arq_df)
            s3.upload_file(
                caminho_df,
                os.getenv("BUCKET_NAME"),
                key_arquivo_s3(pref, arq_df),
            )
        except Exception as e:
            print(f"Erro no upload do arquivo {arq_df}: {str(e)}")
            exit(1)


if __name__ == "__main__":
    # Mantido para compatibilidade se executado como script diretamente.
    main()
