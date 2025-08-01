import os
from datetime import datetime
from enum import Enum

import polars as pl
from boto3 import client


def main():
    print("Script para upload da sintese do DECOMP para o Lake")
    # 1- Confere diretorio sintese/ a partir da chamada
    valida_diretorio_chamada()
    # 2- Pede dados de entrada do usuário e valida cada um:
    # 2.1- competencia (MM/AAAA)
    competencia = obtem_competencia()
    # 2.2- cenario (PROSPECTIVO_INFERIOR, PROSPECTIVO_SUPERIOR, OUTRO)
    cenario = obtem_cenario()
    # 2.3- revisão (inteiro >= 0)
    revisao = obtem_revisao()
    # 3- Edita dataframes da síntese adicionando as colunas a mais
    atualiza_dataframes(competencia, cenario, revisao)
    # 4- Faz upload para o s3
    upload_sintese_s3(competencia, cenario, revisao)
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


DIRETORIO_SINTESE = "sintese"
BUCKET_INGEST_S3 = "ons-dl-02-prd-raw"


def key_arquivo_s3(pref: str, arq: str) -> str:
    return f"ons/lake/decomp/{pref}/{arq}"


def valida_diretorio_chamada():
    conteudo = os.listdir(os.curdir)
    contem_sintese = DIRETORIO_SINTESE in conteudo
    diretorio_valido = os.path.isdir(DIRETORIO_SINTESE)
    if contem_sintese and diretorio_valido:
        print(f"Diretório de sintese (./{DIRETORIO_SINTESE}) encontrado")
    else:
        print(f"Diretório de sintese (./{DIRETORIO_SINTESE}) não encontrado")
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
        + " (PROSPECTIVO_SUPERIOR, PROSPECTIVO_INFERIOR, PMO, OUTRO): "
    )
    try:
        cenario = CenarioEstudo.factory(cenario_str)
        return cenario
    except Exception as e:
        print(str(e))
        exit(1)


def obtem_revisao() -> CenarioEstudo:
    revisao_str = input("Insira a revisao do estudo (numero inteiro >= 0): ")
    try:
        revisao = int(revisao_str)
        return revisao
    except Exception:
        print(f"Erro ao processar a revisao fornecida: {revisao_str}")
        exit(1)


def atualiza_dataframes(
    competencia: datetime, cenario: CenarioEstudo, revisao: int
):
    print("Iniciando atualizacao local dos arquivos...")
    print(f"Competencia: {competencia.isoformat()}")
    print(f"Cenario: {cenario.value}")
    print(f"Revisao: {revisao}")
    arq_diretorio_sintese = os.listdir(DIRETORIO_SINTESE)
    dataframes_sintese = [a for a in arq_diretorio_sintese if ".parquet" in a]
    for arq_df in dataframes_sintese:
        try:
            caminho_df = os.path.join(DIRETORIO_SINTESE, arq_df)
            df = pl.read_parquet(caminho_df)
            dt_type = pl.Datetime
            dt_type.time_unit = "ns"
            df = df.with_columns(
                pl.lit(competencia, dtype=dt_type)
                .alias("competencia")
                .dt.replace_time_zone("UTC"),
                pl.lit(cenario.value).alias("cenario_estudo"),
                pl.lit(revisao, dtype=pl.Int64).alias("revisao"),
            )
            df.write_parquet(caminho_df, compression="snappy")
        except Exception as e:
            print(f"Erro ao atualizar arquivo {arq_df}: {str(e)}")
            exit(1)


def upload_sintese_s3(
    competencia: datetime, cenario: CenarioEstudo, revisao: int
):
    print("Iniciando upload dos arquivos...")
    s3 = client("s3")
    arq_diretorio_sintese = os.listdir(DIRETORIO_SINTESE)
    dataframes_sintese = [a for a in arq_diretorio_sintese if ".parquet" in a]
    pref = competencia.strftime("%Y_%m") + "_" + cenario.value + f"_rv{revisao}"
    for arq_df in dataframes_sintese:
        try:
            caminho_df = os.path.join(DIRETORIO_SINTESE, arq_df)
            s3.upload_file(
                caminho_df, BUCKET_INGEST_S3, key_arquivo_s3(pref, arq_df)
            )
        except Exception as e:
            print(f"Erro no upload do arquivo {arq_df}: {str(e)}")
            exit(1)


if __name__ == "__main__":
    main()
