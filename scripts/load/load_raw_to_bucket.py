import os
from datetime import datetime

import pandas as pd
import boto3
from botocore.exceptions import NoCredentialsError
from dotenv import load_dotenv

try:
    import pyarrow.parquet as pq
    HAS_PYARROW = True
except ImportError:
    HAS_PYARROW = False

BASE_DIR = "/opt/airflow/data"
RAW_DIR = os.path.join(BASE_DIR, "raw")
METADADOS_FILE = os.path.join(RAW_DIR, "raw_metadados.csv")

load_dotenv()

MINIO_ENDPOINT = os.environ["MINIO_ENDPOINT"]
MINIO_ACCESS_KEY = os.environ["MINIO_ROOT_USER"]
MINIO_SECRET_KEY = os.environ["MINIO_ROOT_PASSWORD"]
MINIO_BUCKET = os.environ["MINIO_BUCKET"]


def bytes_to_mib(size_bytes: int) -> float:
    """Converte bytes para MiB com 2 casas decimais."""
    if size_bytes is None:
        return 0.0
    return round(size_bytes / (1024 ** 2), 2)


def contar_linhas_csv(caminho: str, chunksize: int = 100_000) -> int:
    """Conta linhas de CSV em chunks para não estourar memória."""
    total = 0
    for chunk in pd.read_csv(caminho, chunksize=chunksize):
        total += len(chunk)
    return total


def contar_linhas_parquet(caminho: str) -> int:
    """
    Conta linhas de Parquet.
    Prioriza usar metadata (pyarrow). Se não tiver, cai pro pandas (mais pesado).
    """
    if HAS_PYARROW:
        parquet_file = pq.ParquetFile(caminho)
        return parquet_file.metadata.num_rows
    else:
        df = pd.read_parquet(caminho)
        return len(df)


def coletar_metadados_raw():
    """
    Lê arquivos CSV/Parquet em data/raw e gera/atualiza raw_metadados.csv.
    Retorna o caminho do arquivo de metadados.
    """
    if not os.path.exists(RAW_DIR):
        print(f"Pasta '{RAW_DIR}' não encontrada. Nada para processar.")
        return None

    arquivos = [
        f
        for f in os.listdir(RAW_DIR)
        if os.path.isfile(os.path.join(RAW_DIR, f))
        and (f.lower().endswith(".csv") or f.lower().endswith(".parquet"))
        and f != os.path.basename(METADADOS_FILE)
    ]

    if not arquivos:
        print(f"Nenhum arquivo CSV ou Parquet encontrado em '{RAW_DIR}'.")
        return None

    registros = []
    data_extracao = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")

    for arquivo in arquivos:
        caminho_arquivo = os.path.join(RAW_DIR, arquivo)
        try:
            tamanho_bytes = os.path.getsize(caminho_arquivo)
            tamanho_mib = bytes_to_mib(tamanho_bytes)

            if arquivo.lower().endswith(".csv"):
                num_registros = contar_linhas_csv(caminho_arquivo)
            else:
                num_registros = contar_linhas_parquet(caminho_arquivo)

            registros.append(
                {
                    "data_extracao": data_extracao,
                    "arquivo": arquivo,
                    "numero_registros": num_registros,
                    "tamanho_mib": tamanho_mib,
                }
            )

            print(
                f"Coletado metadado de '{arquivo}': "
                f"{num_registros} registros, {tamanho_mib} MiB"
            )

        except Exception as e:
            print(f"Erro ao processar '{caminho_arquivo}': {e}")

    if not registros:
        print("Nenhum metadado gerado.")
        return None

    df_novo = pd.DataFrame(registros)

    if os.path.exists(METADADOS_FILE):
        try:
            df_existente = pd.read_csv(METADADOS_FILE)
            df_final = pd.concat([df_existente, df_novo], ignore_index=True)
        except Exception as e:
            print(f"Erro ao ler metadados existentes, sobrescrevendo arquivo: {e}")
            df_final = df_novo
    else:
        df_final = df_novo

    df_final.to_csv(METADADOS_FILE, index=False)
    print(f"Metadados salvos em '{METADADOS_FILE}'.")
    return METADADOS_FILE


def criar_s3_client():
    """Cria cliente S3/MinIO."""
    s3_client = boto3.client(
        "s3",
        endpoint_url=MINIO_ENDPOINT,
        aws_access_key_id=MINIO_ACCESS_KEY,
        aws_secret_access_key=MINIO_SECRET_KEY,
    )

    try:
        s3_client.head_bucket(Bucket=MINIO_BUCKET)
    except Exception:
        print(f"Bucket '{MINIO_BUCKET}' não encontrado. Criando...")
        s3_client.create_bucket(Bucket=MINIO_BUCKET)

    return s3_client


def enviar_pasta_raw_para_minio():
    """Envia todos os arquivos da pasta raw para o bucket MinIO."""
    if not os.path.exists(RAW_DIR):
        print(f"Pasta '{RAW_DIR}' não encontrada. Nada para enviar.")
        return

    arquivos = [
        f
        for f in os.listdir(RAW_DIR)
        if os.path.isfile(os.path.join(RAW_DIR, f))
    ]

    if not arquivos:
        print(f"Nenhum arquivo encontrado em '{RAW_DIR}'.")
        return

    s3_client = criar_s3_client()

    for arquivo in arquivos:
        caminho_arquivo = os.path.join(RAW_DIR, arquivo)
        try:
            s3_client.upload_file(caminho_arquivo, MINIO_BUCKET, arquivo)
            print(
                f"Arquivo '{caminho_arquivo}' enviado para MinIO "
                f"no bucket '{MINIO_BUCKET}' como '{arquivo}'."
            )
        except NoCredentialsError:
            print("Erro: credenciais do MinIO inválidas.")
            break
        except Exception as e:
            print(f"Erro ao enviar '{caminho_arquivo}': {e}")


def main():
    coletar_metadados_raw()
    enviar_pasta_raw_para_minio()


if __name__ == "__main__":
    main()
