import os
import json
from pathlib import Path

from kaggle.api.kaggle_api_extended import KaggleApi
from dotenv import load_dotenv

load_dotenv()

KAGGLE_USER = os.getenv("KAGGLE_USERNAME")
DATASET_NAME = "onco-360"
DATASET_TITLE = "Dados Onco-360 - Pipeline Semanal"
DATA_DIR = Path("/opt/airflow/data/raw")

DATASET_ID = f"{KAGGLE_USER}/{DATASET_NAME}"


def dataset_exists(api: KaggleApi, owner: str, dataset_name: str) -> bool:
    """
    Verifica se o dataset existe para o usuário informado,
    usando dataset_list e comparando o 'ref' (owner/slug).
    """
    print(f"Verificando se o dataset {owner}/{dataset_name} existe via dataset_list...")

    # alguns clients usam argumentos diferentes; aqui usamos search e owner
    datasets = api.dataset_list(search=dataset_name)

    for d in datasets:
        # em geral d.ref vem no formato "owner/slug"
        # dependendo da versão, pode ser d.slug ou d.ref. Vamos ser defensivos:
        ref = getattr(d, "ref", None) or getattr(d, "slug", None)
        if ref == DATASET_ID:
            print(f"✅ Dataset encontrado na lista: {ref}")
            return True

    print(f"ℹ️ Dataset {DATASET_ID} não encontrado na lista.")
    return False


def load_raw_to_kaggle():
    """
    Cria ou atualiza o dataset público 'onco-360' no Kaggle com
    os arquivos da pasta data/raw.
    """
    print(f"Iniciando o carregamento para o Kaggle: {DATASET_ID}")

    api = KaggleApi()
    api.authenticate()

    # monta o metadata
    metadata = {
        "title": DATASET_TITLE,
        "id": DATASET_ID,
        "licenses": [{"name": "CC0-1.0"}],
        "resources": [],
    }

    # pega arquivos para upload (exceto o metadata)
    files_to_upload = [
        f for f in DATA_DIR.iterdir()
        if f.is_file() and f.name != "dataset-metadata.json"
    ]
    if not files_to_upload:
        print(f"Aviso: Nenhum arquivo encontrado em {DATA_DIR}. Encerrando o processo de upload.")
        return

    metadata_path = DATA_DIR / "dataset-metadata.json"
    with open(metadata_path, "w") as f:
        json.dump(metadata, f, indent=4)

    print(f"Metadata criado em: {metadata_path}")

    try:
        # ---- checa se o dataset existe usando dataset_list ----
        try:
            dataset_exists_flag = dataset_exists(api, KAGGLE_USER, DATASET_NAME)
        except Exception as e:
            print(f"Erro ao verificar existência do dataset via dataset_list: {e}")
            # se der algum erro estranho aqui, por segurança não tentamos create_version
            raise

        # ---- cria nova versão ou novo dataset ----
        if dataset_exists_flag:
            print(f"Dataset {DATASET_ID} já existe. Tentando atualizar...")
            api.dataset_create_version(
                folder=str(DATA_DIR),
                version_notes="Atualização semanal via Airflow",
                delete_old_versions=True,
                quiet=False,
            )
            print(f"✅ Dataset {DATASET_ID} atualizado com sucesso!")
        else:
            print(f"Dataset {DATASET_ID} não existe. Tentando criar...")
            api.dataset_create_new(
                folder=str(DATA_DIR),
                public=True,
                quiet=False,
            )
            print(f"✅ Dataset {DATASET_ID} criado com sucesso!")

    except Exception as e:
        print(f"❌ Erro ao interagir com o Kaggle: {e}")
        raise
    finally:
        if metadata_path.exists():
            metadata_path.unlink()
            print(f"Arquivo temporário de metadata {metadata_path} removido.")


if __name__ == "__main__":
    load_raw_to_kaggle()
