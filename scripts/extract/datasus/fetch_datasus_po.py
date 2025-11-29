"""
PO - Painel de Oncologia - desde  2013
Baixa arquivos .dbc do FTP do DATASUS (Painel Oncologia)
"""
import os
from ftplib import FTP, error_perm

FTP_HOST = "ftp.datasus.gov.br"
FTP_DIR = "/dissemin/publicos/PAINEL_ONCOLOGIA/DADOS"
OUTPUT_DIR = "data/utils/dbc_datasus_po"

def ensure_output_dir(path: str):
    os.makedirs(path, exist_ok=True)

def listar_arquivos_dbc(ftp: FTP):
    """
    Lista apenas arquivos .dbc no diretório FTP_DIR.
    Retorna uma lista de nomes de arquivo (sem caminho).
    """
    arquivos = []

    def coletor(nome):
        if nome.lower().endswith(".dbc"):
            arquivos.append(nome)

    ftp.cwd(FTP_DIR)
    ftp.retrlines("NLST", coletor)

    return arquivos

def get_tamanho_ftp(ftp: FTP, nome_arquivo: str) -> int | None:
    """
    Retorna o tamanho do arquivo no FTP (em bytes), ou None se não conseguir.
    """
    try:
        return ftp.size(nome_arquivo)
    except error_perm:
        return None

def precisa_baixar(ftp: FTP, nome_arquivo: str, pasta_saida: str) -> bool:
    """
    Decide se precisa baixar o arquivo:
    - se não existe localmente -> precisa
    - se existe e tamanho no FTP for conhecido e diferente -> precisa
    - se existe e tamanho for igual -> não precisa
    - se não for possível obter tamanho no FTP -> baixa só se não existir
    """
    local_path = os.path.join(pasta_saida, nome_arquivo)

    if not os.path.exists(local_path):
        return True

    tamanho_local = os.path.getsize(local_path)
    tamanho_ftp = get_tamanho_ftp(ftp, nome_arquivo)

    if tamanho_ftp is None:
        print(f"[INFO] Não foi possível obter SIZE de {nome_arquivo} no FTP. "
              f"Arquivo local existente será mantido.")
        return False

    if tamanho_local == tamanho_ftp:
        print(f"[SKIP] {nome_arquivo} (já existe com mesmo tamanho: {tamanho_local} bytes)")
        return False
    else:
        print(f"[WARN] {nome_arquivo} existe, mas tamanho diferente "
              f"(local: {tamanho_local}, FTP: {tamanho_ftp}) -> rebaixando.")
        return True

def baixar_arquivo(ftp: FTP, nome_arquivo: str, pasta_saida: str):
    local_path = os.path.join(pasta_saida, nome_arquivo)

    if not precisa_baixar(ftp, nome_arquivo, pasta_saida):
        return

    print(f"[DOWN] {nome_arquivo}")
    with open(local_path, "wb") as f:
        ftp.retrbinary(f"RETR {nome_arquivo}", f.write)

    print(f"[OK] {local_path}")

def main():
    ensure_output_dir(OUTPUT_DIR)

    print(f"Conectando a {FTP_HOST}...")
    with FTP(FTP_HOST, timeout=60) as ftp:
        ftp.login()  # anônimo
        arquivos = listar_arquivos_dbc(ftp)

        if not arquivos:
            print("Nenhum .dbc encontrado.")
            return

        print(f"Encontrados {len(arquivos)} arquivos .dbc.")
        for arq in arquivos:
            baixar_arquivo(ftp, arq, OUTPUT_DIR)

    print("Fim.")

if __name__ == "__main__":
    main()
