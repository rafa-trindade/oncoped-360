import os
import subprocess
from dbfread import DBF
import pandas as pd

idade_maxima = 18

base_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))  # raiz do projeto
dbc_dir = os.path.join(base_dir, 'data', 'utils/datasus_dbc')
csv_dir = os.path.join(base_dir, 'data', 'raw')

dbc2dbf_exe = os.path.join(dbc_dir, 'dbf2dbc.exe')

os.makedirs(csv_dir, exist_ok=True)

arquivos_dbc = [f for f in os.listdir(dbc_dir) if f.lower().endswith('.dbc')]

dfs_infantil = []

if not arquivos_dbc:
    print("Nenhum arquivo .DBC encontrado em data/dbc.")
else:
    for arquivo in arquivos_dbc:
        caminho_dbc = os.path.join(dbc_dir, arquivo)
        nome_base = os.path.splitext(arquivo)[0]
        caminho_dbf = os.path.join(dbc_dir, f"{nome_base}.dbf")

        print(f"\nConvertendo {arquivo} para DBF...")
        subprocess.run([dbc2dbf_exe, caminho_dbc], check=True, cwd=dbc_dir)

        print(f"Lendo {nome_base}.dbf e filtrando registros de crianças...")
        tabela = DBF(caminho_dbf, encoding='latin-1')
        df = pd.DataFrame(iter(tabela))

        df['IDADE'] = pd.to_numeric(df['IDADE'], errors='coerce')

        df = df[df['IDADE'].notnull() & (df['IDADE'] <= idade_maxima)]

        dfs_infantil.append(df)

        if os.path.exists(caminho_dbf):
            os.remove(caminho_dbf)

    if dfs_infantil:
        df_final = pd.concat(dfs_infantil, ignore_index=True)
        caminho_final = os.path.join(csv_dir, 'datasus_oncologia_infantil.csv')

        df_final.to_csv(caminho_final, index=False, encoding='utf-8')
        print(f"\nTodos os CSVs foram unidos em: {caminho_final}")
    else:
        print("Nenhum registro infantil encontrado nos arquivos DBC.")
