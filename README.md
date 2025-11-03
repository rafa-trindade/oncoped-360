# 🎗️ oncoped-360
`em desenvolvimento`

Monitoramento de casos, atendimentos, repasses públicos e estrutura hospitalar voltados à oncologia pediátrica no Brasil, integrando dados do DATASUS, INCA, CNES e Portal da Transparência.

## 📄 Relatório de Execução do Projeto:

`dbc_to_csv.py` 
- ✅ Download dos arquivos `.dbc` do DATASUS (Painel de Oncologia 2016~2025) e utilização do executável `dbf2dbc.exe` (fornecido pelo DATASUS/TabWin para expansão de arquivos DBC) para conversão automatizada para `.csv`, com filtragem de registros de idade ≤ 18 anos.
- ✅ Unificação de arquivos intermediários e organização dos dados exportados em `data/raw/datasus_oncologia_infantil.csv`.

---

## 📦 Bibliotecas Utilizadas:

| Pacote            | Versão      | Observação |
|-------------------|------------|------------|
| **pandas**        | 2.3.3      | Manipulação e transformação de dados |
| **dbfread**       | 2.0.7      | Leitura de arquivos `.dbf` gerados pelo DATASUS |

**dbf2dbc.exe** - Executável fornecido pelo DATASUS/TabWin para expansão de arquivos `.dbc` para `.dbf` 