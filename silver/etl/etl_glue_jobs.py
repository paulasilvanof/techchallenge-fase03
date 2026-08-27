
import re
import sys
import unicodedata
from datetime import datetime
from functools import reduce

import boto3
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from pyspark.sql import functions as F


# 1. INICIALIZACAO DO GLUE / SPARK


args = getResolvedOptions(sys.argv, ["JOB_NAME"])

spark_context = SparkContext.getOrCreate()
glue_context = GlueContext(spark_context)
spark = glue_context.spark_session

job = Job(glue_context)
job.init(args["JOB_NAME"], args)

dt_start = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

print("============================================================")
print("INICIO DO GLUE JOB - CAMADA SILVER")
print("Start time:", dt_start)
print("============================================================")



# 2. CONFIGURACAO E CAMINHOS S3


BUCKET = "tech-challenge-014478672967"
DATABASE = "dados_catalogados"
TABELA = "silver_state_of_data"

ANOS = (2023, 2024, 2025)

# um arquivo por ano; aponta direto no arquivo para o Spark nao tentar
# inferir "ano" como coluna de particao a partir da pasta ano=YYYY
CAMINHOS = {
    2023: f"s3://{BUCKET}/bronze/ano=2023/dados_2023.csv",
    2024: f"s3://{BUCKET}/bronze/ano=2024/dados_2024.csv",
    2025: f"s3://{BUCKET}/bronze/ano=2025/dados_2025.csv",
}

PREFIXO_SILVER = "silver"
KEY_PARQUET = f"{PREFIXO_SILVER}/parquet/state_of_data_unificado.parquet"
KEY_CSV = f"{PREFIXO_SILVER}/csv/state_of_data_unificado.csv"

# diretorios temporarios: o Spark escreve uma pasta com part-00000-...,
# que a secao 14 renomeia para o nome final
TMP_PARQUET = f"{PREFIXO_SILVER}/_tmp_parquet/"
TMP_CSV = f"{PREFIXO_SILVER}/_tmp_csv/"

# numeros de referencia, validados pelo pipeline local em pandas
ESPERADO_LINHAS = 14002
ESPERADO_COLUNAS = 156



# 3. MAPEAMENTO DAS COLUNAS (DE-PARA)


COLUNAS_SEMANTICAS = {
    # --- Identificacao ---
    "id_respondente": {2023: "P0", 2024: "0.a_token", 2025: "0.a_token"},
    "data_envio": {
        2023: None,  # a pesquisa de 2023 publicada no Kaggle nao traz data/hora
        2024: "0.d_data/hora_envio",
        2025: "0.d_data/hora_envio",
    },
    # --- Perfil demografico (secao 1) ---
    "idade": {2023: "P1_a", 2024: "1.a_idade", 2025: "1.a_idade"},
    "faixa_idade": {2023: "P1_a_1", 2024: "1.a.1_faixa_idade", 2025: "1.a.1_faixa_idade"},
    "genero": {2023: "P1_b", 2024: "1.b_genero", 2025: "1.b_genero"},
    "cor_raca_etnia": {2023: "P1_c", 2024: "1.c_cor/raca/etnia", 2025: "1.c_cor/raca/etnia"},
    "pcd": {2023: "P1_d", 2024: "1.d_pcd", 2025: "1.d_pcd"},
    "experiencia_prejudicada": {
        2023: "P1_e",
        2024: "1.e_experiencia_profissional_prejudicada",
        2025: "1.e_experiencia_profissional_prejudicada",
    },
    "vive_no_brasil": {2023: "P1_g", 2024: "1.g_vive_no_brasil", 2025: "1.g_vive_no_brasil"},
    "pais_onde_mora": {2023: None, 2024: "1.h_pais_onde_mora", 2025: "1.h_pais_onde_mora"},
    "estado_onde_mora": {2023: "P1_i", 2024: "1.i_estado_onde_mora", 2025: "1.i_estado_onde_mora"},
    "uf_onde_mora": {2023: "P1_i_1", 2024: "1.i.1_uf_onde_mora", 2025: "1.i.1_uf_onde_mora"},
    "regiao_onde_mora": {2023: "P1_i_2", 2024: "1.i.2_regiao_onde_mora", 2025: "1.i.2_regiao_onde_mora"},
    "regiao_de_origem": {2023: "P1_k", 2024: "1.k.2_regiao_de_origem", 2025: "1.k.2_regiao_de_origem"},
    "nivel_ensino": {2023: "P1_l", 2024: "1.l_nivel_de_ensino", 2025: "1.l_nivel_de_ensino"},
    "area_formacao": {2023: "P1_m", 2024: "1.m_área_de_formação", 2025: "1.m_área_de_formação"},
    # --- Situacao profissional (secao 2) ---
    "situacao_trabalho": {2023: "P2_a", 2024: "2.a_situação_de_trabalho", 2025: "2.a_situação_de_trabalho"},
    "setor": {2023: "P2_b", 2024: "2.b_setor", 2025: "2.b_setor"},
    "numero_funcionarios": {2023: "P2_c", 2024: "2.c_numero_de_funcionarios", 2025: "2.c_numero_de_funcionarios"},
    "atua_como_gestor": {2023: "P2_d", 2024: "2.d_atua_como_gestor", 2025: "2.d_atua_como_gestor"},
    "cargo_como_gestor": {2023: "P2_e", 2024: "2.e_cargo_como_gestor", 2025: "2.e_cargo_como_gestor"},
    "cargo_atual": {2023: "P2_f", 2024: "2.f_cargo_atual", 2025: "2.f_cargo_atual"},
    "nivel": {2023: "P2_g", 2024: "2.g_nivel", 2025: "2.g_nivel"},
    "faixa_salarial": {2023: "P2_h", 2024: "2.h_faixa_salarial", 2025: "2.h_faixa_salarial"},
    "tempo_experiencia_dados": {
        2023: "P2_i",
        2024: "2.i_tempo_de_experiencia_em_dados",
        2025: "2.i_tempo_de_experiencia_em_dados",
    },
    "tempo_experiencia_ti": {
        2023: "P2_j",
        2024: "2.j_tempo_de_experiencia_em_ti",
        2025: "2.j_tempo_de_experiencia_em_ti",
    },
    "satisfeito_empresa_atual": {2023: "P2_k", 2024: "2.k_satisfeito_atualmente", 2025: "2.k_satisfeito_atualmente"},
    "participou_entrevistas_6m": {
        2023: "P2_m",
        2024: "2.m_participou_de_entrevistas_ultimos_6m",
        2025: "2.m_participou_de_entrevistas_ultimos_6m",
    },
    "pretende_mudar_emprego_6m": {
        2023: "P2_n",
        2024: "2.n_planos_de_mudar_de_emprego_6m",
        2025: "2.n_planos_de_mudar_de_emprego_6m",
    },
    # ATENCAO: deslocamento de letra em 2025 (layoff saiu de 2.q para 2.p).
    "empresa_teve_layoff": {
        2023: "P2_q",
        2024: "2.q_empresa_passou_por_layoff_em_2024",
        2025: "2.p_empresa_passou_por_layoff_em_2025",
    },
    "modelo_trabalho_atual": {
        2023: "P2_r",
        2024: "2.r_modelo_de_trabalho_atual",
        2025: "2.q_modelo_de_trabalho_atual",
    },
    "modelo_trabalho_ideal": {
        2023: "P2_s",
        2024: "2.s_modelo_de_trabalho_ideal",
        2025: "2.r_modelo_de_trabalho_ideal",
    },
    "atitude_retorno_presencial": {
        2023: "P2_t",
        2024: "2.t_atitude_em_caso_de_retorno_presencial",
        2025: "2.s_atitude_em_caso_de_retorno_presencial",
    },
    # --- Empresa / time de dados (secao 3) ---
    "numero_pessoas_dados": {
        2023: "P3_a",
        2024: "3.a_numero_de_pessoas_em_dados",
        2025: "3.a_numero_de_pessoas_em_dados",
    },
    "ia_generativa_prioridade": {
        2023: "P3_e",
        2024: "3.e_ai_generativa_e_llm_é_uma_prioridade?",
        2025: "3.e_ai_generativa_e_llm_é_uma_prioridade?",
    },
    # Pergunta nova em 2025 - fica NULL nos anos anteriores.
    "ia_bons_resultados_llm": {
        2023: None,
        2024: None,
        2025: "3.g_empresa_está_conseguindo_ter_bons_resultados_com_llms",
    },
    # --- Atuacao e tecnologias (secao 4) ---
    "funcao_atuacao": {2023: "P4_a", 2024: "4.a_funcao_de_atuacao", 2025: "4.a_funcao_de_atuacao"},
    # Texto concatenado do multi-select de linguagens. Em 2025 a coluna-mae esta
    # rotulada como "linguagem_preferida" na fonte, mas o conteudo e a lista do dia
    # a dia (80%+ dos valores tem virgula) - por isso entra aqui, nao em "preferida".
    "linguagens_dia_a_dia": {
        2023: "P4_d",
        2024: "4.d_linguagem_de_programacao_(dia_a_dia)",
        2025: "4.c_linguagem_preferida",
    },
    # Perguntas de valor unico que 2025 deixou de fazer.
    "linguagem_mais_usada": {2023: "P4_e", 2024: "4.e_linguagem_mais_usada", 2025: None},
    "linguagem_preferida": {2023: "P4_f", 2024: "4.f_linguagem_preferida", 2025: None},
    "cloud_preferida": {2023: "P4_i", 2024: "4.i_cloud_preferida", 2025: "4.f_cloud_preferida"},
    "bi_preferida": {
        2023: "P4_k",
        2024: "4.k_ferramenta_de_bi_preferida",
        2025: "4.h_ferramenta_de_bi_preferida",
    },
    "tipo_uso_ia_empresa": {
        2023: "P4_l",
        2024: "4.l_tipo_de_uso_de_ai_generativa_e_llm_na_empresa",
        2025: "4.i_tipo_de_uso_de_ai_generativa_e_llm_na_empresa",
    },
    "usa_llm_trabalho": {
        2023: "P4_m",
        2024: "4.m_usa_chatgpt_ou_copilot_no_trabalho?",
        2025: "4.j_usa_chatgpt_ou_copilot_no_trabalho?",
    },
    # --- Busca por oportunidade (secao 5) ---
    "objetivo_area_dados": {2023: "P5_a", 2024: "5.a_objetivo_na_area_de_dados", 2025: "5.a_objetivo_na_area_de_dados"},
    "oportunidade_buscada": {2023: "P5_b", 2024: "5.b_oportunidade_buscada", 2025: "5.b_oportunidade_buscada"},
    # --- Engenharia de dados (secao 6) ---
    "possui_data_lake": {2023: "P6_c", 2024: "6.c_possui_data_lake", 2025: "6.c_possui_data_lake"},
    "possui_data_warehouse": {2023: "P6_e", 2024: "6.e_possui_data_warehouse", 2025: "6.e_possui_data_warehouse"},
}

# --- 3.2 blocos multi-resposta (dummies 0/1) -> colunas booleanas -----------
#
# Os prefixos finais levam "tec_" para nao colidirem com as colunas de texto
# cloud_preferida / bi_preferida: sem isso, selecionar as booleanas de cloud por
# prefixo traria colunas de texto junto e quebraria a media das analises.

COLUNAS_TECNOLOGIA = {
    "tec_fonte_": {2023: "P4_b_", 2024: "4.b.", 2025: "4.b."},
    "tec_lang_": {2023: "P4_d_", 2024: "4.d.", 2025: "4.c."},
    "tec_db_": {2023: "P4_g_", 2024: "4.g.", 2025: "4.d."},
    "tec_cloud_": {2023: "P4_h_", 2024: "4.h.", 2025: "4.e."},
    "tec_bi_": {2023: "P4_j_", 2024: "4.j.", 2025: "4.g."},
    "ia_uso_": {2023: "P4_m_", 2024: "4.m.", 2025: "4.j."},
}

# quantas opcoes cada bloco deve render por ano (validacao da secao 7)
OPCOES_ESPERADAS = {
    2023: {"tec_fonte_": 8, "tec_lang_": 15, "tec_db_": 33, "tec_cloud_": 7, "tec_bi_": 23, "ia_uso_": 5},
    2024: {"tec_fonte_": 8, "tec_lang_": 15, "tec_db_": 33, "tec_cloud_": 7, "tec_bi_": 18, "ia_uso_": 5},
    2025: {"tec_fonte_": 8, "tec_lang_": 10, "tec_db_": 33, "tec_cloud_": 7, "tec_bi_": 18, "ia_uso_": 5},
}

# Colunas sim/nao que a origem codifica de forma diferente por ano: 0/1 em 2023 e
# 2025, TRUE/FALSE em 2024. Lidas como texto, viram 4 categorias distintas na
# mesma coluna e qualquer agregacao sai errada.
COLUNAS_BOOLEANAS_SIM_NAO = [
    "atua_como_gestor",
    "satisfeito_empresa_atual",
    "possui_data_lake",
    "possui_data_warehouse",
]

# Rotulos que mudaram de redacao entre os anos (ou que ficam ilegiveis depois do
# truncamento do slugify) e precisam convergir para o mesmo nome canonico.
ALIASES_SLUG = {
    # Linguagens - 2023 escreve "nenhuma linguagem", 2024/2025 "nenhuma das linguagens listadas"
    "nao_utilizo_nenhuma_linguagem": "nao_utilizo_nenhuma",
    "nao_utilizo_nenhuma_das_linguagens_listadas": "nao_utilizo_nenhuma",
    # BI - encurta rotulos longos truncados pelo slugify
    "fazemos_todas_as_analises_utilizando_apenas_e": "apenas_excel_ou_planilhas",
    "nao_utilizo_nenhuma_ferramenta_de_bi_no_traba": "nao_utilizo_nenhuma",
    "looker_studio_google_data_studio": "looker_studio",
    # Cloud
    "servidores_on_premise_nao_utilizamos_cloud": "on_premise",
    # Uso de IA com foco em produtividade - rotulos identicos nos 3 anos, apenas
    # encurtados aqui para o nome da coluna final ficar utilizavel (renome 1:1).
    "nao_uso_solucoes_de_ai_generativa_com_foco_em": "nao_uso",
    "uso_solucoes_gratuitas_de_ai_generativa_com_f": "gratuitas",
    "uso_e_pago_pelas_solucoes_de_ai_generativa_co": "pagas_proprio_bolso",
    "a_empresa_que_trabalho_paga_pelas_solucoes_de": "pagas_pela_empresa",
    "uso_solucoes_do_tipo_copilot": "copilot",
}

# --- 3.3 correcoes de valores (erros de digitacao na origem) 

CORRECOES_VALOR = {
    "faixa_salarial": {
        # 2023: faltou um digito no limite inferior
        "de R$ 101/mês a R$ 2.000/mês": "de R$ 1.001/mês a R$ 2.000/mês",
        # 2025: "3000" onde deveria ser "30.000"
        "de R$ 25.001/mês a R$ 3000/mês": "de R$ 25.001/mês a R$ 30.000/mês",
    },
    "participou_entrevistas_6m": {
        # 2023 usa uma redacao mais curta para a mesma opcao de 2024/2025
        "Sim, fiz entrevistas mas não fui aprovado": (
            "Sim, fiz entrevistas mas não fui aprovado (ou ainda aguardo resposta)"
        ),
    },
}

# --- 3.4 ordenacoes (categorica -> posicao) 

ORDEM_FAIXA_SALARIAL = [
    "Menos de R$ 1.000/mês",
    "de R$ 1.001/mês a R$ 2.000/mês",
    "de R$ 2.001/mês a R$ 3.000/mês",
    "de R$ 3.001/mês a R$ 4.000/mês",
    "de R$ 4.001/mês a R$ 6.000/mês",
    "de R$ 6.001/mês a R$ 8.000/mês",
    "de R$ 8.001/mês a R$ 12.000/mês",
    "de R$ 12.001/mês a R$ 16.000/mês",
    "de R$ 16.001/mês a R$ 20.000/mês",
    "de R$ 20.001/mês a R$ 25.000/mês",
    "de R$ 25.001/mês a R$ 30.000/mês",
    "de R$ 30.001/mês a R$ 40.000/mês",
    "Acima de R$ 40.001/mês",
]

ORDEM_NIVEL = ["Júnior", "Pleno", "Sênior", "Especialista/Staff+"]

# 2025 criou o nivel "Especialista/Staff+", que nao existe em 2023/2024.
# Para series temporais comparaveis ele e agrupado em "Senior"; a coluna
# "nivel" preserva o valor original.
NIVEL_COMPARAVEL = {"Especialista/Staff+": "Sênior"}

ORDEM_FAIXA_IDADE = ["17-21", "22-24", "25-29", "30-34", "35-39", "40-44", "45-49", "50-54", "55+"]

ORDEM_PORTE_EMPRESA = [
    "de 1 a 5",
    "de 6 a 10",
    "de 11 a 50",
    "de 51 a 100",
    "de 101 a 500",
    "de 501 a 1.000",
    "de 1.001 a 3.000",
    "Acima de 3.000",
]

ORDEM_TEMPO_EXPERIENCIA = [
    "Não tenho experiência na área de dados",
    "Menos de 1 ano",
    "de 1 a 2 anos",
    "de 2 a 3 anos",
    "de 4 a 6 anos",
    "de 7 a 10 anos",
    "Mais de 10 anos",
]

# Valores de "situacao_trabalho" que caracterizam vinculo de trabalho ativo.
SITUACOES_EMPREGADO = [
    "Empregado (CLT)",
    "Empreendedor ou Empregado (CNPJ)",
    "Servidor Público",
    "Estagiário",
    "Freelancer",
    "Trabalho na área Acadêmica/Pesquisador",
    "Vivo no Brasil e trabalho remoto para empresa de fora do Brasil",
    "Vivo fora do Brasil e trabalho para empresa de fora do Brasil",
]

# --- 3.5 respostas abertas que precisam de canonicalizacao -----------------
# O respondente digitava a vontade, entao aparecem '.', 'Datadricks', frases inteiras.

SEM_PREFERENCIA = "Sem preferência / Não sei opinar"

COLUNAS_TEXTO_LIVRE = {
    "cloud_preferida": [
        "Amazon Web Services (AWS)",
        "Google Cloud (GCP)",
        "Azure (Microsoft)",
        "Oracle Cloud",
        "IBM",
        "Databricks",
        "Snowflake",
        "Servidores On Premise/Não utilizamos Cloud",
        "Cloud Própria",
        SEM_PREFERENCIA,
    ],
    "bi_preferida": [
        "Microsoft PowerBI",
        "Qlik View/Qlik Sense",
        "Tableau",
        "Metabase",
        "Superset",
        "Redash",
        "Looker",
        "Looker Studio(Google Data Studio)",
        "Amazon Quicksight",
        "Alteryx",
        "MicroStrategy",
        "SAP Business Objects/SAP Analytics",
        "Oracle Business Intelligence",
        "Salesforce/Einstein Analytics",
        "SAS Visual Analytics",
        "Grafana",
        "Pentaho",
        "Databricks",
        "Streamlit",
        "Plotly Dash",
        "Shiny",
        "Excel",
        SEM_PREFERENCIA,
    ],
}

# Variacoes digitadas -> opcao canonica (chave = slugify do que o respondente escreveu).
# A opcao "nao tenho preferencia" e uma resposta VALIDA do questionario, nao lixo:
# sem estes aliases ela cairia em "Outros" e inflaria essa categoria em ~1.800 casos.
ALIASES_TEXTO_LIVRE = {
    "cloud_preferida": {
        "nao_sei_opinar_nao_tenho_preferencia": SEM_PREFERENCIA,
        "nao_tenho_preferencia_nao_sei_opinar": SEM_PREFERENCIA,
        "nao_tenho_preferencia": SEM_PREFERENCIA,
        "sem_preferencia": SEM_PREFERENCIA,
        "nao_sei": SEM_PREFERENCIA,
        "nao_sei_responder": SEM_PREFERENCIA,
        "nao_sei_opinar": SEM_PREFERENCIA,
        "nao_consigo_escolher": SEM_PREFERENCIA,
        "qualquer_uma": SEM_PREFERENCIA,
        "oracle": "Oracle Cloud",
        "on_prem": "Servidores On Premise/Não utilizamos Cloud",
        "on_premise": "Servidores On Premise/Não utilizamos Cloud",
        "aws": "Amazon Web Services (AWS)",
        "gcp": "Google Cloud (GCP)",
        "azure": "Azure (Microsoft)",
        "datadricks": "Databricks",
        "cloud_com_databricks": "Databricks",
    },
    "bi_preferida": {
        "nao_tenho_preferencia_nao_sei_opinar": SEM_PREFERENCIA,
        "nao_sei_opinar_nao_tenho_preferencia": SEM_PREFERENCIA,
        "nao_tenho_preferencia": SEM_PREFERENCIA,
        "sem_preferencia": SEM_PREFERENCIA,
        "nao_sei": SEM_PREFERENCIA,
        "nao_sei_opinar": SEM_PREFERENCIA,
        "looker_studio": "Looker Studio(Google Data Studio)",
        "google_data_studio": "Looker Studio(Google Data Studio)",
        "data_studio": "Looker Studio(Google Data Studio)",
        "power_bi": "Microsoft PowerBI",
        "powerbi": "Microsoft PowerBI",
        "databricks_sql": "Databricks",
        "qlik": "Qlik View/Qlik Sense",
        "qliksense": "Qlik View/Qlik Sense",
        "quicksight": "Amazon Quicksight",
    },
}

# Ordem final das colunas (as de tecnologia entram depois, em bloco).
ORDEM_COLUNAS_BASE = [
    "ano_pesquisa",
    "id_unificado",
    "id_respondente",
    "data_envio",
    # perfil
    "idade",
    "faixa_idade",
    "faixa_idade_ordem",
    "genero",
    "cor_raca_etnia",
    "pcd",
    "experiencia_prejudicada",
    "vive_no_brasil",
    "pais_onde_mora",
    "estado_onde_mora",
    "uf_onde_mora",
    "regiao_onde_mora",
    "regiao_de_origem",
    "nivel_ensino",
    "area_formacao",
    # trabalho
    "situacao_trabalho",
    "flag_empregado",
    "setor",
    "numero_funcionarios",
    "porte_empresa_ordem",
    "atua_como_gestor",
    "cargo_como_gestor",
    "cargo_atual",
    "funcao_atuacao",
    "flag_atua_com_dados",
    "nivel",
    "nivel_comparavel",
    "nivel_ordem",
    "tempo_experiencia_dados",
    "tempo_experiencia_dados_ordem",
    "tempo_experiencia_ti",
    # remuneracao
    "faixa_salarial",
    "faixa_salarial_ordem",
    "salario_min",
    "salario_max",
    "salario_medio",
    # modelo de trabalho e satisfacao
    "modelo_trabalho_atual",
    "modelo_trabalho_ideal",
    "atitude_retorno_presencial",
    "satisfeito_empresa_atual",
    "participou_entrevistas_6m",
    "pretende_mudar_emprego_6m",
    "empresa_teve_layoff",
    "objetivo_area_dados",
    "oportunidade_buscada",
    # empresa / maturidade de dados
    "numero_pessoas_dados",
    "possui_data_lake",
    "possui_data_warehouse",
    # IA
    "ia_generativa_prioridade",
    "ia_bons_resultados_llm",
    "tipo_uso_ia_empresa",
    "usa_llm_trabalho",
    "flag_usa_ia",
    # tecnologias (texto)
    "linguagens_dia_a_dia",
    "linguagem_mais_usada",
    "linguagem_preferida",
    "cloud_preferida",
    "cloud_preferida_original",
    "bi_preferida",
    "bi_preferida_original",
]


# 4. FUNCOES AUXILIARES


_RE_TUPLA_2023 = re.compile(r"^\('(?P<cod>.+?)\s*',\s*'(?P<rotulo>.*)'\)$", re.S)


def parse_coluna_2023(coluna):
    """Converte "('P2_h ', 'Faixa salarial')" em ("P2_h", "Faixa salarial").

    Atencao ao espaco a direita dentro da tupla ('P1_a ') - sem o strip a busca falha.
    """
    m = _RE_TUPLA_2023.match(coluna)
    if not m:
        return None, coluna
    return m.group("cod").strip(), m.group("rotulo").strip()


def slugify(texto, max_len=45):
    """Normaliza um rotulo de opcao para virar sufixo de nome de coluna.

    'Microsoft PowerBI' -> 'microsoft_powerbi'
    """
    texto = unicodedata.normalize("NFKD", str(texto))
    texto = "".join(c for c in texto if not unicodedata.combining(c))
    texto = texto.lower()
    texto = re.sub(r"[^a-z0-9]+", "_", texto)
    texto = re.sub(r"_+", "_", texto).strip("_")
    return texto[:max_len].strip("_")


def nome_seguro(nome, max_len=128):
    """Cabecalho cru -> nome de coluna que o Spark aceita.

    Os cabecalhos originais tem '.', '(', ')', '/', aspas e espacos. F.col("4.h.1_Amazon...")
    leria os pontos como acesso a campo aninhado e falharia. A mesma regra e usada na
    catalogacao da bronze (criar_tabelas_bronze.py).
    """
    nome = nome.strip()
    if nome.startswith("(") and nome.endswith(")"):
        nome = nome[1:-1].replace("'", " ")
    nome = unicodedata.normalize("NFKD", nome)
    nome = nome.encode("ascii", "ignore").decode("ascii")
    nome = nome.lower()
    nome = re.sub(r"[^a-z0-9]+", "_", nome).strip("_")
    if not nome:
        return ""
    if nome[0].isdigit():
        nome = "p" + nome
    return nome[:max_len].rstrip("_")


def mapa_nomes_seguros(originais):
    """[cabecalhos crus] -> ({original: seguro}, [seguros na ordem original])."""
    vistos = {}
    seguros = []
    mapa = {}
    for i, original in enumerate(originais):
        nome = nome_seguro(original) or f"coluna_{i}"
        if nome in vistos:
            vistos[nome] += 1
            sufixo = f"_{vistos[nome]}"
            nome = nome[: 128 - len(sufixo)] + sufixo
        else:
            vistos[nome] = 1
        seguros.append(nome)
        mapa[original] = nome
    return mapa, seguros


# --- expressoes Spark -------------------------------------------------------

# valores que a origem usa como "vazio" e que precisam virar NULL de verdade
VAZIOS = ["", ".", "..", "-", "N/A", "n/a"]

_ACENTOS_DE = "áàâãäåéèêëíìîïóòôõöúùûüçñýÿ"
_ACENTOS_PARA = "aaaaaaeeeeiiiiooooouuuucnyy"


def para_boolean(coluna):
    """Uniformiza as varias codificacoes de sim/nao da origem.

    2023 e 2025 trazem 0/1; 2024 traz TRUE/FALSE. Lidas como texto, viram 4
    categorias distintas na mesma coluna e quebram qualquer agregacao.
    NULL continua NULL: quem nao respondeu o bloco nao vira 'false'.
    """
    valor = F.lower(F.trim(coluna))
    return (
        F.when(valor.isin("1", "1.0", "true", "sim"), F.lit(True))
        .when(valor.isin("0", "0.0", "false", "nao", "não"), F.lit(False))
        .otherwise(F.lit(None).cast("boolean"))
    )


def slug_spark(coluna):
    """slugify() equivalente, em Spark puro (sem UDF Python)."""
    s = F.lower(F.trim(coluna))
    s = F.translate(s, _ACENTOS_DE, _ACENTOS_PARA)
    s = F.regexp_replace(s, "[^a-z0-9]+", "_")
    s = F.regexp_replace(s, "^_+|_+$", "")
    s = F.substring(s, 1, 45)
    return F.regexp_replace(s, "_+$", "")


def ordinal(coluna, ordem):
    """Categorica -> posicao 1..n, para ordenar os graficos."""
    pares = []
    for i, valor in enumerate(ordem):
        pares.extend([F.lit(valor), F.lit(i + 1)])
    return F.create_map(*pares)[coluna].cast("int")


def de_para(coluna, correcoes):
    """Aplica um dicionario {valor_errado: valor_certo} sobre a coluna."""
    expr = coluna
    for errado, certo in correcoes.items():
        expr = F.when(coluna == F.lit(errado), F.lit(certo)).otherwise(expr)
    return expr



# 5. LEITURA DOS 3 CSVs CRUS

#
# inferSchema=False: lemos tudo como STRING e tipamos explicitamente no fim,
# para evitar conversoes automaticas incorretas.
#
# multiLine NAO e usado: foi verificado que nenhum dos 3 arquivos tem quebra de
# linha dentro de campo, e liga-lo impediria o Spark de dividir o arquivo.



def ler_bronze(ano):
    df = (
        spark.read.option("header", True)
        .option("inferSchema", False)
        .option("quote", '"')
        .option("escape", '"')
        .option("encoding", "UTF-8")
        .csv(CAMINHOS[ano])
    )
    originais = df.columns
    mapa, seguros = mapa_nomes_seguros(originais)
    df = df.toDF(*seguros)
    print(f"  {ano}: {len(originais)} colunas lidas de {CAMINHOS[ano]}")
    return df, originais, mapa


print("\n--- 5. LEITURA DA BRONZE ---")
brutos = {}
for ano in ANOS:
    brutos[ano] = ler_bronze(ano)



# 6. DE-PARA SEMANTICO POR ANO



def indice_2023(originais):
    """Codigo canonico -> nome real da coluna, para o cabecalho em formato de tupla."""
    indice = {}
    for coluna in originais:
        cod, _ = parse_coluna_2023(coluna)
        if cod:
            indice[cod] = coluna
    return indice


def montar_semanticas(ano, originais, mapa):
    idx23 = indice_2023(originais) if ano == 2023 else {}
    saida = {}
    ausentes = []
    for nome_final, origens in COLUNAS_SEMANTICAS.items():
        origem = origens[ano]
        if origem is None:
            saida[nome_final] = F.lit(None).cast("string")
            continue
        original = idx23.get(origem) if ano == 2023 else origem
        seguro = mapa.get(original) if original else None
        if seguro is None:
            saida[nome_final] = F.lit(None).cast("string")
            ausentes.append(f"{nome_final} (esperava '{origem}')")
            continue
        saida[nome_final] = F.col(seguro).cast("string")

    if ausentes:
        print(f"  [ATENCAO] {ano}: colunas mapeadas mas nao encontradas no arquivo:")
        for a in ausentes:
            print(f"      - {a}")
    return saida



# 7. BLOCOS MULTI-RESPOSTA -> COLUNAS BOOLEANAS

#
# O casamento entre anos e feito pelo ROTULO da opcao (normalizado), nao pelo
# indice numerico - a ordem das opcoes muda entre as pesquisas.



def montar_tecnologias(ano, originais, mapa):
    saida = {}
    for prefixo_final, origens in COLUNAS_TECNOLOGIA.items():
        prefixo = origens[ano]
        if prefixo is None:
            continue
        encontradas = 0
        for original in originais:
            if ano == 2023:
                cod, rotulo = parse_coluna_2023(original)
                if not cod or not re.fullmatch(rf"{re.escape(prefixo)}\d+", cod):
                    continue
            else:
                if not original.startswith(prefixo):
                    continue
                # o separador depois do indice pode ser "_" ou " " (varia na origem)
                m = re.match(r"^\d+[_ ](.*)$", original[len(prefixo) :])
                if not m:
                    continue
                rotulo = m.group(1)

            slug = slugify(rotulo)
            slug = ALIASES_SLUG.get(slug, slug)
            saida[f"{prefixo_final}{slug}"] = para_boolean(F.col(mapa[original]))
            encontradas += 1

        esperado = OPCOES_ESPERADAS[ano][prefixo_final]
        marca = "ok" if encontradas == esperado else f"DIVERGENTE (esperado {esperado})"
        print(f"  {ano} bloco '{prefixo_final}' (origem '{prefixo}'): {encontradas} opcoes  {marca}")
    return saida


print("\n--- 6/7. HARMONIZACAO DO ESQUEMA ---")

partes = {}
tipos = {}  # nome da coluna -> "string" ou "boolean", do primeiro ano em que existe

for ano in ANOS:
    df_ano, originais, mapa = brutos[ano]
    semanticas = montar_semanticas(ano, originais, mapa)
    tecnologias = montar_tecnologias(ano, originais, mapa)

    for nome in semanticas:
        tipos.setdefault(nome, "string")
    for nome in tecnologias:
        tipos.setdefault(nome, "boolean")

    colunas = {}
    colunas.update(semanticas)
    colunas.update(tecnologias)
    partes[ano] = (df_ano, colunas)



# 8. ALINHAMENTO DO ESQUEMA E UNIAO

#
# Coluna que nao existe em um ano vira NULL COM O TIPO CERTO. Sem o cast
# explicito, o unionByName resolveria a coluna toda-nula como void e
# contaminaria o tipo final.


todas = list(tipos.keys())

alinhados = []
for ano in ANOS:
    df_ano, colunas = partes[ano]
    faltando = [c for c in todas if c not in colunas]
    if faltando:
        print(f"  {ano}: {len(faltando)} colunas nao existem nesta pesquisa (preenchidas com NULL)")

    exprs = [F.lit(ano).cast("int").alias("ano_pesquisa")]
    for nome in todas:
        expr = colunas.get(nome)
        if expr is None:
            expr = F.lit(None).cast(tipos[nome])
        exprs.append(expr.alias(nome))

    # UM select por ano, nunca withColumn em laco: com ~150 colunas o plano do
    # Catalyst cresceria a ponto de travar.
    alinhados.append(df_ano.select(*exprs))

df = reduce(lambda a, b: a.unionByName(b), alinhados)

print(f"  esquema unificado: {len(df.columns)} colunas")


# 9. CHAVE E DEDUPLICACAO


df = df.withColumn("id_respondente", F.trim(F.col("id_respondente")))
df = df.withColumn(
    "id_unificado",
    F.concat(F.col("ano_pesquisa").cast("string"), F.lit("_"), F.col("id_respondente")),
)

print("\n--- 9. DEDUPLICACAO ---")
antes = df.count()
df = df.dropDuplicates()
sem_identicas = df.count()
df = df.dropDuplicates(["id_unificado"])
depois = df.count()

print(f"  {antes:,} -> {depois:,} linhas")
print(f"  removidas {antes - sem_identicas} linhas identicas e {sem_identicas - depois} duplicatas de id_unificado")

df = df.cache()



# 10. LIMPEZA, CORRECOES, BOOLEANAS E TEXTO LIVRE


colunas_string = [c.name for c in df.schema.fields if c.dataType.simpleString() == "string"]

exprs = []
for nome in df.columns:
    if nome not in colunas_string:
        exprs.append(F.col(nome))
        continue
    # trim nas pontas; vazio / "." / "-" / "N/A" viram NULL de verdade
    limpo = F.trim(F.col(nome))
    exprs.append(F.when(limpo.isin(VAZIOS), F.lit(None).cast("string")).otherwise(limpo).alias(nome))

df = df.select(*exprs)

# correcoes de digitacao da origem + colunas sim/nao codificadas diferente por ano
exprs = []
for nome in df.columns:
    if nome in CORRECOES_VALOR:
        exprs.append(de_para(F.col(nome), CORRECOES_VALOR[nome]).alias(nome))
    elif nome in COLUNAS_BOOLEANAS_SIM_NAO:
        exprs.append(para_boolean(F.col(nome)).alias(nome))
    else:
        exprs.append(F.col(nome))

df = df.select(*exprs)

# canonicalizacao das respostas abertas, preservando o texto digitado
#
# O respondente digitava a vontade em cloud_preferida / bi_preferida. Casamos por
# slug contra o dominio oficial mais os apelidos conhecidos; o que nao bater vira
# 'Outros' e o texto digitado fica em <coluna>_original.
for nome, opcoes in COLUNAS_TEXTO_LIVRE.items():
    por_slug = {slugify(o): o for o in opcoes}
    por_slug.update(ALIASES_TEXTO_LIVRE.get(nome, {}))

    pares = []
    for chave, valor in por_slug.items():
        pares.extend([F.lit(chave), F.lit(valor)])
    mapa_canon = F.create_map(*pares)

    original = F.col(nome)
    canon = F.when(original.isNull(), F.lit(None).cast("string")).otherwise(
        F.coalesce(mapa_canon[slug_spark(original)], F.lit("Outros"))
    )

    # a coluna canonica fica na posicao original; a _original entra no fim
    exprs = [(canon.alias(nome) if c == nome else F.col(c)) for c in df.columns]
    exprs.append(original.alias(f"{nome}_original"))
    df = df.select(*exprs)

print("\n--- 10. LIMPEZA CONCLUIDA ---")



# 11. COLUNAS DERIVADAS E TIPAGEM FINAL

#
# salario_min / salario_max / salario_medio saem do texto da faixa:
#   'de R$ 4.001/mês a R$ 6.000/mês' -> (4001, 6000, 5000.5)
#   'Menos de R$ 1.000/mês'          -> (0, 1000, 500)
#   'Acima de R$ 40.001/mês'         -> (40001, NULL, 40001)
#
# Na faixa aberta usamos o proprio piso como medio: subestima de proposito,
# em vez de inventar um teto.


_PAT_1 = r"R\$\s*([\d.]+)"
_PAT_2 = r"R\$\s*[\d.]+.*?R\$\s*([\d.]+)"


def _numero(expr):
    return F.when(expr == "", F.lit(None).cast("double")).otherwise(
        F.regexp_replace(expr, r"\.", "").cast("double")
    )


faixa = F.col("faixa_salarial")
baixo = F.lower(F.coalesce(faixa, F.lit("")))
n1 = _numero(F.regexp_extract(faixa, _PAT_1, 1))
n2 = _numero(F.regexp_extract(faixa, _PAT_2, 1))

sal_min = (
    F.when(n1.isNull(), F.lit(None).cast("double"))
    .when(baixo.startswith("menos de"), F.lit(0.0))
    .otherwise(n1)
)
sal_max = (
    F.when(n1.isNull(), F.lit(None).cast("double"))
    .when(baixo.startswith("menos de"), n1)
    .when(baixo.startswith("acima de"), F.lit(None).cast("double"))
    .otherwise(F.coalesce(n2, n1))
)
sal_medio = (
    F.when(sal_min.isNull(), F.lit(None).cast("double"))
    .when(sal_max.isNull(), sal_min)
    .otherwise((sal_min + sal_max) / F.lit(2.0))
)

# usa IA se marcou qualquer opcao do bloco que nao seja "nao uso"
cols_ia = [c for c in df.columns if c.startswith("ia_uso_") and c != "ia_uso_nao_uso"]
if cols_ia:
    usa_ia = reduce(lambda a, b: a | b, [F.coalesce(F.col(c), F.lit(False)) for c in cols_ia])
    respondeu_ia = reduce(
        lambda a, b: a | b, [F.col(c).isNotNull() for c in cols_ia + ["ia_uso_nao_uso"]]
    )
    flag_usa_ia = F.when(respondeu_ia, usa_ia).otherwise(F.lit(None).cast("boolean"))
else:
    flag_usa_ia = F.lit(None).cast("boolean")

derivadas = {
    "idade": F.col("idade").cast("int"),
    "data_envio": F.to_timestamp(F.col("data_envio"), "dd/MM/yyyy HH:mm:ss"),
    "salario_min": sal_min,
    "salario_max": sal_max,
    "salario_medio": sal_medio,
    "faixa_salarial_ordem": ordinal(F.col("faixa_salarial"), ORDEM_FAIXA_SALARIAL),
    "nivel_ordem": ordinal(F.col("nivel"), ORDEM_NIVEL),
    "faixa_idade_ordem": ordinal(F.col("faixa_idade"), ORDEM_FAIXA_IDADE),
    "porte_empresa_ordem": ordinal(F.col("numero_funcionarios"), ORDEM_PORTE_EMPRESA),
    "tempo_experiencia_dados_ordem": ordinal(F.col("tempo_experiencia_dados"), ORDEM_TEMPO_EXPERIENCIA),
    "nivel_comparavel": de_para(F.col("nivel"), NIVEL_COMPARAVEL),
    "flag_empregado": F.when(
        F.col("situacao_trabalho").isNull(), F.lit(None).cast("boolean")
    ).otherwise(F.col("situacao_trabalho").isin(SITUACOES_EMPREGADO)),
    "flag_atua_com_dados": F.col("cargo_atual").isNotNull(),
    "flag_usa_ia": flag_usa_ia,
}

exprs = [
    (derivadas[c].alias(c) if c in derivadas else F.col(c))
    for c in df.columns
]
exprs += [expr.alias(nome) for nome, expr in derivadas.items() if nome not in df.columns]

df = df.select(*exprs)

print("\n--- 11. DERIVADAS ---")
print("  salario_min/max/medio, *_ordem, nivel_comparavel, flag_*")



# 12. ORDEM FINAL DAS COLUNAS


base = [c for c in ORDEM_COLUNAS_BASE if c in df.columns]
tecnologia = sorted(c for c in df.columns if c not in base)
df = df.select(*(base + tecnologia))

df = df.cache()


# 13. VALIDACOES


print("\n============================================================")
print("VALIDACAO")
print("============================================================")

total = df.count()
n_colunas = len(df.columns)
colunas_bool = [c.name for c in df.schema.fields if c.dataType.simpleString() == "boolean"]

# uma unica passada em vez de um job Spark por coluna
checagem = df.agg(
    F.count("*").alias("linhas"),
    F.countDistinct("id_unificado").alias("ids_distintos"),
    F.sum(F.when(F.col("id_unificado").isNull(), 1).otherwise(0)).alias("ids_nulos"),
).collect()[0]

print(f"  linhas            : {total:,}   (esperado {ESPERADO_LINHAS:,})")
print(f"  colunas           : {n_colunas}    (esperado {ESPERADO_COLUNAS})")
print(f"  colunas booleanas : {len(colunas_bool)}")
print(f"  id_unificado unico: {checagem['ids_distintos'] == total}")
print(f"  id_unificado nulo : {checagem['ids_nulos']}")

print("\n  linhas por ano:")
df.groupBy("ano_pesquisa").count().orderBy("ano_pesquisa").show()

print("  distribuicao de nivel_comparavel:")
df.groupBy("nivel_comparavel", "ano_pesquisa").count().orderBy(
    "nivel_comparavel", "ano_pesquisa"
).show(20, truncate=False)

erros = []
if total != ESPERADO_LINHAS:
    erros.append(f"linhas={total}, esperado {ESPERADO_LINHAS}")
if n_colunas != ESPERADO_COLUNAS:
    erros.append(f"colunas={n_colunas}, esperado {ESPERADO_COLUNAS}")
if checagem["ids_distintos"] != total:
    erros.append("id_unificado nao e unico")
if checagem["ids_nulos"]:
    erros.append(f"{checagem['ids_nulos']} id_unificado nulos")

if erros:
    raise Exception("Validacao falhou antes da escrita: " + "; ".join(erros))

print("\n  OK - validacoes passaram, seguindo para a escrita.")


# 14. ESCRITA NA SILVER


s3 = boto3.client("s3")


def limpar_prefixo(prefixo):
    resp = s3.list_objects_v2(Bucket=BUCKET, Prefix=prefixo)
    chaves = [{"Key": o["Key"]} for o in resp.get("Contents", [])]
    if chaves:
        s3.delete_objects(Bucket=BUCKET, Delete={"Objects": chaves})


def consolidar(prefixo_tmp, destino, extensao):
    """Move o unico part-... da pasta temporaria para o nome final e limpa o resto."""
    resp = s3.list_objects_v2(Bucket=BUCKET, Prefix=prefixo_tmp)
    partes_arquivo = [
        o["Key"]
        for o in resp.get("Contents", [])
        if "/part-" in o["Key"] and o["Key"].endswith(extensao)
    ]
    if len(partes_arquivo) != 1:
        raise Exception(f"esperava 1 arquivo em {prefixo_tmp}, achei {len(partes_arquivo)}")

    s3.copy_object(
        Bucket=BUCKET,
        CopySource={"Bucket": BUCKET, "Key": partes_arquivo[0]},
        Key=destino,
    )
    limpar_prefixo(prefixo_tmp)
    tamanho = s3.head_object(Bucket=BUCKET, Key=destino)["ContentLength"]
    print(f"  s3://{BUCKET}/{destino}  ({tamanho / 1024 / 1024:.1f} MB)")


print("\n--- 14. ESCRITA ---")

# --- Parquet (fonte de verdade, tipado) ---
limpar_prefixo(TMP_PARQUET)
df.coalesce(1).write.mode("overwrite").parquet(f"s3://{BUCKET}/{TMP_PARQUET}")
consolidar(TMP_PARQUET, KEY_PARQUET, ".parquet")

# --- CSV (conveniencia: download, Excel, notebook local) ---
exprs_csv = [
    (F.col(c).cast("int").alias(c) if c in colunas_bool else F.col(c)) for c in df.columns
]
limpar_prefixo(TMP_CSV)
(
    df.select(*exprs_csv)
    .coalesce(1)
    .write.mode("overwrite")
    .option("header", True)
    .option("quote", '"')
    .option("escape", '"')  # aspas internas viram "" (padrao RFC4180), nao \"
    .option("nullValue", "")
    .option("encoding", "UTF-8")
    .csv(f"s3://{BUCKET}/{TMP_CSV}")
)
consolidar(TMP_CSV, KEY_CSV, ".csv")
print(f"  {len(colunas_bool)} colunas booleanas gravadas no CSV como 1/0/vazio")



# 15. CATALOGACAO NO GLUE DATA CATALOG

#
# A tabela aponta para a PASTA silver/parquet/ (Location tem que ser diretorio,
# nunca um arquivo), que contem exatamente o nosso unico parquet.


glue = boto3.client("glue")

colunas_catalogo = [
    {"Name": campo.name, "Type": campo.dataType.simpleString()} for campo in df.schema.fields
]

table_input = {
    "Name": TABELA,
    "Description": "Camada silver - State of Data Brasil 2023/2024/2025 unificado",
    "TableType": "EXTERNAL_TABLE",
    "Parameters": {
        "classification": "parquet",
        "EXTERNAL": "TRUE",
        "criado_por": "tentativa1_unificar.py",
    },
    "StorageDescriptor": {
        "Columns": colunas_catalogo,
        "Location": f"s3://{BUCKET}/{PREFIXO_SILVER}/parquet/",
        "InputFormat": "org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat",
        "OutputFormat": "org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat",
        "Compressed": False,
        "SerdeInfo": {
            "SerializationLibrary": "org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe",
            "Parameters": {"serialization.format": "1"},
        },
        "StoredAsSubDirectories": False,
    },
}

try:
    glue.get_table(DatabaseName=DATABASE, Name=TABELA)
    glue.update_table(DatabaseName=DATABASE, TableInput=table_input)
    print(f"\n  tabela {DATABASE}.{TABELA} atualizada ({len(colunas_catalogo)} colunas)")
except glue.exceptions.EntityNotFoundException:
    glue.create_table(DatabaseName=DATABASE, TableInput=table_input)
    print(f"\n  tabela {DATABASE}.{TABELA} criada ({len(colunas_catalogo)} colunas)")



# 16. FINALIZACAO


dt_end = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

print("\n============================================================")
print("FIM DO GLUE JOB")
print("End time:", dt_end)
print("Consulte no Athena:")
print(f'  SELECT * FROM "{DATABASE}"."{TABELA}" LIMIT 10;')
print("============================================================")

job.commit()
