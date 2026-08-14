-- ============================================================================
-- TECH CHALLENGE - FASE 3
-- CAMADA GOLD - FRENTE 5: INTELIGENCIA ARTIFICIAL E OPORTUNIDADES ESTRATEGICAS
-- Motor: Amazon Athena (SQL / CTAS)
-- Origem: tabela parquet da camada Silver
-- Banco de origem: dados_catalogados
-- Bucket: s3://tech-challenge-014478672967/
-- ============================================================================
--
-- COMO EXECUTAR
-- 1. Execute cada bloco separadamente, na ordem numerada.
-- 2. A tabela Silver registrada no Glue Data Catalog e:
--    dados_catalogados.parquet.
-- 3. Sua localizacao fisica confirmada e:
--    s3://tech-challenge-014478672967/silver/parquet/
-- 4. "parquet" e o nome catalogado da tabela, nao apenas a extensao do arquivo.
-- 5. Cada external_location precisa estar vazio na primeira execucao do CTAS.
-- 6. Para uma nova versao, use novos nomes de tabela e caminhos, por exemplo v2.
--
-- TABELAS CRIADAS
-- dados_gold.ft_ia_profissional
-- dados_gold.agg_ia_adocao_anual
-- dados_gold.agg_ia_adocao_segmento
-- dados_gold.agg_ia_maturidade_organizacional
-- dados_gold.agg_ia_salario_senioridade
-- dados_gold.agg_ia_tecnologias_senioridade
-- ============================================================================


-- ============================================================================
-- 00. CONFERENCIA DA TABELA DE ORIGEM
-- ============================================================================

SHOW TABLES IN dados_catalogados;

-- Visualize alguns registros da tabela unificada da camada Silver.

SELECT *
FROM dados_catalogados.parquet
LIMIT 10;


-- ============================================================================
-- 01. CRIACAO DO DATABASE GOLD
-- ============================================================================

CREATE DATABASE IF NOT EXISTS dados_gold;


-- ============================================================================
-- 02. VIEW DE ORIGEM
-- Altere apenas esta view se o nome da tabela Silver no Catalog for diferente.
-- A view tambem impede que todos os CTAS dependam diretamente do nome fisico.
-- ============================================================================

CREATE OR REPLACE VIEW dados_gold.vw_silver_ia_fonte AS
SELECT *
FROM dados_catalogados.parquet;


-- ============================================================================
-- 03. PERFIL DOS CAMPOS TEXTUAIS DE IA
-- Execute antes dos CTAS para conhecer as categorias originais e a cobertura.
-- Nao transforma respostas textuais em flags sem conhecer o significado real.
-- ============================================================================

SELECT
    ano_pesquisa,
    'ia_generativa_prioridade' AS indicador,
    ia_generativa_prioridade AS resposta,
    COUNT(*) AS quantidade
FROM dados_gold.vw_silver_ia_fonte
GROUP BY ano_pesquisa, ia_generativa_prioridade

UNION ALL

SELECT
    ano_pesquisa,
    'ia_bons_resultados_llm' AS indicador,
    ia_bons_resultados_llm AS resposta,
    COUNT(*) AS quantidade
FROM dados_gold.vw_silver_ia_fonte
GROUP BY ano_pesquisa, ia_bons_resultados_llm

UNION ALL

SELECT
    ano_pesquisa,
    'tipo_uso_ia_empresa' AS indicador,
    tipo_uso_ia_empresa AS resposta,
    COUNT(*) AS quantidade
FROM dados_gold.vw_silver_ia_fonte
GROUP BY ano_pesquisa, tipo_uso_ia_empresa

UNION ALL

SELECT
    ano_pesquisa,
    'usa_llm_trabalho' AS indicador,
    usa_llm_trabalho AS resposta,
    COUNT(*) AS quantidade
FROM dados_gold.vw_silver_ia_fonte
GROUP BY ano_pesquisa, usa_llm_trabalho

ORDER BY 1, 2, 4 DESC;


-- ============================================================================
-- 04. FATO ANALITICO INDIVIDUAL DE IA
-- Grao: uma linha por id_unificado/respondente/ano.
-- Enriquece a Silver com perfil de acesso e quantidade de tecnologias.
-- A tabela e particionada por ano_pesquisa; por isso o ano e a ultima coluna.
-- ============================================================================

CREATE TABLE dados_gold.ft_ia_profissional
WITH (
    format = 'PARQUET',
    write_compression = 'SNAPPY',
    external_location = 's3://tech-challenge-014478672967/gold/frente_5_ia/ft_ia_profissional_v1/',
    partitioned_by = ARRAY['ano_pesquisa']
) AS

WITH base AS (
    SELECT
        id_unificado,
        id_respondente,
        data_envio,

        idade,
        NULLIF(TRIM(faixa_idade), '') AS faixa_idade,
        faixa_idade_ordem,
        NULLIF(TRIM(genero), '') AS genero,
        NULLIF(TRIM(cor_raca_etnia), '') AS cor_raca_etnia,
        NULLIF(TRIM(pcd), '') AS pcd,
        vive_no_brasil,
        NULLIF(TRIM(pais_onde_mora), '') AS pais_onde_mora,
        NULLIF(TRIM(estado_onde_mora), '') AS estado_onde_mora,
        NULLIF(TRIM(uf_onde_mora), '') AS uf_onde_mora,
        NULLIF(TRIM(regiao_onde_mora), '') AS regiao_onde_mora,
        NULLIF(TRIM(nivel_ensino), '') AS nivel_ensino,
        NULLIF(TRIM(area_formacao), '') AS area_formacao,

        NULLIF(TRIM(situacao_trabalho), '') AS situacao_trabalho,
        flag_empregado,
        NULLIF(TRIM(setor), '') AS setor,
        NULLIF(TRIM(numero_funcionarios), '') AS numero_funcionarios,
        porte_empresa_ordem,
        atua_como_gestor,
        NULLIF(TRIM(cargo_como_gestor), '') AS cargo_como_gestor,
        NULLIF(TRIM(cargo_atual), '') AS cargo_atual,
        NULLIF(TRIM(funcao_atuacao), '') AS funcao_atuacao,
        flag_atua_com_dados,
        NULLIF(TRIM(nivel), '') AS nivel,
        NULLIF(TRIM(nivel_comparavel), '') AS nivel_comparavel,
        nivel_ordem,
        NULLIF(TRIM(tempo_experiencia_dados), '') AS tempo_experiencia_dados,
        tempo_experiencia_dados_ordem,
        NULLIF(TRIM(tempo_experiencia_ti), '') AS tempo_experiencia_ti,
        NULLIF(TRIM(faixa_salarial), '') AS faixa_salarial,
        faixa_salarial_ordem,
        salario_min,
        salario_max,
        salario_medio,
        NULLIF(TRIM(modelo_trabalho_atual), '') AS modelo_trabalho_atual,
        NULLIF(TRIM(modelo_trabalho_ideal), '') AS modelo_trabalho_ideal,

        NULLIF(TRIM(ia_generativa_prioridade), '') AS ia_generativa_prioridade,
        NULLIF(TRIM(ia_bons_resultados_llm), '') AS ia_bons_resultados_llm,
        NULLIF(TRIM(tipo_uso_ia_empresa), '') AS tipo_uso_ia_empresa,
        NULLIF(TRIM(usa_llm_trabalho), '') AS usa_llm_trabalho,

        -- Classificacao da prioridade estrategica. As cinco categorias foram
        -- confirmadas no perfil textual exportado do Athena.
        CASE
            WHEN ia_generativa_prioridade IS NULL
                 OR TRIM(ia_generativa_prioridade) = '' THEN NULL
            WHEN LOWER(ia_generativa_prioridade) LIKE 'sim, é nossa principal prioridade%'
                THEN '4 - Principal prioridade'
            WHEN LOWER(ia_generativa_prioridade) LIKE 'sim, está entre nossas principais prioridades%'
                THEN '3 - Entre as principais prioridades'
            WHEN LOWER(ia_generativa_prioridade) LIKE 'mais ou menos%'
                THEN '2 - Iniciativas isoladas, pouco foco'
            WHEN LOWER(ia_generativa_prioridade) LIKE 'não é uma iniciativa%'
                THEN '1 - Nao e prioridade'
            WHEN LOWER(ia_generativa_prioridade) LIKE 'não sei opinar%'
                THEN '0 - Nao sabe opinar'
            ELSE 'Outros'
        END AS categoria_prioridade_ia,

        CASE
            WHEN ia_generativa_prioridade IS NULL
                 OR TRIM(ia_generativa_prioridade) = '' THEN NULL
            WHEN LOWER(ia_generativa_prioridade) LIKE 'sim, é nossa principal prioridade%'
                 OR LOWER(ia_generativa_prioridade) LIKE 'sim, está entre nossas principais prioridades%'
                THEN 1
            WHEN LOWER(ia_generativa_prioridade) LIKE 'não sei opinar%'
                THEN NULL
            ELSE 0
        END AS flag_prioridade_estrategica,

        -- A pergunta sobre resultados com LLM so possui respostas em 2025.
        CASE
            WHEN ia_bons_resultados_llm IS NULL
                 OR TRIM(ia_bons_resultados_llm) = '' THEN NULL
            WHEN LOWER(ia_bons_resultados_llm) LIKE 'sim. temos projetos%'
                THEN '4 - Producao com impacto no negocio'
            WHEN LOWER(ia_bons_resultados_llm) LIKE 'em partes%'
                THEN '3 - Pilotos com pouco impacto'
            WHEN LOWER(ia_bons_resultados_llm) LIKE 'não. os projetos%'
                THEN '2 - Investigacao e planejamento'
            WHEN LOWER(ia_bons_resultados_llm) LIKE 'não, ainda não começamos%'
                THEN '1 - Nenhum projeto iniciado'
            WHEN LOWER(ia_bons_resultados_llm) LIKE 'não sei opinar%'
                THEN '0 - Nao sabe opinar'
            ELSE 'Outros'
        END AS categoria_resultado_llm,

        CASE
            WHEN ia_bons_resultados_llm IS NULL
                 OR TRIM(ia_bons_resultados_llm) = '' THEN NULL
            WHEN LOWER(ia_bons_resultados_llm) LIKE 'sim. temos projetos%'
                THEN 1
            WHEN LOWER(ia_bons_resultados_llm) LIKE 'não sei opinar%'
                THEN NULL
            ELSE 0
        END AS flag_resultado_positivo_llm,

        -- tipo_uso_ia_empresa e multisselecao. Cada flag identifica a presenca
        -- de um tema; portanto, varias flags podem valer 1 no mesmo registro.
        CASE
            WHEN tipo_uso_ia_empresa IS NULL OR TRIM(tipo_uso_ia_empresa) = '' THEN NULL
            WHEN LOWER(tipo_uso_ia_empresa) LIKE '%forma independente%'
                THEN 1 ELSE 0
        END AS flag_uso_ia_independente,

        CASE
            WHEN tipo_uso_ia_empresa IS NULL OR TRIM(tipo_uso_ia_empresa) = '' THEN NULL
            WHEN LOWER(tipo_uso_ia_empresa) LIKE '%existe um direcionamento centralizado%'
                THEN 1 ELSE 0
        END AS flag_ia_direcionamento_centralizado,

        CASE
            WHEN tipo_uso_ia_empresa IS NULL OR TRIM(tipo_uso_ia_empresa) = '' THEN NULL
            WHEN LOWER(tipo_uso_ia_empresa) LIKE '%equipes de desenvolvimento utilizando soluções%'
                THEN 1 ELSE 0
        END AS flag_ia_desenvolvimento_codigo,

        CASE
            WHEN tipo_uso_ia_empresa IS NULL OR TRIM(tipo_uso_ia_empresa) = '' THEN NULL
            WHEN LOWER(tipo_uso_ia_empresa) LIKE '%aumentar a eficiencia de processos internos%'
                THEN 1 ELSE 0
        END AS flag_ia_eficiencia_processos,

        CASE
            WHEN tipo_uso_ia_empresa IS NULL OR TRIM(tipo_uso_ia_empresa) = '' THEN NULL
            WHEN LOWER(tipo_uso_ia_empresa) LIKE '%propor melhorias e inovações%'
                THEN 1 ELSE 0
        END AS flag_ia_inovacao_produtos,

        CASE
            WHEN tipo_uso_ia_empresa IS NULL OR TRIM(tipo_uso_ia_empresa) = '' THEN NULL
            WHEN LOWER(tipo_uso_ia_empresa) LIKE '%principal frente do negócio%'
                THEN 1 ELSE 0
        END AS flag_ia_principal_negocio,

        CASE
            WHEN tipo_uso_ia_empresa IS NULL OR TRIM(tipo_uso_ia_empresa) = '' THEN NULL
            WHEN LOWER(tipo_uso_ia_empresa) LIKE '%não tenho visto soluções%'
                THEN 1 ELSE 0
        END AS flag_ia_casos_isolados_inicio,

        CASE
            WHEN tipo_uso_ia_empresa IS NULL OR TRIM(tipo_uso_ia_empresa) = '' THEN NULL
            WHEN LOWER(tipo_uso_ia_empresa) LIKE '%não sei opinar%'
                THEN 1 ELSE 0
        END AS flag_ia_nao_sabe_empresa,

        -- No Parquet, estas flags podem estar catalogadas como BOOLEAN. A
        -- conversao por VARCHAR tambem aceita fontes antigas com 0/1.
        CASE
            WHEN LOWER(CAST(flag_usa_ia AS VARCHAR)) IN ('true', '1') THEN 1
            WHEN LOWER(CAST(flag_usa_ia AS VARCHAR)) IN ('false', '0') THEN 0
        END AS flag_usa_ia,
        CASE
            WHEN LOWER(CAST(ia_uso_copilot AS VARCHAR)) IN ('true', '1') THEN 1
            WHEN LOWER(CAST(ia_uso_copilot AS VARCHAR)) IN ('false', '0') THEN 0
        END AS ia_uso_copilot,
        CASE
            WHEN LOWER(CAST(ia_uso_gratuitas AS VARCHAR)) IN ('true', '1') THEN 1
            WHEN LOWER(CAST(ia_uso_gratuitas AS VARCHAR)) IN ('false', '0') THEN 0
        END AS ia_uso_gratuitas,
        CASE
            WHEN LOWER(CAST(ia_uso_nao_uso AS VARCHAR)) IN ('true', '1') THEN 1
            WHEN LOWER(CAST(ia_uso_nao_uso AS VARCHAR)) IN ('false', '0') THEN 0
        END AS ia_uso_nao_uso,
        CASE
            WHEN LOWER(CAST(ia_uso_pagas_pela_empresa AS VARCHAR)) IN ('true', '1') THEN 1
            WHEN LOWER(CAST(ia_uso_pagas_pela_empresa AS VARCHAR)) IN ('false', '0') THEN 0
        END AS ia_uso_pagas_pela_empresa,
        CASE
            WHEN LOWER(CAST(ia_uso_pagas_proprio_bolso AS VARCHAR)) IN ('true', '1') THEN 1
            WHEN LOWER(CAST(ia_uso_pagas_proprio_bolso AS VARCHAR)) IN ('false', '0') THEN 0
        END AS ia_uso_pagas_proprio_bolso,

        -- Quantidade de ferramentas de BI marcadas pelo respondente.
        -- As flags "nao utilizo nenhuma" nao entram na soma.
        IF(LOWER(CAST(tec_bi_alteryx AS VARCHAR)) IN ('true', '1'), 1, 0)
        + IF(LOWER(CAST(tec_bi_amazon_quicksight AS VARCHAR)) IN ('true', '1'), 1, 0)
        + IF(LOWER(CAST(tec_bi_apenas_excel_ou_planilhas AS VARCHAR)) IN ('true', '1'), 1, 0)
        + IF(LOWER(CAST(tec_bi_birst AS VARCHAR)) IN ('true', '1'), 1, 0)
        + IF(LOWER(CAST(tec_bi_grafana AS VARCHAR)) IN ('true', '1'), 1, 0)
        + IF(LOWER(CAST(tec_bi_ibm_analytics_cognos AS VARCHAR)) IN ('true', '1'), 1, 0)
        + IF(LOWER(CAST(tec_bi_looker AS VARCHAR)) IN ('true', '1'), 1, 0)
        + IF(LOWER(CAST(tec_bi_looker_studio AS VARCHAR)) IN ('true', '1'), 1, 0)
        + IF(LOWER(CAST(tec_bi_metabase AS VARCHAR)) IN ('true', '1'), 1, 0)
        + IF(LOWER(CAST(tec_bi_microsoft_powerbi AS VARCHAR)) IN ('true', '1'), 1, 0)
        + IF(LOWER(CAST(tec_bi_microstrategy AS VARCHAR)) IN ('true', '1'), 1, 0)
        + IF(LOWER(CAST(tec_bi_mode AS VARCHAR)) IN ('true', '1'), 1, 0)
        + IF(LOWER(CAST(tec_bi_oracle_business_intelligence AS VARCHAR)) IN ('true', '1'), 1, 0)
        + IF(LOWER(CAST(tec_bi_pentaho AS VARCHAR)) IN ('true', '1'), 1, 0)
        + IF(LOWER(CAST(tec_bi_qlik_view_qlik_sense AS VARCHAR)) IN ('true', '1'), 1, 0)
        + IF(LOWER(CAST(tec_bi_redash AS VARCHAR)) IN ('true', '1'), 1, 0)
        + IF(LOWER(CAST(tec_bi_salesforce_einstein_analytics AS VARCHAR)) IN ('true', '1'), 1, 0)
        + IF(LOWER(CAST(tec_bi_sap_business_objects_sap_analytics AS VARCHAR)) IN ('true', '1'), 1, 0)
        + IF(LOWER(CAST(tec_bi_sas_visual_analytics AS VARCHAR)) IN ('true', '1'), 1, 0)
        + IF(LOWER(CAST(tec_bi_superset AS VARCHAR)) IN ('true', '1'), 1, 0)
        + IF(LOWER(CAST(tec_bi_tableau AS VARCHAR)) IN ('true', '1'), 1, 0)
        + IF(LOWER(CAST(tec_bi_tibco_spotfire AS VARCHAR)) IN ('true', '1'), 1, 0) AS qtd_tecnologias_bi,

        -- Quantidade de ambientes/plataformas de cloud.
        IF(LOWER(CAST(tec_cloud_amazon_web_services_aws AS VARCHAR)) IN ('true', '1'), 1, 0)
        + IF(LOWER(CAST(tec_cloud_azure_microsoft AS VARCHAR)) IN ('true', '1'), 1, 0)
        + IF(LOWER(CAST(tec_cloud_cloud_propria AS VARCHAR)) IN ('true', '1'), 1, 0)
        + IF(LOWER(CAST(tec_cloud_google_cloud_gcp AS VARCHAR)) IN ('true', '1'), 1, 0)
        + IF(LOWER(CAST(tec_cloud_ibm AS VARCHAR)) IN ('true', '1'), 1, 0)
        + IF(LOWER(CAST(tec_cloud_on_premise AS VARCHAR)) IN ('true', '1'), 1, 0)
        + IF(LOWER(CAST(tec_cloud_oracle_cloud AS VARCHAR)) IN ('true', '1'), 1, 0) AS qtd_tecnologias_cloud,

        -- Quantidade de bancos, data platforms e mecanismos de consulta.
        IF(LOWER(CAST(tec_db_amazon_athena AS VARCHAR)) IN ('true', '1'), 1, 0)
        + IF(LOWER(CAST(tec_db_amazon_aurora_ou_rds AS VARCHAR)) IN ('true', '1'), 1, 0)
        + IF(LOWER(CAST(tec_db_amazon_redshift AS VARCHAR)) IN ('true', '1'), 1, 0)
        + IF(LOWER(CAST(tec_db_cassandra AS VARCHAR)) IN ('true', '1'), 1, 0)
        + IF(LOWER(CAST(tec_db_coachdb AS VARCHAR)) IN ('true', '1'), 1, 0)
        + IF(LOWER(CAST(tec_db_databricks AS VARCHAR)) IN ('true', '1'), 1, 0)
        + IF(LOWER(CAST(tec_db_datomic AS VARCHAR)) IN ('true', '1'), 1, 0)
        + IF(LOWER(CAST(tec_db_db2 AS VARCHAR)) IN ('true', '1'), 1, 0)
        + IF(LOWER(CAST(tec_db_dynamodb AS VARCHAR)) IN ('true', '1'), 1, 0)
        + IF(LOWER(CAST(tec_db_elasticsearch AS VARCHAR)) IN ('true', '1'), 1, 0)
        + IF(LOWER(CAST(tec_db_firebase AS VARCHAR)) IN ('true', '1'), 1, 0)
        + IF(LOWER(CAST(tec_db_firebird AS VARCHAR)) IN ('true', '1'), 1, 0)
        + IF(LOWER(CAST(tec_db_google_bigquery AS VARCHAR)) IN ('true', '1'), 1, 0)
        + IF(LOWER(CAST(tec_db_google_firestore AS VARCHAR)) IN ('true', '1'), 1, 0)
        + IF(LOWER(CAST(tec_db_hbase AS VARCHAR)) IN ('true', '1'), 1, 0)
        + IF(LOWER(CAST(tec_db_hive AS VARCHAR)) IN ('true', '1'), 1, 0)
        + IF(LOWER(CAST(tec_db_mariadb AS VARCHAR)) IN ('true', '1'), 1, 0)
        + IF(LOWER(CAST(tec_db_microsoft_access AS VARCHAR)) IN ('true', '1'), 1, 0)
        + IF(LOWER(CAST(tec_db_mongodb AS VARCHAR)) IN ('true', '1'), 1, 0)
        + IF(LOWER(CAST(tec_db_mysql AS VARCHAR)) IN ('true', '1'), 1, 0)
        + IF(LOWER(CAST(tec_db_neo4j AS VARCHAR)) IN ('true', '1'), 1, 0)
        + IF(LOWER(CAST(tec_db_oracle AS VARCHAR)) IN ('true', '1'), 1, 0)
        + IF(LOWER(CAST(tec_db_postgresql AS VARCHAR)) IN ('true', '1'), 1, 0)
        + IF(LOWER(CAST(tec_db_presto AS VARCHAR)) IN ('true', '1'), 1, 0)
        + IF(LOWER(CAST(tec_db_redis AS VARCHAR)) IN ('true', '1'), 1, 0)
        + IF(LOWER(CAST(tec_db_s3 AS VARCHAR)) IN ('true', '1'), 1, 0)
        + IF(LOWER(CAST(tec_db_sap_hana AS VARCHAR)) IN ('true', '1'), 1, 0)
        + IF(LOWER(CAST(tec_db_snowflake AS VARCHAR)) IN ('true', '1'), 1, 0)
        + IF(LOWER(CAST(tec_db_splunk AS VARCHAR)) IN ('true', '1'), 1, 0)
        + IF(LOWER(CAST(tec_db_sql_server AS VARCHAR)) IN ('true', '1'), 1, 0)
        + IF(LOWER(CAST(tec_db_sqlite AS VARCHAR)) IN ('true', '1'), 1, 0)
        + IF(LOWER(CAST(tec_db_sybase AS VARCHAR)) IN ('true', '1'), 1, 0)
        + IF(LOWER(CAST(tec_db_vertica AS VARCHAR)) IN ('true', '1'), 1, 0) AS qtd_tecnologias_banco,

        -- Quantidade de linguagens. A normalizacao aceita BOOLEAN, 0/1 e
        -- string. A flag "nao utilizo nenhuma" nao entra na soma.
        IF(LOWER(CAST(tec_lang_c_c_c AS VARCHAR)) IN ('true', '1'), 1, 0)
        + IF(LOWER(CAST(tec_lang_dax AS VARCHAR)) IN ('true', '1'), 1, 0)
        + IF(LOWER(CAST(tec_lang_java AS VARCHAR)) IN ('true', '1'), 1, 0)
        + IF(LOWER(CAST(tec_lang_javascript AS VARCHAR)) IN ('true', '1'), 1, 0)
        + IF(LOWER(CAST(tec_lang_julia AS VARCHAR)) IN ('true', '1'), 1, 0)
        + IF(LOWER(CAST(tec_lang_matlab AS VARCHAR)) IN ('true', '1'), 1, 0)
        + IF(LOWER(CAST(tec_lang_net AS VARCHAR)) IN ('true', '1'), 1, 0)
        + IF(LOWER(CAST(tec_lang_php AS VARCHAR)) IN ('true', '1'), 1, 0)
        + IF(LOWER(CAST(tec_lang_python AS VARCHAR)) IN ('true', '1'), 1, 0)
        + IF(LOWER(CAST(tec_lang_r AS VARCHAR)) IN ('true', '1'), 1, 0)
        + IF(LOWER(CAST(tec_lang_rust AS VARCHAR)) IN ('true', '1'), 1, 0)
        + IF(LOWER(CAST(tec_lang_sas_stata AS VARCHAR)) IN ('true', '1'), 1, 0)
        + IF(LOWER(CAST(tec_lang_scala AS VARCHAR)) IN ('true', '1'), 1, 0)
        + IF(LOWER(CAST(tec_lang_sql AS VARCHAR)) IN ('true', '1'), 1, 0)
        + IF(LOWER(CAST(tec_lang_visual_basic_vba AS VARCHAR)) IN ('true', '1'), 1, 0) AS qtd_tecnologias_linguagem,

        CAST(ano_pesquisa AS BIGINT) AS ano_pesquisa

    FROM dados_gold.vw_silver_ia_fonte
)

SELECT
    id_unificado,
    id_respondente,
    data_envio,
    idade,
    faixa_idade,
    faixa_idade_ordem,
    genero,
    cor_raca_etnia,
    pcd,
    vive_no_brasil,
    pais_onde_mora,
    estado_onde_mora,
    uf_onde_mora,
    regiao_onde_mora,
    nivel_ensino,
    area_formacao,
    situacao_trabalho,
    flag_empregado,
    setor,
    numero_funcionarios,
    porte_empresa_ordem,
    atua_como_gestor,
    cargo_como_gestor,
    cargo_atual,
    funcao_atuacao,
    flag_atua_com_dados,
    nivel,
    nivel_comparavel,
    nivel_ordem,
    tempo_experiencia_dados,
    tempo_experiencia_dados_ordem,
    tempo_experiencia_ti,
    faixa_salarial,
    faixa_salarial_ordem,
    salario_min,
    salario_max,
    salario_medio,
    modelo_trabalho_atual,
    modelo_trabalho_ideal,
    ia_generativa_prioridade,
    ia_bons_resultados_llm,
    tipo_uso_ia_empresa,
    usa_llm_trabalho,
    categoria_prioridade_ia,
    flag_prioridade_estrategica,
    categoria_resultado_llm,
    flag_resultado_positivo_llm,
    flag_uso_ia_independente,
    flag_ia_direcionamento_centralizado,
    flag_ia_desenvolvimento_codigo,
    flag_ia_eficiencia_processos,
    flag_ia_inovacao_produtos,
    flag_ia_principal_negocio,
    flag_ia_casos_isolados_inicio,
    flag_ia_nao_sabe_empresa,
    flag_usa_ia,
    ia_uso_copilot,
    ia_uso_gratuitas,
    ia_uso_nao_uso,
    ia_uso_pagas_pela_empresa,
    ia_uso_pagas_proprio_bolso,

    CASE
        WHEN flag_usa_ia = 0 OR ia_uso_nao_uso = 1
            THEN 'Nao utiliza IA'
        WHEN flag_usa_ia = 1
             AND ia_uso_pagas_pela_empresa = 1
             AND ia_uso_pagas_proprio_bolso = 1
            THEN 'Financiamento misto'
        WHEN flag_usa_ia = 1 AND ia_uso_pagas_pela_empresa = 1
            THEN 'Financiada pela empresa'
        WHEN flag_usa_ia = 1 AND ia_uso_pagas_proprio_bolso = 1
            THEN 'Autofinanciada'
        WHEN flag_usa_ia = 1
             AND (ia_uso_gratuitas = 1 OR ia_uso_copilot = 1)
            THEN 'Gratuita ou Copilot'
        WHEN flag_usa_ia = 1
            THEN 'Modalidade nao identificada'
        ELSE NULL
    END AS perfil_acesso_ia,

    CASE
        WHEN flag_usa_ia = 1 AND ia_uso_pagas_pela_empresa = 1 THEN 1
        WHEN flag_usa_ia IN (0, 1) THEN 0
    END AS flag_apoio_institucional,

    CASE
        WHEN flag_usa_ia = 1
             AND ia_uso_pagas_proprio_bolso = 1
             AND COALESCE(ia_uso_pagas_pela_empresa, 0) = 0 THEN 1
        WHEN flag_usa_ia IN (0, 1) THEN 0
    END AS flag_autofinanciamento_exclusivo,

    CASE
        WHEN tipo_uso_ia_empresa IS NULL THEN NULL
        WHEN COALESCE(flag_ia_direcionamento_centralizado, 0) = 1
          OR COALESCE(flag_ia_desenvolvimento_codigo, 0) = 1
          OR COALESCE(flag_ia_eficiencia_processos, 0) = 1
          OR COALESCE(flag_ia_inovacao_produtos, 0) = 1
          OR COALESCE(flag_ia_principal_negocio, 0) = 1
            THEN 1
        ELSE 0
    END AS flag_uso_ia_organizacional,

    -- Nivel mais avancado identificado entre as alternativas selecionadas.
    CASE
        WHEN tipo_uso_ia_empresa IS NULL THEN NULL
        WHEN flag_ia_principal_negocio = 1
            THEN '5 - Estrategica ou transformadora'
        WHEN flag_ia_direcionamento_centralizado = 1
            THEN '4 - Institucionalizada'
        WHEN flag_ia_eficiencia_processos = 1 OR flag_ia_inovacao_produtos = 1
            THEN '3 - Aplicada a processos ou produtos'
        WHEN flag_ia_desenvolvimento_codigo = 1
            THEN '2 - Departamental ou tecnica'
        WHEN flag_uso_ia_independente = 1
            THEN '1 - Individual sem centralizacao'
        WHEN flag_ia_casos_isolados_inicio = 1
            THEN '0 - Inicial ou sem prioridade'
        WHEN flag_ia_nao_sabe_empresa = 1
            THEN 'Nao sabe opinar'
        ELSE 'Outros'
    END AS categoria_maturidade_ia_empresa,

    COALESCE(ia_uso_copilot, 0)
    + COALESCE(ia_uso_gratuitas, 0)
    + COALESCE(ia_uso_pagas_pela_empresa, 0)
    + COALESCE(ia_uso_pagas_proprio_bolso, 0) AS qtd_modalidades_acesso_ia,

    qtd_tecnologias_bi,
    qtd_tecnologias_cloud,
    qtd_tecnologias_banco,
    qtd_tecnologias_linguagem,
    qtd_tecnologias_bi
        + qtd_tecnologias_cloud
        + qtd_tecnologias_banco
        + qtd_tecnologias_linguagem AS qtd_tecnologias_total,

    ano_pesquisa

FROM base;


-- ============================================================================
-- 05. AGREGADO DE ADOCAO ANUAL
-- Grao: uma linha por ano.
-- Os percentuais de modalidades podem somar mais de 100%, pois a pesquisa
-- permite selecionar mais de uma forma de acesso.
-- ============================================================================

CREATE TABLE dados_gold.agg_ia_adocao_anual
WITH (
    format = 'PARQUET',
    write_compression = 'SNAPPY',
    external_location = 's3://tech-challenge-014478672967/gold/frente_5_ia/agg_ia_adocao_anual_v1/'
) AS

WITH contagens AS (
    SELECT
        ano_pesquisa,
        COUNT(*) AS total_registros,
        COUNT(flag_usa_ia) AS respostas_validas_uso_ia,
        SUM(CASE WHEN flag_usa_ia = 1 THEN 1 ELSE 0 END) AS usuarios_ia,
        SUM(CASE WHEN flag_usa_ia = 0 THEN 1 ELSE 0 END) AS nao_usuarios_ia,
        SUM(CASE WHEN flag_usa_ia = 1 AND ia_uso_copilot = 1 THEN 1 ELSE 0 END)
            AS usuarios_copilot,
        SUM(CASE WHEN flag_usa_ia = 1 AND ia_uso_gratuitas = 1 THEN 1 ELSE 0 END)
            AS usuarios_ferramentas_gratuitas,
        SUM(CASE WHEN flag_usa_ia = 1 AND ia_uso_pagas_pela_empresa = 1 THEN 1 ELSE 0 END)
            AS usuarios_com_ferramenta_paga_empresa,
        SUM(CASE WHEN flag_usa_ia = 1 AND ia_uso_pagas_proprio_bolso = 1 THEN 1 ELSE 0 END)
            AS usuarios_que_pagaram_proprio_bolso,
        SUM(CASE WHEN flag_autofinanciamento_exclusivo = 1 THEN 1 ELSE 0 END)
            AS usuarios_autofinanciamento_exclusivo
    FROM dados_gold.ft_ia_profissional
    GROUP BY ano_pesquisa
)

SELECT
    ano_pesquisa,
    total_registros,
    respostas_validas_uso_ia,
    usuarios_ia,
    nao_usuarios_ia,
    usuarios_copilot,
    usuarios_ferramentas_gratuitas,
    usuarios_com_ferramenta_paga_empresa,
    usuarios_que_pagaram_proprio_bolso,
    usuarios_autofinanciamento_exclusivo,

    ROUND(
        100.0 * respostas_validas_uso_ia / NULLIF(total_registros, 0),
        2
    ) AS pct_cobertura_pergunta_uso_ia,

    ROUND(
        100.0 * usuarios_ia / NULLIF(respostas_validas_uso_ia, 0),
        2
    ) AS pct_adocao_ia,

    ROUND(
        100.0 * usuarios_copilot / NULLIF(usuarios_ia, 0),
        2
    ) AS pct_copilot_entre_usuarios_ia,

    ROUND(
        100.0 * usuarios_ferramentas_gratuitas / NULLIF(usuarios_ia, 0),
        2
    ) AS pct_gratuitas_entre_usuarios_ia,

    ROUND(
        100.0 * usuarios_com_ferramenta_paga_empresa / NULLIF(usuarios_ia, 0),
        2
    ) AS pct_paga_empresa_entre_usuarios_ia,

    ROUND(
        100.0 * usuarios_que_pagaram_proprio_bolso / NULLIF(usuarios_ia, 0),
        2
    ) AS pct_paga_proprio_entre_usuarios_ia,

    ROUND(
        100.0 * usuarios_autofinanciamento_exclusivo / NULLIF(usuarios_ia, 0),
        2
    ) AS pct_autofinanciamento_exclusivo_entre_usuarios,

    ROUND(
        100.0 * usuarios_com_ferramenta_paga_empresa
        / NULLIF(respostas_validas_uso_ia, 0),
        2
    ) AS pct_apoio_institucional_total,

    -- Diferenca, em pontos percentuais, entre adocao e apoio empresarial.
    ROUND(
        100.0 * (usuarios_ia - usuarios_com_ferramenta_paga_empresa)
        / NULLIF(respostas_validas_uso_ia, 0),
        2
    ) AS gap_adocao_apoio_pp

FROM contagens;


-- ============================================================================
-- 06. AGREGADO DE ADOCAO POR SEGMENTO
-- Grao: ano + tipo_dimensao + categoria.
-- Dimensoes: senioridade, regiao, modelo de trabalho e cargo.
-- ============================================================================

CREATE TABLE dados_gold.agg_ia_adocao_segmento
WITH (
    format = 'PARQUET',
    write_compression = 'SNAPPY',
    external_location = 's3://tech-challenge-014478672967/gold/frente_5_ia/agg_ia_adocao_segmento_v1/'
) AS

WITH segmentos AS (
    SELECT
        ano_pesquisa,
        'Senioridade' AS tipo_dimensao,
        nivel_comparavel AS categoria,
        flag_usa_ia,
        flag_apoio_institucional,
        flag_autofinanciamento_exclusivo
    FROM dados_gold.ft_ia_profissional

    UNION ALL

    SELECT
        ano_pesquisa,
        'Regiao' AS tipo_dimensao,
        regiao_onde_mora AS categoria,
        flag_usa_ia,
        flag_apoio_institucional,
        flag_autofinanciamento_exclusivo
    FROM dados_gold.ft_ia_profissional

    UNION ALL

    SELECT
        ano_pesquisa,
        'Modelo de trabalho' AS tipo_dimensao,
        modelo_trabalho_atual AS categoria,
        flag_usa_ia,
        flag_apoio_institucional,
        flag_autofinanciamento_exclusivo
    FROM dados_gold.ft_ia_profissional

    UNION ALL

    SELECT
        ano_pesquisa,
        'Cargo' AS tipo_dimensao,
        cargo_atual AS categoria,
        flag_usa_ia,
        flag_apoio_institucional,
        flag_autofinanciamento_exclusivo
    FROM dados_gold.ft_ia_profissional
),

contagens AS (
    SELECT
        ano_pesquisa,
        tipo_dimensao,
        categoria,
        COUNT(*) AS total_registros,
        COUNT(flag_usa_ia) AS respostas_validas_uso_ia,
        SUM(CASE WHEN flag_usa_ia = 1 THEN 1 ELSE 0 END) AS usuarios_ia,
        SUM(CASE WHEN flag_apoio_institucional = 1 THEN 1 ELSE 0 END)
            AS usuarios_com_apoio_empresa,
        SUM(CASE WHEN flag_autofinanciamento_exclusivo = 1 THEN 1 ELSE 0 END)
            AS usuarios_autofinanciamento_exclusivo
    FROM segmentos
    WHERE categoria IS NOT NULL
    GROUP BY ano_pesquisa, tipo_dimensao, categoria
)

SELECT
    ano_pesquisa,
    tipo_dimensao,
    categoria,
    total_registros,
    respostas_validas_uso_ia,
    usuarios_ia,
    usuarios_com_apoio_empresa,
    usuarios_autofinanciamento_exclusivo,

    ROUND(
        100.0 * respostas_validas_uso_ia / NULLIF(total_registros, 0),
        2
    ) AS pct_cobertura_pergunta_uso_ia,

    ROUND(
        100.0 * usuarios_ia / NULLIF(respostas_validas_uso_ia, 0),
        2
    ) AS pct_adocao_ia,

    ROUND(
        100.0 * usuarios_com_apoio_empresa / NULLIF(usuarios_ia, 0),
        2
    ) AS pct_apoio_empresa_entre_usuarios,

    ROUND(
        100.0 * usuarios_autofinanciamento_exclusivo / NULLIF(usuarios_ia, 0),
        2
    ) AS pct_autofinanciamento_exclusivo_entre_usuarios,

    ROUND(
        100.0 * (usuarios_ia - usuarios_com_apoio_empresa)
        / NULLIF(respostas_validas_uso_ia, 0),
        2
    ) AS gap_adocao_apoio_pp

FROM contagens;


-- ============================================================================
-- 07. AGREGADO DE MATURIDADE ORGANIZACIONAL
-- Grao: ano + indicador + categoria analitica.
-- Prioridade, resultados, maturidade e acesso sao distribuicoes exclusivas.
-- Formas de uso e multisselecao: os percentuais podem somar mais de 100%.
-- As regras foram validadas com o perfil textual exportado do Athena.
-- ============================================================================

CREATE TABLE dados_gold.agg_ia_maturidade_organizacional
WITH (
    format = 'PARQUET',
    write_compression = 'SNAPPY',
    external_location = 's3://tech-challenge-014478672967/gold/frente_5_ia/agg_ia_maturidade_organizacional_v1/'
) AS

WITH respostas AS (
    SELECT
        ano_pesquisa,
        'Prioridade de IA generativa' AS indicador,
        'Distribuicao exclusiva' AS tipo_metrica,
        categoria_prioridade_ia AS resposta
    FROM dados_gold.ft_ia_profissional
    WHERE categoria_prioridade_ia IS NOT NULL

    UNION ALL

    SELECT
        ano_pesquisa,
        'Bons resultados com LLM' AS indicador,
        'Distribuicao exclusiva' AS tipo_metrica,
        categoria_resultado_llm AS resposta
    FROM dados_gold.ft_ia_profissional
    WHERE categoria_resultado_llm IS NOT NULL

    UNION ALL

    SELECT
        ano_pesquisa,
        'Maturidade organizacional de IA' AS indicador,
        'Distribuicao exclusiva' AS tipo_metrica,
        categoria_maturidade_ia_empresa AS resposta
    FROM dados_gold.ft_ia_profissional
    WHERE categoria_maturidade_ia_empresa IS NOT NULL

    UNION ALL

    SELECT
        ano_pesquisa,
        'Perfil de acesso a IA' AS indicador,
        'Distribuicao exclusiva' AS tipo_metrica,
        perfil_acesso_ia AS resposta
    FROM dados_gold.ft_ia_profissional
    WHERE perfil_acesso_ia IS NOT NULL

    UNION ALL

    SELECT
        ano_pesquisa,
        'Formas de uso de IA na empresa' AS indicador,
        'Multisselecao' AS tipo_metrica,
        'Uso individual sem centralizacao' AS resposta
    FROM dados_gold.ft_ia_profissional
    WHERE flag_uso_ia_independente = 1

    UNION ALL

    SELECT
        ano_pesquisa,
        'Formas de uso de IA na empresa' AS indicador,
        'Multisselecao' AS tipo_metrica,
        'Uso organizacional estruturado' AS resposta
    FROM dados_gold.ft_ia_profissional
    WHERE flag_uso_ia_organizacional = 1

    UNION ALL

    SELECT
        ano_pesquisa,
        'Formas de uso de IA na empresa' AS indicador,
        'Multisselecao' AS tipo_metrica,
        'Direcionamento centralizado e apoio a custos' AS resposta
    FROM dados_gold.ft_ia_profissional
    WHERE flag_ia_direcionamento_centralizado = 1

    UNION ALL

    SELECT
        ano_pesquisa,
        'Formas de uso de IA na empresa' AS indicador,
        'Multisselecao' AS tipo_metrica,
        'Produtividade em desenvolvimento ou codigo' AS resposta
    FROM dados_gold.ft_ia_profissional
    WHERE flag_ia_desenvolvimento_codigo = 1

    UNION ALL

    SELECT
        ano_pesquisa,
        'Formas de uso de IA na empresa' AS indicador,
        'Multisselecao' AS tipo_metrica,
        'Eficiencia de processos internos' AS resposta
    FROM dados_gold.ft_ia_profissional
    WHERE flag_ia_eficiencia_processos = 1

    UNION ALL

    SELECT
        ano_pesquisa,
        'Formas de uso de IA na empresa' AS indicador,
        'Multisselecao' AS tipo_metrica,
        'Inovacao em produtos ou servicos' AS resposta
    FROM dados_gold.ft_ia_profissional
    WHERE flag_ia_inovacao_produtos = 1

    UNION ALL

    SELECT
        ano_pesquisa,
        'Formas de uso de IA na empresa' AS indicador,
        'Multisselecao' AS tipo_metrica,
        'IA como principal frente do negocio' AS resposta
    FROM dados_gold.ft_ia_profissional
    WHERE flag_ia_principal_negocio = 1

    UNION ALL

    SELECT
        ano_pesquisa,
        'Formas de uso de IA na empresa' AS indicador,
        'Multisselecao' AS tipo_metrica,
        'Casos isolados ou em estagio inicial' AS resposta
    FROM dados_gold.ft_ia_profissional
    WHERE flag_ia_casos_isolados_inicio = 1

    UNION ALL

    SELECT
        ano_pesquisa,
        'Formas de uso de IA na empresa' AS indicador,
        'Multisselecao' AS tipo_metrica,
        'Nao sabe opinar' AS resposta
    FROM dados_gold.ft_ia_profissional
    WHERE flag_ia_nao_sabe_empresa = 1
),

totais_ano AS (
    SELECT
        ano_pesquisa,
        COUNT(*) AS total_registros_ano
    FROM dados_gold.ft_ia_profissional
    GROUP BY ano_pesquisa
),

denominadores AS (
    SELECT
        ano_pesquisa,
        'Prioridade de IA generativa' AS indicador,
        COUNT(categoria_prioridade_ia) AS total_respostas_validas_indicador
    FROM dados_gold.ft_ia_profissional
    GROUP BY ano_pesquisa

    UNION ALL

    SELECT
        ano_pesquisa,
        'Bons resultados com LLM' AS indicador,
        COUNT(categoria_resultado_llm) AS total_respostas_validas_indicador
    FROM dados_gold.ft_ia_profissional
    GROUP BY ano_pesquisa

    UNION ALL

    SELECT
        ano_pesquisa,
        'Maturidade organizacional de IA' AS indicador,
        COUNT(tipo_uso_ia_empresa) AS total_respostas_validas_indicador
    FROM dados_gold.ft_ia_profissional
    GROUP BY ano_pesquisa

    UNION ALL

    SELECT
        ano_pesquisa,
        'Formas de uso de IA na empresa' AS indicador,
        COUNT(tipo_uso_ia_empresa) AS total_respostas_validas_indicador
    FROM dados_gold.ft_ia_profissional
    GROUP BY ano_pesquisa

    UNION ALL

    SELECT
        ano_pesquisa,
        'Perfil de acesso a IA' AS indicador,
        COUNT(flag_usa_ia) AS total_respostas_validas_indicador
    FROM dados_gold.ft_ia_profissional
    GROUP BY ano_pesquisa
),

contagens AS (
    SELECT
        ano_pesquisa,
        indicador,
        tipo_metrica,
        resposta,
        COUNT(*) AS quantidade_resposta
    FROM respostas
    GROUP BY ano_pesquisa, indicador, tipo_metrica, resposta
)

SELECT
    c.ano_pesquisa,
    c.indicador,
    c.tipo_metrica,
    c.resposta,
    c.quantidade_resposta,
    d.total_respostas_validas_indicador,
    t.total_registros_ano,

    ROUND(
        100.0 * c.quantidade_resposta
        / NULLIF(d.total_respostas_validas_indicador, 0),
        2
    ) AS pct_entre_respostas_validas,

    ROUND(
        100.0 * d.total_respostas_validas_indicador
        / NULLIF(t.total_registros_ano, 0),
        2
    ) AS pct_cobertura_indicador

FROM contagens c
INNER JOIN denominadores d
    ON c.ano_pesquisa = d.ano_pesquisa
   AND c.indicador = d.indicador
INNER JOIN totais_ano t
    ON c.ano_pesquisa = t.ano_pesquisa;


-- ============================================================================
-- 08. AGREGADO DE IA, SALARIO E SENIORIDADE
-- Grao: ano + senioridade comparavel + grupo de uso de IA.
-- A mediana deve ser priorizada na analise, pois sofre menos com outliers.
-- Esta tabela mostra associacao; nao prova que usar IA causa salario maior.
-- ============================================================================

CREATE TABLE dados_gold.agg_ia_salario_senioridade
WITH (
    format = 'PARQUET',
    write_compression = 'SNAPPY',
    external_location = 's3://tech-challenge-014478672967/gold/frente_5_ia/agg_ia_salario_senioridade_v1/'
) AS

SELECT
    ano_pesquisa,
    nivel_comparavel,
    nivel_ordem,
    flag_usa_ia,
    CASE
        WHEN flag_usa_ia = 1 THEN 'Usa IA'
        WHEN flag_usa_ia = 0 THEN 'Nao usa IA'
    END AS grupo_uso_ia,
    COUNT(*) AS quantidade_profissionais,
    ROUND(AVG(salario_medio), 2) AS salario_medio_grupo,
    ROUND(approx_percentile(salario_medio, 0.25), 2) AS salario_primeiro_quartil,
    ROUND(approx_percentile(salario_medio, 0.50), 2) AS salario_mediano,
    ROUND(approx_percentile(salario_medio, 0.75), 2) AS salario_terceiro_quartil,
    ROUND(MIN(salario_medio), 2) AS salario_minimo_observado,
    ROUND(MAX(salario_medio), 2) AS salario_maximo_observado

FROM dados_gold.ft_ia_profissional
WHERE salario_medio IS NOT NULL
  AND salario_medio > 0
  AND nivel_comparavel IS NOT NULL
  AND flag_usa_ia IN (0, 1)
GROUP BY
    ano_pesquisa,
    nivel_comparavel,
    nivel_ordem,
    flag_usa_ia;


-- ============================================================================
-- 09. AGREGADO DE IA, TECNOLOGIAS E SENIORIDADE
-- Grao: ano + senioridade comparavel + grupo de uso de IA.
-- Permite verificar se usuarios de IA utilizam um ecossistema tecnologico
-- mais amplo, sem confundir totalmente esse efeito com senioridade.
-- ============================================================================

CREATE TABLE dados_gold.agg_ia_tecnologias_senioridade
WITH (
    format = 'PARQUET',
    write_compression = 'SNAPPY',
    external_location = 's3://tech-challenge-014478672967/gold/frente_5_ia/agg_ia_tecnologias_senioridade_v1/'
) AS

SELECT
    ano_pesquisa,
    nivel_comparavel,
    nivel_ordem,
    flag_usa_ia,
    CASE
        WHEN flag_usa_ia = 1 THEN 'Usa IA'
        WHEN flag_usa_ia = 0 THEN 'Nao usa IA'
    END AS grupo_uso_ia,
    COUNT(*) AS quantidade_profissionais,

    ROUND(AVG(qtd_tecnologias_bi), 2) AS media_tecnologias_bi,
    ROUND(AVG(qtd_tecnologias_cloud), 2) AS media_tecnologias_cloud,
    ROUND(AVG(qtd_tecnologias_banco), 2) AS media_tecnologias_banco,
    ROUND(AVG(qtd_tecnologias_linguagem), 2) AS media_tecnologias_linguagem,
    ROUND(AVG(qtd_tecnologias_total), 2) AS media_tecnologias_total,
    approx_percentile(qtd_tecnologias_total, 0.50) AS mediana_tecnologias_total

FROM dados_gold.ft_ia_profissional
WHERE nivel_comparavel IS NOT NULL
  AND flag_usa_ia IN (0, 1)
GROUP BY
    ano_pesquisa,
    nivel_comparavel,
    nivel_ordem,
    flag_usa_ia;


-- ============================================================================
-- 10. VALIDACOES FINAIS
-- ============================================================================

-- A quantidade da fato deve ser igual a quantidade da Silver.
SELECT
    (SELECT COUNT(*) FROM dados_gold.vw_silver_ia_fonte) AS linhas_silver,
    (SELECT COUNT(*) FROM dados_gold.ft_ia_profissional) AS linhas_fato_gold;

-- Quantidade de linhas de cada tabela criada.
SELECT 'ft_ia_profissional' AS tabela, COUNT(*) AS quantidade_linhas
FROM dados_gold.ft_ia_profissional

UNION ALL

SELECT 'agg_ia_adocao_anual', COUNT(*)
FROM dados_gold.agg_ia_adocao_anual

UNION ALL

SELECT 'agg_ia_adocao_segmento', COUNT(*)
FROM dados_gold.agg_ia_adocao_segmento

UNION ALL

SELECT 'agg_ia_maturidade_organizacional', COUNT(*)
FROM dados_gold.agg_ia_maturidade_organizacional

UNION ALL

SELECT 'agg_ia_salario_senioridade', COUNT(*)
FROM dados_gold.agg_ia_salario_senioridade

UNION ALL

SELECT 'agg_ia_tecnologias_senioridade', COUNT(*)
FROM dados_gold.agg_ia_tecnologias_senioridade;


-- Checagem de indicadores principais.
SELECT *
FROM dados_gold.agg_ia_adocao_anual
ORDER BY ano_pesquisa;

-- Checagem de segmentos com amostra minimamente representativa.
-- O corte de 30 e uma regra exploratoria e pode ser alterado na documentacao.
SELECT *
FROM dados_gold.agg_ia_adocao_segmento
WHERE respostas_validas_uso_ia >= 30
ORDER BY ano_pesquisa, tipo_dimensao, pct_adocao_ia DESC;

-- Checagem das categorias organizacionais e cobertura das perguntas.
SELECT *
FROM dados_gold.agg_ia_maturidade_organizacional
ORDER BY ano_pesquisa, indicador, quantidade_resposta DESC;


-- ============================================================================
-- OBSERVACOES PARA REEXECUCAO
-- ============================================================================
-- O Athena CTAS nao sobrescreve external_location com arquivos existentes.
-- Durante o desenvolvimento, a opcao mais segura e criar uma nova versao:
--
-- tabela:  dados_gold.ft_ia_profissional_v2
-- caminho: .../ft_ia_profissional_v2/
--
-- Somente remova tabelas e objetos antigos do S3 quando tiver confirmado que
-- eles nao sao mais necessarios.
-- ============================================================================
