/* Frente Fabricio */

CREATE DATABASE IF NOT EXISTS dados_gold;

-- 1. TABELA FATO: ft_stack_profissional (Desnormalização de Tecnologias)
CREATE TABLE dados_gold.ft_stack_profissional
WITH (
    format = 'PARQUET',
    parquet_compression = 'SNAPPY',
    external_location = 's3://tech-challenge-014478672967/gold/ft_stack_profissional/'
) AS
WITH base_silver AS (
    SELECT
        CAST(ano_pesquisa AS INT) AS ano_pesquisa,
        id_unificado AS id_profissional,
        cargo_atual,
        nivel_comparavel AS nivel_senioridade,
        faixa_salarial,
        CAST(salario_medio AS DOUBLE) AS salario_estimado,
        modelo_trabalho_atual AS modelo_trabalho,
        regiao_onde_mora AS regiao,
        COALESCE(CAST(flag_usa_ia AS INT), 0) AS flag_usa_ia,

        -- Linguagens
        COALESCE(CAST(tec_lang_sql AS INT), 0) AS lang_sql,
        COALESCE(CAST(tec_lang_python AS INT), 0) AS lang_python,
        COALESCE(CAST(tec_lang_r AS INT), 0) AS lang_r,
        COALESCE(CAST(tec_lang_java AS INT), 0) AS lang_java,
        COALESCE(CAST(tec_lang_scala AS INT), 0) AS lang_scala,
        COALESCE(CAST(tec_lang_visual_basic_vba AS INT), 0) AS lang_vba,

        -- Cloud
        COALESCE(CAST(tec_cloud_amazon_web_services_aws AS INT), 0) AS cloud_aws,
        COALESCE(CAST(tec_cloud_azure_microsoft AS INT), 0) AS cloud_azure,
        COALESCE(CAST(tec_cloud_google_cloud_gcp AS INT), 0) AS cloud_gcp,
        COALESCE(CAST(tec_cloud_oracle_cloud AS INT), 0) AS cloud_oracle,
        COALESCE(CAST(tec_cloud_on_premise AS INT), 0) AS cloud_on_premise,

        -- Bancos e Plataformas
        COALESCE(CAST(tec_db_postgresql AS INT), 0) AS db_postgresql,
        COALESCE(CAST(tec_db_mysql AS INT), 0) AS db_mysql,
        COALESCE(CAST(tec_db_sql_server AS INT), 0) AS db_sql_server,
        COALESCE(CAST(tec_db_databricks AS INT), 0) AS db_databricks,
        COALESCE(CAST(tec_db_google_bigquery AS INT), 0) AS db_bigquery,
        COALESCE(CAST(tec_db_snowflake AS INT), 0) AS db_snowflake,

        -- BI
        COALESCE(CAST(tec_bi_microsoft_powerbi AS INT), 0) AS bi_power_bi,
        COALESCE(CAST(tec_bi_tableau AS INT), 0) AS bi_tableau,
        COALESCE(CAST(tec_bi_looker_studio AS INT), 0) AS bi_looker_studio,
        COALESCE(CAST(tec_bi_metabase AS INT), 0) AS bi_metabase
    FROM dados_catalogados.parquet
)
-- Unpivot das 4 Categorias:
SELECT ano_pesquisa, id_profissional, cargo_atual, nivel_senioridade, faixa_salarial, salario_estimado, modelo_trabalho, regiao, flag_usa_ia, 'Linguagens' AS categoria_tech, 'SQL' AS tecnologia, lang_sql AS flag_adota FROM base_silver
UNION ALL
SELECT ano_pesquisa, id_profissional, cargo_atual, nivel_senioridade, faixa_salarial, salario_estimado, modelo_trabalho, regiao, flag_usa_ia, 'Linguagens', 'Python', lang_python FROM base_silver
UNION ALL
SELECT ano_pesquisa, id_profissional, cargo_atual, nivel_senioridade, faixa_salarial, salario_estimado, modelo_trabalho, regiao, flag_usa_ia, 'Linguagens', 'R', lang_r FROM base_silver
UNION ALL
SELECT ano_pesquisa, id_profissional, cargo_atual, nivel_senioridade, faixa_salarial, salario_estimado, modelo_trabalho, regiao, flag_usa_ia, 'Linguagens', 'Java', lang_java FROM base_silver
UNION ALL
SELECT ano_pesquisa, id_profissional, cargo_atual, nivel_senioridade, faixa_salarial, salario_estimado, modelo_trabalho, regiao, flag_usa_ia, 'Cloud', 'AWS', cloud_aws FROM base_silver
UNION ALL
SELECT ano_pesquisa, id_profissional, cargo_atual, nivel_senioridade, faixa_salarial, salario_estimado, modelo_trabalho, regiao, flag_usa_ia, 'Cloud', 'Azure', cloud_azure FROM base_silver
UNION ALL
SELECT ano_pesquisa, id_profissional, cargo_atual, nivel_senioridade, faixa_salarial, salario_estimado, modelo_trabalho, regiao, flag_usa_ia, 'Cloud', 'GCP', cloud_gcp FROM base_silver
UNION ALL
SELECT ano_pesquisa, id_profissional, cargo_atual, nivel_senioridade, faixa_salarial, salario_estimado, modelo_trabalho, regiao, flag_usa_ia, 'Bancos e Plataformas', 'PostgreSQL', db_postgresql FROM base_silver
UNION ALL
SELECT ano_pesquisa, id_profissional, cargo_atual, nivel_senioridade, faixa_salarial, salario_estimado, modelo_trabalho, regiao, flag_usa_ia, 'Bancos e Plataformas', 'Databricks', db_databricks FROM base_silver
UNION ALL
SELECT ano_pesquisa, id_profissional, cargo_atual, nivel_senioridade, faixa_salarial, salario_estimado, modelo_trabalho, regiao, flag_usa_ia, 'Bancos e Plataformas', 'BigQuery', db_bigquery FROM base_silver
UNION ALL
SELECT ano_pesquisa, id_profissional, cargo_atual, nivel_senioridade, faixa_salarial, salario_estimado, modelo_trabalho, regiao, flag_usa_ia, 'Bancos e Plataformas', 'Snowflake', db_snowflake FROM base_silver
UNION ALL
SELECT ano_pesquisa, id_profissional, cargo_atual, nivel_senioridade, faixa_salarial, salario_estimado, modelo_trabalho, regiao, flag_usa_ia, 'BI', 'Power BI', bi_power_bi FROM base_silver
UNION ALL
SELECT ano_pesquisa, id_profissional, cargo_atual, nivel_senioridade, faixa_salarial, salario_estimado, modelo_trabalho, regiao, flag_usa_ia, 'BI', 'Tableau', bi_tableau FROM base_silver
UNION ALL
SELECT ano_pesquisa, id_profissional, cargo_atual, nivel_senioridade, faixa_salarial, salario_estimado, modelo_trabalho, regiao, flag_usa_ia, 'BI', 'Looker Studio', bi_looker_studio FROM base_silver;


-- 2. TABELA AGREGADA: agg_tech_ranking_geral (Top N e Adoção por Categoria)
CREATE TABLE dados_gold.agg_tech_ranking_geral
WITH (
    format = 'PARQUET',
    parquet_compression = 'SNAPPY',
    external_location = 's3://tech-challenge-014478672967/gold/agg_tech_ranking_geral/'
) AS
SELECT
    ano_pesquisa,
    categoria_tech AS categoria,
    tecnologia,
    COUNT(DISTINCT id_profissional) AS total_respondentes,
    SUM(flag_adota) AS total_usuarios,
    ROUND(100.0 * SUM(flag_adota) / COUNT(DISTINCT id_profissional), 2) AS pct_adocao
FROM dados_gold.ft_stack_profissional
GROUP BY ano_pesquisa, categoria_tech, tecnologia;


-- 3. TABELA AGREGADA: agg_tech_segmentos (Senioridade, Cargo, Região, Modelo de Trabalho)
CREATE TABLE dados_gold.agg_tech_segmentos
WITH (
    format = 'PARQUET',
    parquet_compression = 'SNAPPY',
    external_location = 's3://tech-challenge-014478672967/gold/agg_tech_segmentos/'
) AS
SELECT
    ano_pesquisa,
    'Senioridade' AS tipo_dimensao,
    nivel_senioridade AS categoria,
    tecnologia,
    categoria_tech,
    COUNT(DISTINCT id_profissional) AS total_amostra,
    SUM(flag_adota) AS total_usuarios,
    ROUND(100.0 * SUM(flag_adota) / COUNT(DISTINCT id_profissional), 2) AS pct_adocao
FROM dados_gold.ft_stack_profissional
WHERE nivel_senioridade IS NOT NULL
GROUP BY ano_pesquisa, nivel_senioridade, tecnologia, categoria_tech
UNION ALL
SELECT
    ano_pesquisa,
    'Cargo' AS tipo_dimensao,
    cargo_atual AS categoria,
    tecnologia,
    categoria_tech,
    COUNT(DISTINCT id_profissional) AS total_amostra,
    SUM(flag_adota) AS total_usuarios,
    ROUND(100.0 * SUM(flag_adota) / COUNT(DISTINCT id_profissional), 2) AS pct_adocao
FROM dados_gold.ft_stack_profissional
WHERE cargo_atual IS NOT NULL
GROUP BY ano_pesquisa, cargo_atual, tecnologia, categoria_tech;


-- 4. TABELA AGREGADA: agg_tech_salario_senioridade (Cruzamento Salarial)
CREATE TABLE dados_gold.agg_tech_salario_senioridade
WITH (
    format = 'PARQUET',
    parquet_compression = 'SNAPPY',
    external_location = 's3://tech-challenge-014478672967/gold/agg_tech_salario_senioridade/'
) AS
SELECT
    ano_pesquisa,
    categoria_tech,
    tecnologia,
    nivel_senioridade,
    flag_adota,
    CASE WHEN flag_adota = 1 THEN 'Adota Tecnologia' ELSE 'Nao Adota' END AS grupo_adocao,
    COUNT(DISTINCT id_profissional) AS quantidade_profissionais,
    ROUND(AVG(salario_estimado), 2) AS salario_medio,
    approx_percentile(salario_estimado, 0.5) AS salario_mediano
FROM dados_gold.ft_stack_profissional
WHERE salario_estimado IS NOT NULL
  AND nivel_senioridade IN ('Júnior', 'Pleno', 'Sênior')
GROUP BY ano_pesquisa, categoria_tech, tecnologia, nivel_senioridade, flag_adota;


-- 5. TABELA AGREGADA: agg_tech_evolucao_anual (Série Temporal)
CREATE TABLE dados_gold.agg_tech_evolucao_anual
WITH (
    format = 'PARQUET',
    parquet_compression = 'SNAPPY',
    external_location = 's3://tech-challenge-014478672967/gold/agg_tech_evolucao_anual/'
) AS
SELECT
    ano_pesquisa,
    categoria_tech,
    tecnologia,
    COUNT(DISTINCT id_profissional) AS total_pesquisa_ano,
    SUM(flag_adota) AS usuarios_tech,
    ROUND(100.0 * SUM(flag_adota) / COUNT(DISTINCT id_profissional), 2) AS pct_adocao
FROM dados_gold.ft_stack_profissional
GROUP BY ano_pesquisa, categoria_tech, tecnologia;
