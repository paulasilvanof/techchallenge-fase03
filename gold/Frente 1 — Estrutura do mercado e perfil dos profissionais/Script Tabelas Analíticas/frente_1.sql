-- ============================================================================
-- TECH CHALLENGE - FASE 3
-- CAMADA GOLD - FRENTE 1: ESTRUTURA DO MERCADO E PERFIL DOS PROFISSIONAIS
-- Motor: Amazon Athena (SQL / CTAS)  |  Origem: camada Silver (dados_catalogados.parquet)
--
-- Responde: como esta estruturado o mercado de Dados, quais perfis sao mais
-- valorizados e quais diferencas existem por senioridade.
--
-- Tabelas geradas:
--   dados_gold.agg_estrutura_distribuicao   (cargo, senioridade, escolaridade,
--                                            formacao, experiencia, regiao)
--   dados_gold.agg_salario_cargo            (remuneracao por cargo)
--   dados_gold.agg_salario_senioridade      (remuneracao por senioridade)
--   dados_gold.agg_resumo_evolucao_ano      (evolucao do perfil por ano)
-- ============================================================================


-- ============================================================================
-- 01. DATABASE GOLD
-- ============================================================================

CREATE DATABASE IF NOT EXISTS dados_gold;


-- ============================================================================
-- 02. VIEW DE ORIGEM
-- ============================================================================

CREATE OR REPLACE VIEW dados_gold.vw_silver_estrutura_fonte AS
SELECT *
FROM dados_catalogados.parquet;


-- ============================================================================
-- 03. DISTRIBUICAO DO PERFIL (6 dimensoes)
-- Grao: ano + tipo_dimensao + categoria.
-- ordem_categoria ordena as dimensoes com ordem natural (senioridade e
-- experiencia); NULL nas demais. pct_no_ano = participacao na dimensao, no ano.
-- ============================================================================

CREATE TABLE dados_gold.agg_estrutura_distribuicao
WITH (
    format = 'PARQUET',
    write_compression = 'SNAPPY',
    external_location = 's3://tech-challenge-014478672967/gold/frente_1_estrutura/agg_estrutura_distribuicao_v1/'
) AS

WITH base AS (
    SELECT ano_pesquisa, 'Cargo'                 AS tipo_dimensao, NULLIF(TRIM(cargo_atual), '')             AS categoria, CAST(NULL AS INTEGER)                         AS ordem_categoria FROM dados_gold.vw_silver_estrutura_fonte
    UNION ALL
    SELECT ano_pesquisa, 'Senioridade',          NULLIF(TRIM(nivel_comparavel), ''),        CAST(nivel_ordem AS INTEGER)                  FROM dados_gold.vw_silver_estrutura_fonte
    UNION ALL
    SELECT ano_pesquisa, 'Escolaridade',         NULLIF(TRIM(nivel_ensino), ''),            CAST(NULL AS INTEGER)                         FROM dados_gold.vw_silver_estrutura_fonte
    UNION ALL
    SELECT ano_pesquisa, 'Area de formacao',     NULLIF(TRIM(area_formacao), ''),           CAST(NULL AS INTEGER)                         FROM dados_gold.vw_silver_estrutura_fonte
    UNION ALL
    SELECT ano_pesquisa, 'Tempo de experiencia', NULLIF(TRIM(tempo_experiencia_dados), ''), CAST(tempo_experiencia_dados_ordem AS INTEGER) FROM dados_gold.vw_silver_estrutura_fonte
    UNION ALL
    SELECT ano_pesquisa, 'Regiao',               NULLIF(TRIM(regiao_onde_mora), ''),        CAST(NULL AS INTEGER)                         FROM dados_gold.vw_silver_estrutura_fonte
),

contagens AS (
    SELECT
        ano_pesquisa,
        tipo_dimensao,
        categoria,
        MAX(ordem_categoria) AS ordem_categoria,
        COUNT(*)             AS quantidade
    FROM base
    WHERE categoria IS NOT NULL
    GROUP BY ano_pesquisa, tipo_dimensao, categoria
)

SELECT
    ano_pesquisa,
    tipo_dimensao,
    categoria,
    ordem_categoria,
    quantidade,
    SUM(quantidade) OVER (PARTITION BY ano_pesquisa, tipo_dimensao) AS total_respostas_validas_dimensao,
    ROUND(
        100.0 * quantidade
        / NULLIF(SUM(quantidade) OVER (PARTITION BY ano_pesquisa, tipo_dimensao), 0),
        2
    ) AS pct_no_ano
FROM contagens
ORDER BY ano_pesquisa, tipo_dimensao, quantidade DESC;


-- ============================================================================
-- 04. REMUNERACAO POR CARGO
-- Grao: ano + cargo. Mediana priorizada por ser robusta a outliers.
-- ============================================================================

CREATE TABLE dados_gold.agg_salario_cargo
WITH (
    format = 'PARQUET',
    write_compression = 'SNAPPY',
    external_location = 's3://tech-challenge-014478672967/gold/frente_1_estrutura/agg_salario_cargo_v1/'
) AS

WITH base AS (
    SELECT
        ano_pesquisa,
        NULLIF(TRIM(cargo_atual), '') AS cargo_atual,
        salario_medio
    FROM dados_gold.vw_silver_estrutura_fonte
    WHERE salario_medio IS NOT NULL
      AND salario_medio > 0
)

SELECT
    ano_pesquisa,
    cargo_atual,
    COUNT(*)                                         AS quantidade_profissionais,
    ROUND(AVG(salario_medio), 2)                     AS salario_medio,
    ROUND(approx_percentile(salario_medio, 0.25), 2) AS salario_q1,
    ROUND(approx_percentile(salario_medio, 0.50), 2) AS salario_mediano,
    ROUND(approx_percentile(salario_medio, 0.75), 2) AS salario_q3
FROM base
WHERE cargo_atual IS NOT NULL
GROUP BY ano_pesquisa, cargo_atual
ORDER BY ano_pesquisa, salario_mediano DESC;


-- ============================================================================
-- 05. REMUNERACAO POR SENIORIDADE
-- Grao: ano + senioridade. Ordenada do menos para o mais senior (nivel_ordem).
-- ============================================================================

CREATE TABLE dados_gold.agg_salario_senioridade
WITH (
    format = 'PARQUET',
    write_compression = 'SNAPPY',
    external_location = 's3://tech-challenge-014478672967/gold/frente_1_estrutura/agg_salario_senioridade_v1/'
) AS

WITH base AS (
    SELECT
        ano_pesquisa,
        NULLIF(TRIM(nivel_comparavel), '') AS nivel_comparavel,
        CAST(nivel_ordem AS INTEGER)       AS nivel_ordem,
        salario_medio
    FROM dados_gold.vw_silver_estrutura_fonte
    WHERE salario_medio IS NOT NULL
      AND salario_medio > 0
)

SELECT
    ano_pesquisa,
    nivel_comparavel,
    nivel_ordem,
    COUNT(*)                                         AS quantidade_profissionais,
    ROUND(AVG(salario_medio), 2)                     AS salario_medio,
    ROUND(approx_percentile(salario_medio, 0.25), 2) AS salario_q1,
    ROUND(approx_percentile(salario_medio, 0.50), 2) AS salario_mediano,
    ROUND(approx_percentile(salario_medio, 0.75), 2) AS salario_q3
FROM base
WHERE nivel_comparavel IS NOT NULL
GROUP BY ano_pesquisa, nivel_comparavel, nivel_ordem
ORDER BY ano_pesquisa, nivel_ordem;


-- ============================================================================
-- 06. RESUMO DA EVOLUCAO DO PERFIL POR ANO
-- Grao: uma linha por ano. Visao macro da mudanca do mercado (2023-2025).
-- ============================================================================

CREATE TABLE dados_gold.agg_resumo_evolucao_ano
WITH (
    format = 'PARQUET',
    write_compression = 'SNAPPY',
    external_location = 's3://tech-challenge-014478672967/gold/frente_1_estrutura/agg_resumo_evolucao_ano_v1/'
) AS

SELECT
    ano_pesquisa,
    COUNT(*)                                                    AS total_respondentes,
    ROUND(AVG(idade), 1)                                        AS idade_media,
    ROUND(approx_percentile(
        CASE WHEN salario_medio > 0 THEN salario_medio END, 0.50), 2) AS salario_mediano_geral,
    COUNT(DISTINCT NULLIF(TRIM(cargo_atual), ''))               AS qtd_cargos_distintos,
    COUNT(DISTINCT NULLIF(TRIM(nivel_comparavel), ''))          AS qtd_niveis_distintos,
    COUNT(DISTINCT NULLIF(TRIM(regiao_onde_mora), ''))          AS qtd_regioes_distintas
FROM dados_gold.vw_silver_estrutura_fonte
GROUP BY ano_pesquisa
ORDER BY ano_pesquisa;
