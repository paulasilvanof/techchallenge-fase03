-- Frente: Remuneração, senioridade e valorização profissional
-- Camada Gold construída via SQL (DuckDB local; Athena: trocar 'silver' e quantile_cont->approx_percentile).

CREATE OR REPLACE VIEW dados_catalogados AS
SELECT * FROM dados_catalogados.parquet;

-- FATO: 1 linha por respondente, base completa + flag de universo
CREATE TABLE dados_gold.ft_remuneracao_profissional
WITH (
    format = 'PARQUET',
    write_compression = 'SNAPPY',
    external_location = 's3://tech-challenge-014478672967/gold/frente_2_remuneracao/ft_remuneracao_profissional_v1/'
)
AS
SELECT
    ano_pesquisa,
    id_unificado,

    -- Na Silver esta coluna ja e BOOLEAN; portanto, nao deve ser comparada com 1.
    COALESCE(flag_atua_com_dados, FALSE) AS atua_com_dados,

    nivel_comparavel AS senioridade,
    nivel_ordem AS senioridade_ordem,

    CASE
        WHEN funcao_atuacao LIKE '%Análise de Dados/BI%'
            THEN 'Análise de Dados/BI'
        WHEN funcao_atuacao LIKE '%Engenharia de Dados%'
            THEN 'Engenharia de Dados'
        WHEN funcao_atuacao LIKE '%Ciência de Dados/Machine Learning/AI%'
            THEN 'Ciência de Dados/ML/AI'
        WHEN funcao_atuacao IS NULL
            THEN NULL
        ELSE 'Outras frentes de dados'
    END AS funcao_dados,

    CASE
        WHEN cargo_atual LIKE 'Engenheiro de Dados%'
            THEN 'Engenheiro de Dados/Arquiteto'
        ELSE cargo_atual
    END AS cargo,

    CASE
        WHEN tempo_experiencia_dados = 'Não tenho experiência na área de dados'
            THEN 'Sem experiência'
        WHEN tempo_experiencia_dados = 'Menos de 1 ano'
            THEN '< 1 ano'
        WHEN tempo_experiencia_dados = 'de 1 a 2 anos'
            THEN '1 a 2 anos'
        WHEN tempo_experiencia_dados = 'de 3 a 4 anos'
            THEN '3 a 4 anos'
        WHEN tempo_experiencia_dados IN ('de 4 a 6 anos', 'de 5 a 6 anos')
            THEN '5 a 6 anos'
        WHEN tempo_experiencia_dados = 'de 7 a 10 anos'
            THEN '7 a 10 anos'
        WHEN tempo_experiencia_dados = 'Mais de 10 anos'
            THEN '10+ anos'
        ELSE NULL
    END AS faixa_exp_dados,

    CASE
        WHEN tempo_experiencia_dados = 'Não tenho experiência na área de dados' THEN 0
        WHEN tempo_experiencia_dados = 'Menos de 1 ano'                         THEN 1
        WHEN tempo_experiencia_dados = 'de 1 a 2 anos'                          THEN 2
        WHEN tempo_experiencia_dados = 'de 3 a 4 anos'                          THEN 3
        WHEN tempo_experiencia_dados IN ('de 4 a 6 anos', 'de 5 a 6 anos')      THEN 4
        WHEN tempo_experiencia_dados = 'de 7 a 10 anos'                         THEN 5
        WHEN tempo_experiencia_dados = 'Mais de 10 anos'                        THEN 6
        ELSE NULL
    END AS faixa_exp_dados_ordem,

    nivel_ensino AS escolaridade,

    CASE nivel_ensino
        WHEN 'Não tenho graduação formal' THEN 0
        WHEN 'Estudante de Graduação'      THEN 1
        WHEN 'Graduação/Bacharelado'       THEN 2
        WHEN 'Pós-graduação'               THEN 3
        WHEN 'Mestrado'                    THEN 4
        WHEN 'Doutorado ou Phd'            THEN 5
        ELSE NULL
    END AS escolaridade_ordem,

    regiao_onde_mora AS regiao,
    uf_onde_mora AS uf,

    CASE
        WHEN modelo_trabalho_atual LIKE 'Modelo 100%remoto'
            THEN 'Remoto'
        WHEN modelo_trabalho_atual LIKE 'Modelo híbrido flexível%'
            THEN 'Híbrido flexível'
        WHEN modelo_trabalho_atual LIKE 'Modelo híbrido com dias fixos%'
            THEN 'Híbrido dias fixos'
        WHEN modelo_trabalho_atual LIKE 'Modelo 100%presencial'
            THEN 'Presencial'
        ELSE NULL
    END AS modelo_trabalho,

    setor AS setor,

    CASE
        WHEN numero_funcionarios = 'de 501 a 100'
            THEN NULL
        ELSE numero_funcionarios
    END AS porte_empresa,

    porte_empresa_ordem,

    CASE
        WHEN flag_usa_ia = TRUE  THEN 'Usa IA'
        WHEN flag_usa_ia = FALSE THEN 'Nao usa IA'
        ELSE 'Nao respondeu'
    END AS grupo_uso_ia,

    -- Athena nao converte BOOLEAN diretamente para INTEGER.
    CASE
        WHEN flag_usa_ia = TRUE  THEN 1
        WHEN flag_usa_ia = FALSE THEN 0
        ELSE NULL
    END AS flag_usa_ia,

    linguagem_mais_usada AS linguagem_principal,
    cloud_preferida AS cloud_preferida,

    salario_medio,
    salario_min,
    salario_max,
    faixa_salarial,
    faixa_salarial_ordem,

    CASE
        WHEN faixa_salarial_ordem = 13 THEN TRUE
        ELSE FALSE
    END AS salario_censurado,

    CASE
        WHEN salario_medio IS NOT NULL THEN TRUE
        ELSE FALSE
    END AS tem_salario

FROM dados_catalogados.parquet;


CREATE TABLE dados_gold.agg_remuneracao_segmento
WITH (
    format = 'PARQUET',
    write_compression = 'SNAPPY',
    external_location = 's3://tech-challenge-014478672967/gold/frente_2_remuneracao/agg_remuneracao_segmento_v1/'
)
AS
WITH base AS (
    SELECT *
    FROM dados_gold.ft_remuneracao_profissional
    WHERE atua_com_dados = TRUE
),
dimensoes AS (
    SELECT
        'Senioridade' AS tipo_dimensao,
        ano_pesquisa,
        senioridade AS categoria,
        salario_medio
    FROM base
    WHERE senioridade IS NOT NULL

    UNION ALL

    SELECT
        'Cargo' AS tipo_dimensao,
        ano_pesquisa,
        cargo AS categoria,
        salario_medio
    FROM base
    WHERE cargo IS NOT NULL

    UNION ALL

    SELECT
        'Funcao de dados' AS tipo_dimensao,
        ano_pesquisa,
        funcao_dados AS categoria,
        salario_medio
    FROM base
    WHERE funcao_dados IS NOT NULL

    UNION ALL

    SELECT
        'Tempo de experiencia' AS tipo_dimensao,
        ano_pesquisa,
        faixa_exp_dados AS categoria,
        salario_medio
    FROM base
    WHERE faixa_exp_dados IS NOT NULL

    UNION ALL

    SELECT
        'Escolaridade' AS tipo_dimensao,
        ano_pesquisa,
        escolaridade AS categoria,
        salario_medio
    FROM base
    WHERE escolaridade IS NOT NULL

    UNION ALL

    SELECT
        'Regiao' AS tipo_dimensao,
        ano_pesquisa,
        regiao AS categoria,
        salario_medio
    FROM base
    WHERE regiao IS NOT NULL

    UNION ALL

    SELECT
        'Modelo de trabalho' AS tipo_dimensao,
        ano_pesquisa,
        modelo_trabalho AS categoria,
        salario_medio
    FROM base
    WHERE modelo_trabalho IS NOT NULL

    UNION ALL

    SELECT
        'Setor' AS tipo_dimensao,
        ano_pesquisa,
        setor AS categoria,
        salario_medio
    FROM base
    WHERE setor IS NOT NULL

    UNION ALL

    SELECT
        'Porte da empresa' AS tipo_dimensao,
        ano_pesquisa,
        porte_empresa AS categoria,
        salario_medio
    FROM base
    WHERE porte_empresa IS NOT NULL

    UNION ALL

    SELECT
        'Linguagem principal' AS tipo_dimensao,
        ano_pesquisa,
        linguagem_principal AS categoria,
        salario_medio
    FROM base
    WHERE linguagem_principal IS NOT NULL
),
metricas AS (
    SELECT
        tipo_dimensao,
        ano_pesquisa,
        categoria,
        COUNT(*) AS total_registros,
        COUNT(salario_medio) AS respostas_validas_salario,
        ROUND(approx_percentile(salario_medio, 0.25), 2) AS salario_primeiro_quartil,
        ROUND(approx_percentile(salario_medio, 0.50), 2) AS salario_mediano,
        ROUND(approx_percentile(salario_medio, 0.75), 2) AS salario_terceiro_quartil,
        ROUND(AVG(salario_medio), 2) AS salario_medio_grupo
    FROM dimensoes
    GROUP BY
        tipo_dimensao,
        ano_pesquisa,
        categoria
)
SELECT *
FROM metricas;


-- ============================================================================
-- 02. AGREGACAO DE EVOLUCAO ANUAL DA REMUNERACAO
-- Destino no Catalog: dados_gold.agg_remuneracao_evolucao_anual
-- Destino no S3:
-- s3://tech-challenge-014478672967/gold/frente_2_remuneracao/agg_remuneracao_evolucao_anual_v1/
-- ============================================================================

CREATE TABLE dados_gold.agg_remuneracao_evolucao_anual
WITH (
    format = 'PARQUET',
    write_compression = 'SNAPPY',
    external_location = 's3://tech-challenge-014478672967/gold/frente_2_remuneracao/agg_remuneracao_evolucao_anual_v1/'
)
AS
SELECT
    ano_pesquisa,
    COUNT(salario_medio) AS respostas_validas_salario,
    ROUND(approx_percentile(salario_medio, 0.25), 2) AS salario_primeiro_quartil,
    ROUND(approx_percentile(salario_medio, 0.50), 2) AS salario_mediano,
    ROUND(approx_percentile(salario_medio, 0.75), 2) AS salario_terceiro_quartil,
    ROUND(AVG(salario_medio), 2) AS salario_medio_grupo
FROM dados_gold.ft_remuneracao_profissional
WHERE atua_com_dados = TRUE
GROUP BY ano_pesquisa;
