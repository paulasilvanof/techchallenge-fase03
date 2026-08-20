
CREATE DATABASE IF NOT EXISTS dados_gold
COMMENT 'Tech Challenge Fase 3 - camada gold';

--------------------------------------------------------------------------------------

CREATE OR REPLACE VIEW dados_gold.vw_silver_frente3_base AS
SELECT
    ano_pesquisa,
    COALESCE(genero,           'Não informado') AS genero,
    COALESCE(nivel_comparavel, 'Não informado') AS nivel,
    COALESCE(regiao_onde_mora, 'Não informado') AS regiao,
    COALESCE(
        CASE cargo_atual
            WHEN 'Engenheiro de Dados/Arquiteto de Dados/Data Engineer/Data Architect'
                 THEN 'Engenheiro/Arquiteto de Dados'
            WHEN 'Engenheiro de Dados/Data Engineer/Data Architect'
                 THEN 'Engenheiro/Arquiteto de Dados'
            WHEN 'Arquiteto de Dados/Data Architect'
                 THEN 'Engenheiro/Arquiteto de Dados'
            ELSE cargo_atual
        END, 'Não informado') AS cargo,
    CASE modelo_trabalho_atual
        WHEN 'Modelo 100% remoto' THEN 'Remoto'
        WHEN 'Modelo híbrido flexível (o funcionário tem liberdade para escolher quando estar no escritório presencialmente)' THEN 'Híbrido flexível'
        WHEN 'Modelo híbrido com dias fixos de trabalho presencial' THEN 'Híbrido fixo'
        WHEN 'Modelo 100% presencial' THEN 'Presencial'
        ELSE 'Não informado'
    END AS modelo,
    CASE nivel_comparavel
        WHEN 'Júnior' THEN 1 WHEN 'Pleno' THEN 2 WHEN 'Sênior' THEN 3 ELSE 99
    END AS ordem_nivel,
    CASE regiao_onde_mora
        WHEN 'Sudeste' THEN 1 WHEN 'Sul' THEN 2 WHEN 'Nordeste' THEN 3
        WHEN 'Centro-oeste' THEN 4 WHEN 'Norte' THEN 5 ELSE 99
    END AS ordem_regiao,
    CASE modelo_trabalho_atual
        WHEN 'Modelo 100% remoto' THEN 1
        WHEN 'Modelo híbrido flexível (o funcionário tem liberdade para escolher quando estar no escritório presencialmente)' THEN 2
        WHEN 'Modelo híbrido com dias fixos de trabalho presencial' THEN 3
        WHEN 'Modelo 100% presencial' THEN 4
        ELSE 99
    END AS ordem_modelo,
    salario_medio,
    faixa_salarial_ordem,
    flag_usa_ia
FROM dados_catalogados."parquet";

--------------------------------------------------------------------------------------

CREATE TABLE dados_gold.gold_genero_senioridade
WITH (
    external_location = 's3://tech-challenge-014478672967/gold/frente_3_diversidade/gold_genero_senioridade_v1/',
    format = 'PARQUET',
    write_compression = 'SNAPPY'
) AS
WITH agg AS (
    SELECT
        ano_pesquisa,
        genero,
        nivel,
        ordem_nivel,
        COUNT(*)                                                    AS n_respondentes,
        COUNT(salario_medio)                                        AS n_com_salario,
        SUM(salario_medio)                                          AS soma_salario,
        ROUND(AVG(salario_medio), 2)                                AS salario_medio,
        approx_percentile(salario_medio, 0.5)                       AS salario_mediano,
        ROUND(AVG(CAST(faixa_salarial_ordem AS double)), 3)         AS media_faixa_ordem,
        SUM(CASE WHEN modelo <> 'Não informado' THEN 1 ELSE 0 END)  AS n_com_modelo,
        SUM(CASE WHEN modelo =  'Remoto'        THEN 1 ELSE 0 END)  AS n_remoto
    FROM dados_gold.vw_silver_frente3_base
    GROUP BY ano_pesquisa, genero, nivel, ordem_nivel
)
SELECT *,
       CASE WHEN n_com_modelo > 0
            THEN ROUND(n_remoto * 100.0 / n_com_modelo, 2) END AS pct_remoto
FROM agg;

--------------------------------------------------------------------------------------

CREATE TABLE dados_gold.gold_genero_cargo
WITH (
    external_location = 's3://tech-challenge-014478672967/gold/frente_3_diversidade/gold_genero_cargo_v1/',
    format = 'PARQUET',
    write_compression = 'SNAPPY'
) AS
SELECT
    ano_pesquisa,
    genero,
    cargo,
    COUNT(*)                                            AS n_respondentes,
    COUNT(salario_medio)                                AS n_com_salario,
    SUM(salario_medio)                                  AS soma_salario,
    ROUND(AVG(salario_medio), 2)                        AS salario_medio,
    approx_percentile(salario_medio, 0.5)               AS salario_mediano,
    ROUND(AVG(CAST(faixa_salarial_ordem AS double)), 3) AS media_faixa_ordem
FROM dados_gold.vw_silver_frente3_base
GROUP BY ano_pesquisa, genero, cargo;

--------------------------------------------------------------------------------------

CREATE TABLE dados_gold.gold_regiao
WITH (
    external_location = 's3://tech-challenge-014478672967/gold/frente_3_diversidade/gold_regiao_v1/',
    format = 'PARQUET',
    write_compression = 'SNAPPY'
) AS
WITH agg AS (
    SELECT
        ano_pesquisa,
        regiao,
        ordem_regiao,
        nivel,
        ordem_nivel,
        COUNT(*)                                                          AS n_respondentes,
        COUNT(salario_medio)                                              AS n_com_salario,
        SUM(salario_medio)                                                AS soma_salario,
        ROUND(AVG(salario_medio), 2)                                      AS salario_medio,
        approx_percentile(salario_medio, 0.5)                             AS salario_mediano,
        ROUND(AVG(CAST(faixa_salarial_ordem AS double)), 3)               AS media_faixa_ordem,
        COUNT(flag_usa_ia)                                                AS n_com_flag_ia,
        SUM(CASE WHEN flag_usa_ia THEN 1 ELSE 0 END)                      AS n_usa_ia,
        SUM(CASE WHEN modelo <> 'Não informado' THEN 1 ELSE 0 END)        AS n_com_modelo,
        SUM(CASE WHEN modelo =  'Remoto'        THEN 1 ELSE 0 END)        AS n_remoto,
        SUM(CASE WHEN genero IN ('Masculino','Feminino') THEN 1 ELSE 0 END) AS n_com_genero,
        SUM(CASE WHEN genero =  'Feminino'      THEN 1 ELSE 0 END)        AS n_feminino
    FROM dados_gold.vw_silver_frente3_base
    GROUP BY ano_pesquisa, regiao, ordem_regiao, nivel, ordem_nivel
)
SELECT *,
       CASE WHEN n_com_flag_ia > 0 THEN ROUND(n_usa_ia   * 100.0 / n_com_flag_ia, 2) END AS pct_usa_ia,
       CASE WHEN n_com_modelo  > 0 THEN ROUND(n_remoto   * 100.0 / n_com_modelo,  2) END AS pct_remoto,
       CASE WHEN n_com_genero  > 0 THEN ROUND(n_feminino * 100.0 / n_com_genero,  2) END AS pct_feminino
FROM agg;

--------------------------------------------------------------------------------------

CREATE TABLE dados_gold.gold_modelo_trabalho
WITH (
    external_location = 's3://tech-challenge-014478672967/gold/frente_3_diversidade/gold_modelo_trabalho_v1/',
    format = 'PARQUET',
    write_compression = 'SNAPPY'
) AS
WITH agg AS (
    SELECT
        ano_pesquisa,
        modelo,
        ordem_modelo,
        nivel,
        ordem_nivel,
        COUNT(*)                                                          AS n_respondentes,
        COUNT(salario_medio)                                              AS n_com_salario,
        SUM(salario_medio)                                                AS soma_salario,
        ROUND(AVG(salario_medio), 2)                                      AS salario_medio,
        approx_percentile(salario_medio, 0.5)                             AS salario_mediano,
        ROUND(AVG(CAST(faixa_salarial_ordem AS double)), 3)               AS media_faixa_ordem,
        SUM(CASE WHEN genero IN ('Masculino','Feminino') THEN 1 ELSE 0 END) AS n_com_genero,
        SUM(CASE WHEN genero =  'Feminino'      THEN 1 ELSE 0 END)        AS n_feminino
    FROM dados_gold.vw_silver_frente3_base
    GROUP BY ano_pesquisa, modelo, ordem_modelo, nivel, ordem_nivel
)
SELECT *,
       CASE WHEN n_com_genero > 0
            THEN ROUND(n_feminino * 100.0 / n_com_genero, 2) END AS pct_feminino
FROM agg;

