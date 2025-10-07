SET threads = 6;
SET preserve_insertion_order = false;
SET temp_directory = '/opt/airflow/data/temp_duckdb_disk.tmp/';

CREATE OR REPLACE TEMP VIEW estabelecimentos AS
SELECT * FROM read_parquet('/opt/airflow/data/Estabelecimentos0/**/*.parquet');

WITH
    empresas AS (
        SELECT 
            em.cnpj_basico AS base_cnpj,
            em.razao_social,
            CASE
                WHEN em.porte_da_empresa = '00' THEN 'Não informado'
                WHEN em.porte_da_empresa = '01' THEN 'Micro empresa'
                WHEN em.porte_da_empresa = '03' THEN 'Empresa de pequeno porte'
                WHEN em.porte_da_empresa = '05' THEN 'Demais'
            END AS porte,
            na.descricao,
            si.opcao_pelo_simples,
            si.opcao_pelo_mei,
            ROW_NUMBER() OVER(PARTITION BY em.cnpj_basico) AS empresa_num
        FROM read_parquet('/opt/airflow/data/Empresas0/**/*.parquet') AS em
        LEFT JOIN read_parquet('/opt/airflow/data/Simples/**/*.parquet') AS si
            ON em.cnpj_basico = si.cnpj_basico
        LEFT JOIN read_parquet('/opt/airflow/data/Naturezas/**/*.parquet') AS na
            ON na.codigo = em.natureza_juridica
    )

SELECT 
    CONCAT(es.cnpj_basico, cnpj_ordem, cnpj_dv) AS cnpj,
    es.nome_fantasia,
    CASE
        WHEN es.situacao_cadastral = '01' THEN 'Nula'
        WHEN es.situacao_cadastral = '02' THEN 'Ativa'
        WHEN es.situacao_cadastral = '03' THEN 'Suspensa'
        WHEN es.situacao_cadastral = '04' THEN 'Inapta'
        WHEN es.situacao_cadastral = '08' THEN 'Baixada'
    END AS situacao,
    es.data_situacao_cadastral,
    CONCAT(es.logradouro, ' ', es.numero, ' ', es.complemento) AS endereco,
    es.cep,
    es.uf,
    CONCAT(es.ddd_1, ' ', es.telefone_1) AS telefone,
    mu.descricao AS municipio,
    cn.descricao AS cnae,
    em.base_cnpj,
    em.razao_social,
    em.porte,
    em.descricao,
    em.opcao_pelo_simples,
    em.opcao_pelo_mei,
    em.empresa_num AS num
FROM estabelecimentos AS es
LEFT JOIN read_parquet('/opt/airflow/data/Municipios/**/*.parquet') AS mu
    ON mu.codigo = es.municipio
LEFT JOIN read_parquet('/opt/airflow/data/Cnaes/**/*.parquet') AS cn
    ON cn.codigo = es.cnae_fiscal_principal
LEFT JOIN empresas AS em
    ON em.base_cnpj = es.cnpj_basico
WHERE 
    es.year = ? 
    AND es.month = ? 
    AND es.cnpj_basico = ?;