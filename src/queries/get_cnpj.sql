--TODO
--optmization

WITH
    empresas AS (
        SELECT 
            em.cnpj_basico,
            em.razao_social,
            em.porte_da_empresa,
            na.descricao,
            si.opcao_pelo_simples,
            si.opcao_pelo_mei,
            ROW_NUMBER() OVER(PARTITION BY cnpj_basico) AS empresa_num
        FROM read_parquet('/app/data/Empresas0/**/*.parquet') AS em
        LEFT JOIN read_parquet('/app/data/Simples/**/*.parquet') AS si
            ON em.cnpj_basico = si.cnpj_basico
        LEFT JOIN read_parquet('/app/data/Naturezas/**/*.parquet') AS na
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
    cn.descricao AS cnae
FROM read_parquet('/app/data/Estabelecimentos0/**/*.parquet') AS es
LEFT JOIN read_parquet('/app/data/Municipios/**/*.parquet') AS mu
    ON mu.codigo = es.municipio
LEFT JOIN read_parquet('/app/data/Cnaes/**/*.parquet') AS cn
    ON cn.codigo = es.cnae_fiscal_principal
LEFT JOIN empresas AS em
    ON em.cnpj_basico = es.cnpj_basico 
WHERE 
    es.year = ? 
    AND es.month = ? 
    AND es.cnpj_basico = ?;