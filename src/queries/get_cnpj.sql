--TODO
--review data modelling/normalization
--subqueries for join with empresas and partners (and subjoins)
--review cte/with
--window functions + partition by
--optmization
--acid and transaction management

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
WHERE es.year = ? AND es.month = ? AND es.cnpj_basico = ?;