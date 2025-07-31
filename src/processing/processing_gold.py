from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_date
from typing import Any
import os
from config.settings import Settings


dft_year = Settings().DFT_YEAR
dft_month_str = Settings().DFT_MONTH_STR
dft_month = Settings().DFT_MONTH
base_dir_path = Settings().BASE_DIR_PATH


spark = (
    SparkSession
      .builder
      .appName("cnpj-processing-silver")
      .master("local[*]")
      .getOrCreate()
)


def read_df(
    path: str,
    inferSchema: str = 'false'
) -> Any:
    df = (
        spark
        .read
        .option('inferSchema', inferSchema)
        .parquet(path)
    )
    return df

def save_df(df: Any, path: str) -> None:
    (
        df
        .write
        .mode('overwrite')
        .parquet(path)
    )

def create_views() -> None:
    path = f'{base_dir_path}/silver/{dft_year}/{dft_month_str}'
    folders = [name for name in os.listdir(path)]
    for folder in folders:
        df = read_df(f'{path}/{folder}')
        df_name = folder.split('.')[0].split('-')[0].lower()
        df.createOrReplaceTempView(f'{df_name}')


create_views()


spark.sql(
    """
    CREATE OR REPLACE TEMP VIEW dim_partners AS
    SELECT
      s.cnpj_basico,
      CASE
        WHEN s.identificador_socio = 1 THEN 'Pessoa Juridica'
        WHEN s.identificador_socio = 2 THEN 'Pessoa Fisica'
        WHEN s.identificador_socio = 3 THEN 'Estrangeiro'
        WHEN s.identificador_socio = '01' THEN 'Pessoa Juridica'
        WHEN s.identificador_socio = '02' THEN 'Pessoa Fisica'
        WHEN s.identificador_socio = '03' THEN 'Estrangeiro'
      END AS identificador_socio,
      s.nome_do_socio,
      s.cpf_cnpj_do_socio,
      qs.descricao AS qualificacao,
      s.data_de_entrada_sociedade
    FROM socios0 AS s
    FULL OUTER JOIN qualificacoes AS qs
      ON s.qualificacao_do_socio = qs.codigo
    WHERE s.cnpj_basico IS NOT NULL
    """
)

dim_partners = spark.sql("SELECT * FROM dim_partners")
save_df(
  dim_partners,
  f'{base_dir_path}/gold/{dft_year}/{dft_month_str}/Dim_Partners.parquet'
)
dim_partners = None


spark.sql(
    """
    CREATE OR REPLACE TEMP VIEW dim_companies AS
    SELECT
      e.cnpj_basico,
      e.razao_social,
      qs.descricao AS qualificacao_do_responsavel,
      e.capital_social_da_empresa,
      CASE
        WHEN e.porte_da_empresa = 0 THEN 'Não Informado'
        WHEN e.porte_da_empresa = 1 THEN 'Micro Empresa'
        WHEN e.porte_da_empresa = 3 THEN 'Empresa de Pequeno Porte'
        WHEN e.porte_da_empresa = '00' THEN 'Não Informado'
        WHEN e.porte_da_empresa = '01' THEN 'Micro Empresa'
        WHEN e.porte_da_empresa = '03' THEN 'Empresa de Pequeno Porte'
        ELSE 'Demais'
      END AS porte_da_empresa,
      e.ente_federativo_responsavel,
      n.descricao AS natureza_juridica,
      CASE
        WHEN s.opcao_pelo_simples = 'S' THEN 'Sim'
        WHEN s.opcao_pelo_simples = 'N' THEN 'Não'
        ELSE NULL
      END AS opcao_pelo_simples,
      s.data_de_opcao_pelo_simples,
      s.data_de_exclusao_do_simples,
      CASE
        WHEN s.opcao_pelo_mei = 'S' THEN 'Sim'
        WHEN s.opcao_pelo_mei = 'N' THEN 'Não'
        ELSE NULL
      END AS opcao_pelo_mei,
      s.data_de_opcao_pelo_mei,
      s.data_de_exclusao_do_mei
    FROM empresas0 AS e
    FULL OUTER JOIN naturezas AS n
      ON e.natureza_juridica = n.codigo
    FULL OUTER JOIN simples AS s
      ON e.cnpj_basico = s.cnpj_basico
    FULL OUTER JOIN qualificacoes AS qs
      ON e.qualificacao_do_responsavel = qs.codigo
    WHERE e.cnpj_basico IS NOT NULL
    """
)

dim_companies = spark.sql("SELECT * FROM dim_companies")
save_df(
    dim_companies,
    f'{base_dir_path}/gold/{dft_year}/{dft_month_str}/Dim_Companies.parquet'
)
dim_companies = None


spark.sql(
    """
    CREATE OR REPLACE TEMP VIEW fact_cnpj AS
    SELECT
      c.cnpj_basico,
      CONCAT(c.cnpj_basico, c.cnpj_ordem, c.cnpj_dv) AS cnpj,
      p.descricao AS pais,
      cnp.descricao AS cnae_principal,
      cns.descricao as cnae_secundario,
      m.descricao AS municipio,
      CASE
        WHEN c.identificador_matriz_filial = 1 THEN 'Matriz'
        WHEN c.identificador_matriz_filial = 2 THEN 'Filial'
        WHEN c.identificador_matriz_filial = '01' THEN 'Matriz'
        WHEN c.identificador_matriz_filial = '02' THEN 'Filial'
        ELSE 'Demais'
      END AS identificador_matriz_filial,
      c.nome_fantasia,
      CASE
        WHEN c.situacao_cadastral = 1 THEN 'Nula'
        WHEN c.situacao_cadastral = 2 THEN 'Ativa'
        WHEN c.situacao_cadastral = 3 THEN 'Suspensa'
        WHEN c.situacao_cadastral = 4 THEN 'Inapta'
        WHEN c.situacao_cadastral = 8 THEN 'Baixada'
        WHEN c.situacao_cadastral = '01' THEN 'Nula'
        WHEN c.situacao_cadastral = '02' THEN 'Ativa'
        WHEN c.situacao_cadastral = '03' THEN 'Suspensa'
        WHEN c.situacao_cadastral = '04' THEN 'Inapta'
        WHEN c.situacao_cadastral = '08' THEN 'Baixada'
      END AS situacao_cadastral,
      c.data_situacao_cadastral,
      c.motivo_situacao_cadastral,
      c.nome_da_cidade_no_exterior,
      c.data_inicio_atividade,
      CONCAT(c.logradouro, c.numero, c.complemento) AS endereco,
      c.bairro,
      c.cep,
      CONCAT(c.ddd_1, ' ', c.telefone_1) AS telefone_1,
      CONCAT(c.ddd_2, ' ', c.telefone_2) AS telefone_2,
      c.situacao_especial,
      c.data_da_situacao_especial,
      c.uf
    FROM estabelecimentos0 AS c
    FULL OUTER JOIN municipios AS m
      ON c.municipio = m.codigo
    FULL OUTER JOIN cnaes AS cnp
      ON c.cnae_fiscal_principal = cnp.codigo
    FULL OUTER JOIN cnaes AS cns
      ON c.cnae_fiscal_secundario = cns.codigo
    FULL OUTER JOIN paises AS p
      ON p.codigo = c.pais
    WHERE cnpj_basico IS NOT NULL
    """
)

fact_cnpj = spark.sql("SELECT * FROM fact_cnpj")
save_df(
    fact_cnpj,
    f'{base_dir_path}/gold/{dft_year}/{dft_month_str}/Fact_Cnpj.parquet'
)
fact_cnpj = None
