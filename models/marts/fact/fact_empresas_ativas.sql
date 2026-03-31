{{
    config(
        materialized='incremental',
        unique_key='cnpj_basico'
    )
}}

with socios_por_empresa as (

    select
        cnpj_basico,
        count(*) as quantidade_socios
    from {{ ref('stg_socios') }}
    group by 1

),

estabelecimentos_por_empresa as (

    select
        cnpj_basico,
        count(*) as quantidade_estabelecimentos
    from {{ ref('stg_estabelecimentos') }}
    group by 1

),

base as (

    select
        d.cnpj_basico,
        d.cnpj_completo,
        d.razao_social,
        d.nome_fantasia,
        d.natureza_juridica,
        d.capital_social,
        d.porte_empresa,
        d.situacao_cadastral,
        case when d.situacao_cadastral = '02' then true else false end as is_ativa,
        d.data_inicio_atividade,
        d.cnae_fiscal_principal,
        d.uf,
        d.municipio,
        d.opcao_simples,
        d.opcao_mei,
        coalesce(s.quantidade_socios, 0) as quantidade_socios,
        coalesce(e.quantidade_estabelecimentos, 0) as quantidade_estabelecimentos
    from {{ ref('dim_empresa') }} d
    left join socios_por_empresa s
        on d.cnpj_basico = s.cnpj_basico
    left join estabelecimentos_por_empresa e
        on d.cnpj_basico = e.cnpj_basico
)

select * from base
