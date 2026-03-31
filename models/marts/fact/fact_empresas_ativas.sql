{{
    config(
        materialized='incremental',
        unique_key='cnpj_basico'
    )
}}

with metricas as (

    select * from {{ ref('int_empresa_metricas') }}

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
        m.quantidade_socios,
        m.quantidade_estabelecimentos
    from {{ ref('dim_empresa') }} d
    left join metricas m
        on d.cnpj_basico = m.cnpj_basico
)

select * from base
