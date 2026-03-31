with empresas as (

    select * from {{ ref('stg_empresas') }}

),

estabelecimentos_matriz as (

    select *
    from {{ ref('stg_estabelecimentos') }}
    where identificador_matriz_filial = '1'

),

simples as (

    select * from {{ ref('stg_simples') }}

)

select
    e.cnpj_basico,
    em.cnpj_completo,
    e.razao_social,
    em.nome_fantasia,
    e.natureza_juridica,
    e.qualificacao_responsavel,
    e.capital_social,
    e.porte_empresa,
    e.ente_federativo_responsavel,
    em.situacao_cadastral,
    em.data_situacao_cadastral,
    em.motivo_situacao_cadastral,
    em.data_inicio_atividade,
    em.cnae_fiscal_principal,
    em.uf,
    em.municipio,
    s.opcao_simples,
    s.data_opcao_simples,
    s.data_exclusao_simples,
    s.opcao_mei,
    s.data_opcao_mei,
    s.data_exclusao_mei
from empresas e
left join estabelecimentos_matriz em
    on e.cnpj_basico = em.cnpj_basico
left join simples s
    on e.cnpj_basico = s.cnpj_basico
