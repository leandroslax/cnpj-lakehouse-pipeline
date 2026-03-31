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

base_cnpjs as (

    select cnpj_basico from {{ ref('dim_empresa') }}

)

select
    b.cnpj_basico,
    coalesce(s.quantidade_socios, 0) as quantidade_socios,
    coalesce(e.quantidade_estabelecimentos, 0) as quantidade_estabelecimentos
from base_cnpjs b
left join socios_por_empresa s
    on b.cnpj_basico = s.cnpj_basico
left join estabelecimentos_por_empresa e
    on b.cnpj_basico = e.cnpj_basico
