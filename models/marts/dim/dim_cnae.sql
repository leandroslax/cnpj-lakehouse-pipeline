select
    cnae_codigo,
    cnae_descricao
from {{ ref('stg_cnaes') }}
