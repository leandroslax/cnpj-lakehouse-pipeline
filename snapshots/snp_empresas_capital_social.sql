{% snapshot snp_empresas_capital_social %}

{{
    config(
        target_schema='main',
        unique_key='cnpj_basico',
        strategy='check',
        check_cols=['capital_social']
    )
}}

select
    cnpj_basico,
    razao_social,
    natureza_juridica,
    capital_social,
    porte_empresa
from {{ ref('stg_empresas') }}

{% endsnapshot %}
