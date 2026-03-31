with source as (

    select *
    from read_csv(
        '/Users/leandrosantos/Downloads/cnpj-lakehouse-pipeline/data/raw/empresas_sample.csv',
        delim=';',
        quote='"',
        escape='"',
        header=false,
        columns={
            'cnpj_basico': 'varchar',
            'razao_social': 'varchar',
            'natureza_juridica': 'varchar',
            'qualificacao_responsavel': 'varchar',
            'capital_social_raw': 'varchar',
            'porte_empresa': 'varchar',
            'ente_federativo_responsavel': 'varchar'
        }
    )

)

select
    trim(cnpj_basico) as cnpj_basico,
    {{ normalize_text('razao_social') }} as razao_social,
    trim(natureza_juridica) as natureza_juridica,
    trim(qualificacao_responsavel) as qualificacao_responsavel,
    {{ parse_br_decimal('capital_social_raw') }} as capital_social,
    trim(porte_empresa) as porte_empresa,
    {{ normalize_text('ente_federativo_responsavel') }} as ente_federativo_responsavel
from source
