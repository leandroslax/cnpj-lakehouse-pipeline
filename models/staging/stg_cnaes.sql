with source as (

    select *
    from read_csv(
        '/Users/leandrosantos/Downloads/cnpj-lakehouse-pipeline/data/raw/cnaes.csv',
        delim=';',
        quote='"',
        escape='"',
        header=false,
        encoding='latin-1',
        columns={
            'cnae_codigo': 'varchar',
            'cnae_descricao': 'varchar'
        }
    )

)

select
    trim(cnae_codigo) as cnae_codigo,
    trim(cnae_descricao) as cnae_descricao
from source
