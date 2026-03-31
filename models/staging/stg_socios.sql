with source as (

    select *
    from read_csv(
        '/Users/leandrosantos/Downloads/cnpj-lakehouse-pipeline/data/raw/socios_sample.csv',
        delim=';',
        quote='"',
        escape='"',
        header=false,
        encoding='latin-1',
        columns={
            'cnpj_basico': 'varchar',
            'identificador_socio': 'varchar',
            'nome_socio': 'varchar',
            'cpf_cnpj_socio': 'varchar',
            'qualificacao_socio': 'varchar',
            'data_entrada_sociedade': 'varchar',
            'pais': 'varchar',
            'representante_legal': 'varchar',
            'nome_representante': 'varchar',
            'qualificacao_representante_legal': 'varchar',
            'faixa_etaria': 'varchar'
        }
    )

)

select
    trim(cnpj_basico) as cnpj_basico,
    trim(identificador_socio) as identificador_socio,
    trim(nome_socio) as nome_socio,
    trim(cpf_cnpj_socio) as cpf_cnpj_socio,
    trim(qualificacao_socio) as qualificacao_socio,
    case
        when trim(data_entrada_sociedade) = '' then null
        else strptime(trim(data_entrada_sociedade), '%Y%m%d')
    end as data_entrada_sociedade,
    nullif(trim(pais), '') as pais,
    nullif(trim(representante_legal), '') as representante_legal,
    nullif(trim(nome_representante), '') as nome_representante,
    nullif(trim(qualificacao_representante_legal), '') as qualificacao_representante_legal,
    nullif(trim(faixa_etaria), '') as faixa_etaria
from source
