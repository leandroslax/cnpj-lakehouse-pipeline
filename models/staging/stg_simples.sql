with source as (

    select *
    from read_csv(
        '/Users/leandrosantos/Downloads/cnpj-lakehouse-pipeline/data/raw/simples_sample.csv',
        delim=';',
        quote='"',
        escape='"',
        header=false,
        encoding='latin-1',
        columns={
            'cnpj_basico': 'varchar',
            'opcao_simples': 'varchar',
            'data_opcao_simples': 'varchar',
            'data_exclusao_simples': 'varchar',
            'opcao_mei': 'varchar',
            'data_opcao_mei': 'varchar',
            'data_exclusao_mei': 'varchar'
        }
    )

)

select
    trim(cnpj_basico) as cnpj_basico,
    trim(opcao_simples) as opcao_simples,
    case
        when trim(data_opcao_simples) in ('', '00000000') then null
        else strptime(trim(data_opcao_simples), '%Y%m%d')
    end as data_opcao_simples,
    case
        when trim(data_exclusao_simples) in ('', '00000000') then null
        else strptime(trim(data_exclusao_simples), '%Y%m%d')
    end as data_exclusao_simples,
    trim(opcao_mei) as opcao_mei,
    case
        when trim(data_opcao_mei) in ('', '00000000') then null
        else strptime(trim(data_opcao_mei), '%Y%m%d')
    end as data_opcao_mei,
    case
        when trim(data_exclusao_mei) in ('', '00000000') then null
        else strptime(trim(data_exclusao_mei), '%Y%m%d')
    end as data_exclusao_mei
from source
