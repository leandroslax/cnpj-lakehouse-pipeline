# Arquitetura e Estrategia de FinOps

## 1. Visao Geral

Este projeto implementa um pipeline analitico local com foco em dados publicos de CNPJ, utilizando `dbt Core`, `DuckDB` e `Prefect`. A solucao foi desenhada para executar localmente com baixo atrito, mas com estrutura suficiente para migracao futura para um ambiente cloud baseado em `BigQuery`.

O objetivo principal foi construir um fluxo reprodutivel para:

- extrair e preparar amostras dos dados brutos
- padronizar e tipar os dados em camada de `staging`
- consolidar dimensoes e fato analitico
- aplicar testes de qualidade de dados
- manter historico de alteracoes relevantes com `snapshot`
- orquestrar a execucao ponta a ponta

## 2. Arquitetura da Solucao

A arquitetura do projeto foi organizada em camadas para facilitar manutencao, reuso e onboarding.

### 2.1 Ingestao

Os arquivos brutos de CNPJ sao armazenados em `data/raw/`. Para otimizar o tempo de execucao local, o pipeline gera amostras de aproximadamente 10.000 linhas para os arquivos maiores:

- `Empresas`
- `Estabelecimentos`
- `Socios`
- `Simples`

Arquivos menores de referencia, como `CNAE`, `Municipios`, `Naturezas` e `Qualificacoes`, sao copiados integralmente.

### 2.2 Transformacao com dbt

O `dbt` foi estruturado nas seguintes camadas:

- `staging`: limpeza, tipagem e padronizacao dos dados de origem
- `marts/dim`: dimensoes reutilizaveis para consumo analitico
- `marts/fact`: fato consolidada com metricas de empresas

Modelos implementados:

- `stg_empresas`
- `stg_estabelecimentos`
- `stg_socios`
- `stg_simples`
- `stg_cnaes`
- `dim_cnae`
- `dim_empresa`
- `fact_empresas_ativas`

### 2.3 Snapshot

Foi implementado um `snapshot` SCD Type 2 para historizar alteracoes de `capital_social` no modelo de empresas.

Snapshot implementado:

- `snp_empresas_capital_social`

Essa abordagem permite preservar historico de mudancas em um atributo critico de negocio, simulando uma necessidade comum em pipelines de dados corporativos.

### 2.4 Orquestracao com Prefect

O fluxo em `flows/pipeline_flow.py` executa:

1. geracao das amostras locais
2. execucao de `dbt run`
3. execucao de `dbt test`

Essa orquestracao garante repetibilidade e aproxima a solucao de um ambiente operacional real.

## 3. Modelagem Analitica

### 3.1 Dimensoes

#### `dim_cnae`

Tabela de referencia para codigos CNAE, usada para enriquecer a analise de atividade economica.

#### `dim_empresa`

Consolida dados cadastrais da empresa com dados do estabelecimento matriz e informacoes de enquadramento no Simples Nacional.

Essa dimensao concentra atributos como:

- razao social
- nome fantasia
- natureza juridica
- capital social
- porte da empresa
- situacao cadastral
- data de inicio de atividade
- CNAE principal
- UF e municipio
- indicativos de Simples e MEI

### 3.2 Fato

#### `fact_empresas_ativas`

Tabela fato com consolidacao por empresa, incluindo metricas derivadas e atributos relevantes para analise.

Campos e metricas principais:

- `cnpj_basico`
- `cnpj_completo`
- `capital_social`
- `situacao_cadastral`
- `is_ativa`
- `cnae_fiscal_principal`
- `quantidade_socios`
- `quantidade_estabelecimentos`

A fato foi configurada com materializacao `incremental` e `unique_key = cnpj_basico`, evitando duplicidade em reprocessamentos.

## 4. Qualidade de Dados

A camada de qualidade foi implementada com testes automatizados no `dbt`.

Foram aplicados testes de:

- `not_null`
- `unique`
- `relationships`
- `accepted_values`

Ao todo, o projeto conta com 12 testes passando, cobrindo:

- unicidade de chaves
- obrigatoriedade de campos criticos
- integridade referencial entre fato e dimensoes
- validacao de dominio para situacao cadastral

Durante o desenvolvimento, um problema real de duplicidade na fato incremental foi identificado e resolvido com configuracao explicita de `unique_key`, reforcando o papel dos testes como mecanismo de controle de regressao.

## 5. Macros e Reuso

Foram criadas macros Jinja para encapsular regras recorrentes de limpeza e padronizacao:

- `normalize_text`
- `parse_br_decimal`
- `parse_yyyymmdd`

Essas macros reduzem repeticao, aumentam legibilidade e facilitam manutencao futura.

## 6. Estrategia para BigQuery

Embora a implementacao local utilize `DuckDB`, a estrutura foi pensada para migracao para `BigQuery` com minima friccao.

### 6.1 Organizacao por camadas

Em BigQuery, eu manteria a separacao por datasets ou schemas logicos, por exemplo:

- `raw_cnpj`
- `stg_cnpj`
- `mart_cnpj`
- `snapshots_cnpj`

Essa separacao ajuda em:

- governanca
- isolamento de responsabilidades
- observabilidade
- controle de custo por camada

### 6.2 Partitioning

Minha recomendacao seria particionar tabelas grandes e consultadas com frequencia por colunas temporais relevantes.

Exemplos:

- snapshots por data de vigencia ou data de processamento
- fatos historicas por data de referencia ou data de carga
- tabelas de eventos por data do evento, quando aplicavel

No caso deste projeto, a tabela de snapshot e futuras tabelas historicas se beneficiariam mais diretamente de particionamento por data.

### 6.3 Clustering

Minha recomendacao de clustering para consultas analiticas seria priorizar colunas com alto uso em filtros e joins, por exemplo:

- `cnpj_basico`
- `uf`
- `cnae_fiscal_principal`
- `situacao_cadastral`

Isso tende a reduzir leitura desnecessaria de blocos e melhorar tempo de resposta em consultas filtradas.

## 7. Analise de FinOps

FinOps, neste contexto, significa projetar o pipeline para minimizar custo de processamento e leitura no BigQuery sem comprometer qualidade e governanca.

### 7.1 Medidas de reducao de custo

As principais decisoes de FinOps para esse pipeline seriam:

- uso de modelos incrementais para evitar reprocessamento completo
- uso de particionamento em tabelas historicas e de maior volume
- uso de clustering em colunas comuns de filtro
- separacao entre dados brutos, padronizados e analiticos
- execucao seletiva de transformacoes em vez de recalcular toda a cadeia
- uso de snapshots apenas onde ha valor de negocio claro

### 7.2 Impacto esperado

Conceitualmente, essas escolhas reduzem custo porque:

- menos dados sao lidos por consulta
- menos tabelas precisam ser reprocessadas em cada execucao
- joins e filtros tendem a ser mais eficientes
- a orquestracao pode limitar execucoes desnecessarias

Em BigQuery, isso se traduziria em:

- menor volume de bytes lidos por query
- menor consumo de slots em consultas recorrentes
- melhor previsibilidade de custo operacional

### 7.3 Trade-offs

As decisoes de FinOps tambem exigem equilibrio:

- particionamento ruim pode gerar ganho pequeno ou ate aumentar complexidade
- clustering excessivo pode encarecer manutencao sem retorno equivalente
- snapshots de muitos campos podem inflar armazenamento desnecessariamente

Por isso, a recomendacao e sempre alinhar modelagem com os padroes reais de consulta do negocio.

## 8. Onboarding e Operacao

Para facilitar onboarding de novos colaboradores, o projeto foi mantido com:

- estrutura de diretorios clara
- nomes de modelos consistentes
- macros reutilizaveis
- README com instrucoes de execucao
- flow unico de orquestracao para reproducao local

Fluxo sugerido para um novo colaborador:

1. clonar o repositorio
2. criar e ativar ambiente virtual
3. instalar dependencias
4. posicionar arquivos brutos em `data/raw/`
5. executar `dbt debug`
6. executar `python flows/pipeline_flow.py`

## 9. Conclusao

A solucao entregue atende aos requisitos centrais do desafio com uma implementacao local simples, mas aderente a boas praticas de engenharia de dados.

Os principais pontos de destaque sao:

- modelagem em camadas com `dbt`
- fato incremental com controle de unicidade
- snapshot SCD Type 2
- testes automatizados de qualidade
- macros reutilizaveis
- orquestracao com `Prefect`
- desenho compativel com evolucao futura para `BigQuery`

Como proximo passo natural em um ambiente de producao, eu evoluiria a solucao com:

- ingestao completa dos arquivos em vez de amostras
- camadas intermediarias adicionais para enriquecimento
- observabilidade de pipeline
- deploy em ambiente cloud com `BigQuery` e agendamento gerenciado
