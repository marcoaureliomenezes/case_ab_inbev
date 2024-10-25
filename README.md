# AB Inbev Case

- **Autor**: Marco Aurelio Reis Lima Menezes
- **Objetivo**: Apresentação de solução para o case proposto pela AB Inbev;
- **Motivo**: Processo seletivo para a vaga de Engenheiro de Dados Sênior.





#### Referências

A arquitetura de serviços reproduzida nesse trabalho, baseado em docker é baseada no seguintes repositórios:

- https://github.com/marcoaureliomenezes/ice-lakehouse
- https://github.com/marcoaureliomenezes/dd_chain_explorer


### Data Aquisition Job

- Usually 3º party tooling
- Avoid data reloads
- Minimal validation, build to be bulletproof


## 1. Problema proposto

### 1.1.  Objetivo

Consumir dados de uma API https://www.openbrewerydb.org e persistir esses dados em um data Lake. Usar a arquitetura medalhão com seus 3 layers (bronze, silver e gold);

### 1.2. Instruções

- Opções de orquestração em aberto, porém com demonstrar habilidades em como orquestrar um pipeline, tratamento de erros, monitoramento e logging;
- Linguagem em aberto. Python e Pyspark preferível mas não obrigatório;
- Uso de conteinerização. Docker ou Kubernetes por exemplo ganha pontos.

### 1.3. Especificações da arquitetura do data lake

- **Bronze**: `Persistir os dados retornados pela API em sua forma bruta`, sem tratementos e em formato nativo ou qualquer formato mais conveniente.
- **Silver**: 
    - `Transformar o dado em um formato colunar`, como parquet ou delta, e `particionar por brewery location`.
    - `Fazer tratamentos no dado bruto` desde que justificando estes. Esses tratamento incluem como limpeza de dados, remoção de colunas redundantes, etc.
- **Gold**: Criar uma `view agregada com a quantidade de breweries por tipo e localização`**`.

### Monitoring / Alerting

- Descrever como implementaria um processo de monitoramento e alerta para o pipeline;
- Considerar data quality issue, fahlas do pipeline e outros potenciais problemas na resposta


## 2. Introdução ao case criado

Nessa seção serão abordadas as decisões tomadas para construção da solução. Bem como as instruções fornecem um bom caminho para a solução, algumas decisões de caracter técnico foram tomadas para a construção da solução. Abaixo serão enumeradas algumas dessas.

### Tempestividade do processo

Em um pipeline de dados, a natureza deste em relação ao tempo, atende a diferentes própositos. Temos os pipelines de natureza streaming e batch, Jobs em streaming, voltados ao mundo de analytics, tem por objetivo uma menor latência entre o dado gerado na origem e a ponta dalinha, neste case a view Gold. Após uma análise sobre a natureza do dado retornado pela API, é possível inferir que:

- Esses dados estão provavelmente em um database, seja transacional NoSQL ou SQL, ou um banco em memória. 
- A API consome o dado desses bancos e retorna como resposta por meio dos requests HTTP efetuados a ela.
- Em casos reais, esses tipo de dado transacional é alterado de forma assíncrona por uma 2ª API. Por exemplo a aplciação para atualização de cadastro que cada uma das breweries tem acesso.
- Se extendermos para uma ingestão de inúmeras origens e com essas fontes tendo maior TPS (transações por segundo), pode ser interessante criar uma ingestão do dado em streaming usando a API ou mesmo configurar o um conector CDC (Change Data Capture) no banco de dados de origem para trazer esse dado transacional para um ambiente voltado a analytics.

A depender do requisito do dado na ponta, a escolha do tipo de pipeline é crucial. Entre batch e streaming a tempestividade do processo de ingestão passa por um espectro de tempo, que vai desde:
- Ingestão batch que vão de frequencias mensais, semanais, diárias, horárias
- Micro-batch or near-real-time (spark streaming) e tempo real tendo o Apache Flink como exemplo.

Para entender a natureza dessa escolha um exemplo simples ilustra bem. Digamos que as perguntas a serem respondidas la na ponta são do tipo:
- "Quantas breweries foram adicionadas na última hora?" 
- "Quantas vezes e quando ocorreu o cadastro de uma brewery que foi alterado logo em seguida?" 

Para responder a essas perguntas a captura de dados em tempo real é necessária.

Posta essa análise, após consultas na API e interpretação sobre o descritivo e instruções do case foi interpretado que o dado não deve ter uma atualização tão frequente e uma ingestão batch é suficiente para atender a demanda. Porém dada a necessidade, diferentes arquiteturas podem ser implementadas para atender aos requisitos de tempestividade do processo.

#### Escolha em relação a tempestividade

- Realizar ingestão batch, com uma frequência em hora.


### Slowly Changing Dimensions (SCD)

- A API retorna dados de breweries, que são entidades que podem ser alteradas ao longo do tempo.

- Para tratar essas alterações, foi implementado um tratamento de Slowly Changing Dimensions (SCD) do tipo 2.

- O tratamento SCD tipo 2 é um dos mais comuns e é usado para manter o histórico de alterações de uma entidade ao longo do tempo. Portanto, quando ocorre uma alteração de um registro da brewery, uma nova linha é inserida na tabela, mantendo o histórico de alterações.


## 2. Arquitetura de Solução



## 2. Arquitetura Técnica

Nessa sessão estão descritos os serviços que compõem a solução apresentada. Uma breve introdução sobre cada serviço, sua finalidade e como ele se encaixa na arquitetura proposta.

Para esse case foram usados serviços, aqui classificados como camadas.

### Camada Lakehouse

Na camada lakehouse estão os serviços que compõe o mesmo. Para essa arquitetura proposta a seguinte stack foi utilizada:

### 1. Serviço de armazenamento - MinIO 

- Minio é um serviço de armazenamento de objetos compatível com S3. 
- Devido a sua compatibilidade com o S3, ele é uma exelente escolha para ser usado como camada de armazenamento de dados em um lakehouse local para fins de desenvolvimento e testes.
- Surgindo a necessidade de escalar, o MinIO pode ser substituido por um serviço S3 da AWS, por exemplo.

### 2. Serviço de Catálogo e formato de tabelas

Na construção de um Lakehouse, um formato e catálogo de tabela são componentes fundamentais. Em conjunto esses componentes adicionam uma camada no topo de arquivos armazenados no data lake, possibilitando que usuários e aplicações possam ler dados de arquivos armazenados no data lake numa visão database-like.

O 1º serviço desse tipo foi o Apache Hive. criado pelo Facebook e depois tornado open source, veio para dar suporte a queries, no hive com a linguagem HQL, para consulta de dados armazenados em arquivos no HDFS. O Hive fornece ao mesmo tempo 2 capacidades que em arquitetura modernas foram segregadas, um `catálogo de tabelas` e um `formato de tabela`.

Nas arquiteturas de data lake modernas, o Hive ainda é o projeto open source mais usado para atender a finalidade de um catálogo de tabelas. Porém, as tabelas hive hoje são consideradas obsoletas, devido a falta de suporte a operações de escrita e atualização de dados e outros fatores limitantes. A única forma de fazer update em uma tabela hive é dando overwrite em toda tabela ou em uma partição, caso a tabela seja particionada.

A nova geração de tabelas conta com 2 componentes de peso, **tabelas delta** e **tabelas iceberg**. Ambas são tabelas que permitem operações de escrita e atualização de dados, além de outras funcionalidades avançadas como controle de versão de dados, controle de transações, operações de merge, entre outras.

- **O formato de tabela usado nesse case foi o Apache Iceberg**. O Motivo, implementar um formato diferente do usado no trabalho, as delta tables em conjunto com o Databricks. Não por querer ser contra a corrente, mas por compreender que se um formato ainda não matou o outro é porque hoje cada um tem seus pontos e contrapontos que os fazem coexistir. Compreender as semelhanças, diferenças e trade-offs entre esses formatos é um bom caminho para aprender o fundamento e não a ferramenta.

- **O catalogo de tabelas utilizado nesse case foi o nessie**. O motivo, o Nessie é um catalogo de tabelas open source que é compatível com tabelas iceberg. O Nessie é um projeto relativamente novo, mas que já conta com uma comunidade ativa e crescente. Adiciona uma camada de controle de versão sobre as tabelas iceberg, estilo Git, o que é muito interessante para explorar novas funcionalodades.

Ademais, a combinação Minio + Iceberg + Nessie é uma combinação que ainda não vi em nenhum projeto, e por isso, decidi implementar essa arquitetura

, afim de estudalo e compreende-lo melhor. 

Em data lakes implementados na cloud,como opções pode ser usado o Glue Catalog, em ambiente AWS, e o Unity Catalog em um lakehouse no databricks.
e hoje no mercado o Unity, da Databricks, é muito usado por grandes empresas, em conjunto com o Delta Lake como formato de armazenamento, ou o Glue Catalog, da AWS. No mundo open source, o catalogo Nessie é uma opção interessante, pois é compatível com tabelas Iceberg, um outro formato de tabela


- **Nessie**: Quando se fala em lakehouse, o catalogo de dados é um ponto chave. Existem os mais diversos serviços para essa finalidade.



MinIO.

## Tratamentos na camada Silver


Após análises foi constatado que as seguintes colunas são redundantes:

- `adress_1` e `street` contem a mesma informação

- state e state_province contem a mesma informação