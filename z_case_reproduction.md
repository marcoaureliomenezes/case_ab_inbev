## 5. Reprodução do Case

### 5.1. Requisitos

Nessa seção está definido o passo-a-passo para reprodução do case em ambiente local. Essa sessão permite que os avaliadores executem o mesmo e compreendam como ele funciona.

## 5.1. Requisitos

Para reprodução em ambiente local, é necessário que os seguintes requisitos sejam satisfeitos.

1. O case foi desenvolvido e testado em ambiente Linux, distro Ubuntu 22.04.
2. É necessário possuir uma máquina com no mínimo 16 GB de memória RAM e processador com 4 núcleos, devido ao número de containers que serão instanciados.
3. Docker instalado e configurado.
4. Docker Compose e Docker Swarm instalados.
5. Makefile instalado.


### 5.2. Passo a passo para reprodução

Para isso, execute o comando abaixo e em seguida navegue para o diretório do projeto e em seguida navegue para a pasta usando o terminal.

```bash
git clone git@github.com:marcoaureliomenezes/case_ab_inbev.git && cd case_ab_inbev
```
#### 5.1.1 Build de imagens

Neste trabalho existem imagens docker customizadas que precisam ser construídas antes do deploy. São elas:

- Imagem **breweries-spark** e customizada para integração com MinIO, Nessie e Iceberg.
- Imagem **breweries-spark-notebooks** com Jupyterlab instalado.
- Imagem **breweries-spark-apps** contendo aplicações Spark para tratamento de dados.
- Imagem **breweries-python-apps** baseada em  para jobs de ingestão.

As imagens construídas acima tem como imagens base, `python:3.10-slim-buster` e `bitnami/spark:3.5.3`.

```bash
make build_images
```

#### 5.1.2.  Deploy de serviços

Para deploy dos serviços, execute o comando abaixo:

```bash
make deploy_services
```

#### 5.1.3.  Vendo os serviços

Para visualizar os serviços deployados, execute o comando abaixo:

```bash
make watch_services
```

#### 5.1.4.  Parando os serviços

Para parar os serviços deployados, execute o comando abaixo:

```bash
make stop_services
```

O comando usa definições no Makefile para construir as imagens e deployar os serviços usando o Docker Compose.


### 5.3 Configurações Iniciais em recursos deployados

Quando os recursos são deployado pela 1ª vez, ou caso haja mudança no volume para os serviços abaixo, é necessário que o usuário configure os seguintes recursos:

#### 5.3.1. Serviço de armazenamento Minio

O Minio quando deployado pode ser acessado a partir dos usuários `admin` e `password`. Para acessar o Minio, acesse o endereço `http://localhost:9000`.

Quando o lake é criado, é necessário a criação do bucket `breweries` e criação de API Keys para acesso ao Minio. Para isso, siga os passos abaixo:

1. Acesse o Minio em `http://localhost:9000`.
2. Faça login com as credenciais `admin` e `password`.
3. Crie um bucket chamado `breweries`.
4. Crie uma API Key para acesso ao Minio. Para isso, acesse a aba `Users` e clique em `Add User`.
5. Armazene as chaves de acesso geradas no arquivo `services/conf/.secrets.conf` nas variáveis **AWS_ACCESS_KEY_ID** e **AWS_SECRET_ACCESS_KEY**.

Dessa forma as credenciais para acesso ao bucket criado serão passadas aos serviços que necessitam acessar o Minio nos arquivos yml atraves da configuração `env_files`.

#### 5.3.2 Ferramenta de query engine Dremio

O Dremio consegue se conectar a diferentes fontes de dados, como Minio, Nessie e Iceberg. Para isso, é necessário configurar as conexões com essas fontes de dados.


**Source Nessie**:
- **Name**:  nessie
- **Nessie Endpoint URL**: http://nessie:19120/api/v2
- **Nessie Authentication Type**: None
- **AWS Access Key**: access_key
- **AWS Access Secret**: access_secret
- **Connection Properties**:
  - **fs.s3a.path.style.access**:   true
  - **dremio.s3.compat**:   true
  - **fs.s3a.endpoint**:    minio:9000

**Source Minio**
- **Name**:  minio
- **AWS Access Key**: AWS_ACCESS_KEY_ID gerado no Minio
- **AWS Access Secret**: AWS_SECRET_ACCESS_KEY gerado no Minio
- **Check Enable compatibility mode**
- **Connection Properties**:
  - **fs.s3a.path.style.access**:   true
  - **dremio.s3.compat**:   true
  - **fs.s3a.endpoint**:    minio:9000


#### 5.3.3. Execução de pipelines no Airflow


