# AB Inbev Case

- **Autor**: Marco Aurelio Reis Lima Menezes
- **Objetivo**: Apresentação de solução para o case proposto pela AB Inbev;
- **Motivo**: Processo seletivo para a vaga de Engenheiro de Dados Sênior.





#### Referências

A arquitetura de serviços reproduzida nesse trabalho é baseada no seguintes repositórios:

- https://github.com/marcoaureliomenezes/ice-lakehouse
- https://github.com/marcoaureliomenezes/dd_chain_explorer


### Data Aquisition Job

- Usually 3º party tooling
- Avoid data reloads
- Minimal validation, build to be bulletproof



## Tratamentos na camada Silver


Após análises foi constatado que as seguintes colunas são redundantes:

- `adress_1` e `street` contem a mesma informação

- state e state_province contem a mesma informação