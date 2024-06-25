# Guia de Configuração do Projeto

Este projeto utiliza Docker, Airflow, Meltano e Emlbuk para facilitar o desenvolvimento e execução de pipelines de dados. Siga as instruções abaixo para configurar e executar o projeto.

## Execução do docker-compose

Certifique-se de ter o Docker e o plugin compose instalados em seu sistema. Em seguida, siga estas etapas para executar o projeto:

1. Clone este repositório para o seu ambiente local;
2. Navegue até o diretório raiz do projeto;
3. Execute o comando `docker compose up -d` para iniciar os serviços definidos no arquivo `docker-compose.yml`;
4. Aguarde até que todos os serviços estejam prontos. Você verá mensagens de log indicando que o Airflow e os bancos de dados PostgreSQL foram iniciados com sucesso;
5. Execute o comando `docker compose logs airflow | grep password` para obter a credenciais de login do Airflow.

## Definição de Política de Retentativas

Para definir uma política de retentativas para os seus DAGs no Airflow, você pode ajustar os parâmetros `retries`, `retry_delay`, dentro da DAG. Recomenda-se experimentar diferentes configurações com base na natureza do seu trabalho e nos requisitos de tempo de resposta. Ademais, a variável `depends_on_past` habilita a pipeline para ser executada em dias passados.

## Configuração Dinâmica do Airflow

O Airflow oferece recursos poderosos para configuração dinâmica por meio de variáveis e conexões. Ao configurar variáveis e conexões, tome cuidado para proteger informações sensíveis, como senhas e chaves de API. Recomenda-se armazenar essas informações em segredos ou variáveis de ambiente protegidas.

## Disclaimer

Embora o código atualmente tenta atender aos requisitos do projeto, há algumas áreas que podem ser aprimoradas para garantir maior robustez e confiabilidade no pipeline:

1. **Tratamento de Erros**: Embora o código tenha sido desenvolvido com zelo, é importante considerar o tratamento de erros mais abrangente para lidar com situações inesperadas de forma adequada.

2. **Monitoramento**: A implementação de mecanismos de monitoramento permitirá uma visão mais clara do desempenho do pipeline e a identificação de possíveis problemas.

3. **Agendamento Confiável**: Testes adicionais de agendamento podem ser realizados para garantir que o DAG esteja sendo executado nos horários e intervalos desejados.

4. **Segurança**: A revisão da segurança das credenciais e informações sensíveis é essencial para proteger o pipeline contra possíveis vulnerabilidades.

5. **Limpeza de Recursos**: Uma política de limpeza de recursos ajudará a manter o ambiente do Airflow organizado e eficiente, removendo arquivos temporários ou dados não utilizados após a conclusão do pipeline.

Essas melhorias podem ser consideradas para futuras atualizações do código, visando garantir uma pipeline mais sólida e resiliente.

