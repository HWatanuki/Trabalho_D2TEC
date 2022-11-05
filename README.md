# Trabalho_D2TEC
Segundo trabalho da disciplina D2TEC - Tecnologias de Big Data do curso de Especialização em Ciência de Dados do IFSP Campinas.

# Aluno: 
- Hugo Martinelli Watanuki

# ETL de dados usando infraestrutura AWS
O objetivo deste repositório é fornecer um conjunto de instruções, arquivos de configuração e códigos para a criação de uma infraestutrura de processamento e análise de Big Data usando recursos da AWS. A demonstração do passo a passo completo para a construção dessa infraestrutura está disponível aqui: https://youtu.be/vlRkAsbyuNI

# a) Visão geral da solução
A solução criada foi baseada em um paradigma de microserviços em nuvem. Para isso, foram utilizados os seguintes componentes principais:
- 1 Elastic File System (EFS): https://aws.amazon.com/efs/
- 1 Elastic Kubernetes Services (EKS): https://aws.amazon.com/eks/
- 1 High Performance Computing Cluster (HPCC Systems): https://hpccsystems.com/

O diagrama de arquitetura AWS implementada é apresentado abaixo:

![image](https://user-images.githubusercontent.com/50485300/200107439-bf0d4e86-3b02-4c0d-ab3d-927c3134d172.png)


# b) Infraestrutura utilizada
A infrastrutura foi criada na região us-east-1 e envolveu os seguintes recursos:
- 1 usuário Identity and Access Management (IAM) com permissões para administrar clusters EKS (https://docs.aws.amazon.com/eks/latest/userguide/security-iam.html)
- 1 Virtual Private Cloud (VPC) padrão da AWS com subnets públicas em cada zona de disponibilidade (https://docs.aws.amazon.com/vpc/latest/userguide/default-vpc.html)
- 4 nós t3.medium com 2 vCPUs e 4 GiB de memória (https://aws.amazon.com/ec2/instance-types/t3/)
- 5 volumes de 1 GB cada para armazenamento de dados no EFS

# c) Metadados
A base de dados utilizada no trabalho foi a da Comissão de Taxi e Limousine da cidade de Nova York (https://www1.nyc.gov/site/tlc/about/tlc-trip-record-data.page). 

Essa base contém registros de viagens de passageiros de taxi da cidade de Nova York com os seguintes atributos:
- Local, data e hora da partida e chegada de cada viagem
- Distância, custo, tarifa e número de passageiros de cada viagem

Para as análises foram selecionados 2 datasets correspondendo às viagens realizadas entre os meses de Janeiro e Fevereiro de 2017, bem como um dataset contendo os códigos e descrições das áreas da cidade de Nova York.

Os datasets utilizados estão disponíveis aqui: https://github.com/HWatanuki/Trabalho_D2TEC/tree/main/Datasets

A descrição mais detalhada dos dados está disponível aqui:

# d) Scripts de consulta dos dados
Os códigos utilizados para consultas SQL dos dados estão disponíveis aqui:

# e) Visualizações
As visualizações das consultas mais relevantes estão disponíveis aqui:


