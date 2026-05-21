<h1>Sistema Distribuído de Monitoramento do Estreito de Ormuz</h1>

<p>
Este projeto consiste em um sistema distribuído desenvolvido em Python para simular operações de monitoramento marítimo no Estreito de Ormuz, uma das regiões estratégicas mais importantes do planeta para o transporte internacional de petróleo. A aplicação foi construída com foco em conceitos clássicos de Sistemas Distribuídos, incluindo coordenação entre nós, exclusão mútua distribuída, tolerância a falhas, sincronização de estado e despacho cooperativo de drones autônomos.
</p>

<p>
A arquitetura do sistema é baseada em múltiplos brokers responsáveis por receber eventos gerados por sensores marítimos, organizar filas de prioridade e coordenar o envio de drones para execução das missões. Toda a comunicação ocorre através de UDP Multicast, eliminando dependências externas e permitindo que os componentes da aplicação se comuniquem de forma distribuída dentro da rede.
</p>

<hr>

<h2>Arquitetura do Sistema</h2>

<p>
O sistema é dividido em três componentes principais: sensores, brokers e drones. Os sensores representam fontes de eventos marítimos, como colisões iminentes, derramamentos de óleo, falhas de sinalização e congestionamentos navais. Os brokers atuam como nós distribuídos responsáveis pela coordenação global do sistema, enquanto os drones representam agentes autônomos encarregados de executar missões de monitoramento e resposta.
</p>

<pre>
Sensores → Brokers Distribuídos → Drones Autônomos
</pre>

<p>
Cada broker mantém estruturas internas responsáveis pelo gerenciamento da fila distribuída, drones ativos, missões em execução e sincronização com os demais brokers da rede. O sistema foi projetado para continuar operando mesmo diante da falha de um ou mais nós.
</p>

<hr>

<h2>Coordenação Distribuída</h2>

<p>
Para garantir consistência entre os brokers e impedir conflitos durante operações críticas, o projeto implementa o algoritmo de 
:contentReference[oaicite:0]{index=0}. 
Esse algoritmo utiliza relógios lógicos de Lamport para controlar o acesso à seção crítica de maneira distribuída, sem depender de um servidor central.
</p>

<p>
A implementação permite que os brokers realizem eleições de coordenador, sincronizem estados internos e organizem o despacho dos drones sem gerar condições de corrida. O sistema trabalha com três estados principais dentro do algoritmo:
</p>

<pre>
RELEASED → fora da seção crítica
WANTED   → solicitando acesso
HELD     → dentro da seção crítica
</pre>

<p>
Além disso, o projeto implementa mecanismos de deferimento de respostas, controle de peers ativos e remoção dinâmica de nós offline.
</p>

<hr>

<h2>Drones Autônomos</h2>

<p>
Os drones simulam agentes autônomos responsáveis por atender ocorrências marítimas. Cada drone possui um identificador único, localização aproximada, estado operacional e comunicação contínua com os brokers através de heartbeats periódicos.
</p>

<p>
Durante a execução de uma missão, o drone percorre três fases principais: deslocamento até o setor, execução da missão e retorno à base. O comportamento é totalmente assíncrono e executado utilizando múltiplas threads.
</p>

<pre>
idle        → disponível
dispatched  → deslocando-se
on_mission  → executando missão
returning   → retornando à base
</pre>

<p>
Os drones também simulam movimentação geográfica dinâmica utilizando coordenadas aleatórias próximas ao Estreito de Ormuz.
</p>

<hr>

<h2>Sistema de Prioridades</h2>

<p>
Os eventos recebidos pelos brokers são classificados automaticamente conforme sua gravidade. Situações críticas como colisões iminentes ou derramamentos de óleo recebem prioridade máxima, enquanto eventos de monitoramento visual possuem menor prioridade.
</p>

<p>
A fila distribuída é ordenada levando em consideração tanto a severidade quanto o tempo de espera de cada requisição. Caso uma ocorrência permaneça muito tempo aguardando atendimento, o sistema aumenta automaticamente sua prioridade através de um mecanismo de escalonamento dinâmico.
</p>

<p>
Esse comportamento permite que o sistema simule cenários reais de gerenciamento de incidentes marítimos, onde determinadas situações se tornam mais perigosas conforme o tempo passa.
</p>

<hr>

<h2>Comunicação em Rede</h2>

<p>
Toda a comunicação entre os componentes ocorre via UDP Multicast, utilizando sockets nativos da biblioteca padrão do Python. O sistema publica mensagens de status, comandos de despacho e sincronização de estados sem utilizar brokers externos como MQTT ou RabbitMQ.
</p>

<pre>
MC_GROUP = 224.1.1.1
MC_PORT  = 5007
</pre>

<p>
Os principais tópicos utilizados são:
</p>

<pre>
ormuz/drones/status
ormuz/drones/cmd
</pre>

<p>
Essa abordagem permite que múltiplos drones e brokers compartilhem informações simultaneamente dentro da mesma rede multicast.
</p>

<hr>

<h2>Testes</h2>

<p>
O projeto possui uma suíte extensa de testes unitários e testes de integração desenvolvidos com a biblioteca <code>unittest</code>. Os testes cobrem o algoritmo de exclusão mútua distribuída, sincronização da fila distribuída, despacho de drones, escalonamento de severidade, tolerância a falhas e diversos casos de uso completos do sistema.
</p>

<p>
Também foram implementados cenários envolvendo falha de brokers, drones offline, redistribuição automática de missões e sincronização inicial entre nós recém-conectados.
</p>

<hr>

<h2>Docker</h2>

<p>
O projeto pode ser executado facilmente utilizando Docker. O container é baseado na imagem oficial do Python 3.11 Slim e executa diretamente os processos. Abaixo segue um exemplo com drone:
</p>

<pre>
FROM python:3.11-slim

WORKDIR /app

COPY drone.py ./

CMD ["python3", "-u", "drone.py"]
</pre>

<hr>

<h2>Como Executar</h2>

<p>
Primeiro, clone o repositório:
</p>

<pre>
git clone &lt;repo-url&gt;
cd projeto-ormuz
</pre>

<p>
Depois, realize o build da imagem Docker:
</p>

<pre>
docker build -t ormuz-drone .
</pre>

<p>
Por fim, execute um drone:
</p>

<pre>
docker run --rm \
  -e DRONE_ID=drone-001 \
  -e BASE_SECTOR=Base-Alpha \
  ormuz-drone
</pre>

<hr>

<h2>Execução dos Testes</h2>

<pre>
python3 -m unittest -v test_ormuz.py
</pre>

<hr>

<h2>Conceitos Aplicados</h2>

<p>
O projeto aplica diversos conceitos clássicos de Sistemas Distribuídos e Concorrência, incluindo relógios lógicos de Lamport, exclusão mútua distribuída, sincronização entre nós, tolerância a falhas, coordenação distribuída, comunicação assíncrona, gerenciamento concorrente de estado compartilhado e sistemas multiagentes.
</p>

<p>
Além do caráter acadêmico, o sistema também serve como laboratório prático para estudos avançados sobre arquiteturas distribuídas, coordenação de agentes autônomos e simulação de ambientes críticos em larga escala.
</p>

<hr>

<h2>Autor</h2>

<p>
Projeto desenvolvido para fins acadêmicos e experimentais na área de Sistemas Distribuídos.
</p>
