# storm-hello-world
Apache Storm Hello World

## Descrição das Classes
- StreamProducer: Envia as linhas do arquivo taxi.txt para a Queue 
- Topology: Descreve a topologia 
- ActiveMQSpout: Spout que adquire as tuplas da Queue
- ArraySpout: Spout com array estático
- UsainBolt: Separador de palavras
- CountBolt: Contador de palavras
- FileSpout: Spout com arquivo
