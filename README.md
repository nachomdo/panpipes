# Kafka Panpipes

A tool to go crazy with Kafka and Unix pipes inspired by NetCat

**Disclaimer:** 
*This project was created with only one goal in mind: to have fun with Kafka, Unix pipes and tunneling between remote and local environments. Any attempt to make this project a useful tool is totally accidental and unintended* 

![project image](https://upload.wikimedia.org/wikipedia/commons/thumb/4/42/Pan_Pursuing_Syrinx_LACMA_AC1992.225.2.jpg/800px-Pan_Pursuing_Syrinx_LACMA_AC1992.225.2.jpg) 

## Rationale
NetCat and KafkaCat are amazing tools but they have a problem when it comes to binary encoded data. I really like NetCat's model where the user opens a socket somewhere to listen for the input and connect to it from somewhere else. 

## Use cases

#### Moving data between two different clusters
To move data between two clusters you just need to expose a port in the target cluster using Kafka Panpipes in listening mode. 

```
panpipes -brokers target:9092 -topic target-topic
```

Once the port is opened the next step is just to start piping information to that endpoint

```
panpipes -brokers source:9092 -connect host:8888 -topic source-topic
```

It is possible to use panpipes in combination with some tunneling with the help of tools like `socat`

#### Backing up data in a file
Sometimes we just want to get a snapshot of the information stored in a topic, so we can restore that information later in the same topic, in a different one or in another cluster. 

```
panpipes -brokers source:9092 -topic source-topic -file topic-backup.bin 
```

#### Restoring a back up in a topic
If we have a backup file we can restore the information in a topic in our cluster. 

```
panpipes -restore -brokers target:9092 -topic target-topic -file topic-backup.bin
```

#### Clone a topic into a new one 
You can create a verbatim copy of an existing topic by combining panpipes with Unix Pipes

```
panpipes -brokers kafka:9092 -topic source-topic -file - | panpipes -restore -brokers kafka:9092 -topic target-topic -file -
```