# Kafka Panpipes

A tool to go crazy with Kafka and Unix pipes inspired by NetCat

**Disclaimer:** 
*This project was created with only one goal in mind: to have fun with Kafka, Unix pipes and tunneling between remote and local environments. Any attempt to make this project a useful tool is totally accidental and unintended* 

![project image](https://upload.wikimedia.org/wikipedia/commons/thumb/4/42/Pan_Pursuing_Syrinx_LACMA_AC1992.225.2.jpg/800px-Pan_Pursuing_Syrinx_LACMA_AC1992.225.2.jpg) 


## Use cases

#### Moving data between two different clusters
To move data between two clusters you just need to expose a port in the target cluster using Kafka Panpipes in listening mode. 

```
panpipes -b target:9092 -l host:8888 -t target-topic
```

Once the port is opened the next step is just to start sending information to that endpoint

```
panpipes -b source:9092 -t source-topic host:8888
```

It is possible to use panpipes in combination with some tunneling with the help of tools like `socat`

#### Backing up data in a file
Sometimes we just want to get a snapshot of the information stored in a topic, so we can restore that information later in the same topic, in a different one or in another cluster. 

```
panpipes -b source:9092 -t source-topic -f topic-backup.bin 
```

#### Restoring a back up in a topic
If we have a backup file we can restore the information in a topic in our cluster. 

```
panpipes -r -b target:9092 -t target-topic -f topic-backup.bin
```

#### Clone a topic into a new one 
You can create a verbatim copy of an existing topic by combining panpipes with Unix Pipes

```
panpipes -b kafka:9092 -t source-topic -f | panpipes -r -b kafka:9092 -t target-topic -f
```