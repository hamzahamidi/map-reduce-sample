# TP MAP REDUCE

1.1 Exécution locale
--------------------

**1)**
 1. Map input records=2*, donc le nombre d'entrée (dans la map) en input.
 2. Map output records=11*, donc le nombre d'entrée (dans la map) en sortie.

**2)**
*Map output records=11* est égale à *Reduce input records=11* car l'input du reduce et l'output du map (et qu'il n'y a pas de *Combiner*).

**3)**
*Reduce input groups=9* c'est le nombre de clef différentes.

1.2 Premier contact avec HDFS
-----------------------------

```
hdfs dfs -ls /user/odinpi/
```

1.3 Exécution sur le cluster
----------------------------

```
odinpi@NameNode:~$ hadoop jar tp.jar WordCount /data/miserables wordcount
15/01/28 10:51:16 WARN util.NativeCodeLoader: Unable to load native-hadoop library for your platform... using builtin-java classes where applicable
15/01/28 10:51:17 INFO client.RMProxy: Connecting to ResourceManager at NameNode/152.77.78.100:8032
15/01/28 10:51:18 INFO input.FileInputFormat: Total input paths to process : 5
15/01/28 10:51:18 INFO mapreduce.JobSubmitter: number of splits:5
15/01/28 10:51:18 INFO Configuration.deprecation: user.name is deprecated. Instead, use mapreduce.job.user.name
15/01/28 10:51:18 INFO Configuration.deprecation: mapred.jar is deprecated. Instead, use mapreduce.job.jar
15/01/28 10:51:18 INFO Configuration.deprecation: mapred.output.value.class is deprecated. Instead, use mapreduce.job.output.value.class
15/01/28 10:51:18 INFO Configuration.deprecation: mapred.mapoutput.value.class is deprecated. Instead, use mapreduce.map.output.value.class
15/01/28 10:51:18 INFO Configuration.deprecation: mapreduce.map.class is deprecated. Instead, use mapreduce.job.map.class
15/01/28 10:51:18 INFO Configuration.deprecation: mapred.job.name is deprecated. Instead, use mapreduce.job.name
15/01/28 10:51:18 INFO Configuration.deprecation: mapreduce.reduce.class is deprecated. Instead, use mapreduce.job.reduce.class
15/01/28 10:51:18 INFO Configuration.deprecation: mapreduce.inputformat.class is deprecated. Instead, use mapreduce.job.inputformat.class
15/01/28 10:51:18 INFO Configuration.deprecation: mapred.input.dir is deprecated. Instead, use mapreduce.input.fileinputformat.inputdir
15/01/28 10:51:18 INFO Configuration.deprecation: mapred.output.dir is deprecated. Instead, use mapreduce.output.fileoutputformat.outputdir
15/01/28 10:51:18 INFO Configuration.deprecation: mapreduce.outputformat.class is deprecated. Instead, use mapreduce.job.outputformat.class
15/01/28 10:51:18 INFO Configuration.deprecation: mapred.map.tasks is deprecated. Instead, use mapreduce.job.maps
15/01/28 10:51:18 INFO Configuration.deprecation: mapred.output.key.class is deprecated. Instead, use mapreduce.job.output.key.class
15/01/28 10:51:18 INFO Configuration.deprecation: mapred.mapoutput.key.class is deprecated. Instead, use mapreduce.map.output.key.class
15/01/28 10:51:18 INFO Configuration.deprecation: mapred.working.dir is deprecated. Instead, use mapreduce.job.working.dir
15/01/28 10:51:18 INFO mapreduce.JobSubmitter: Submitting tokens for job: job_1415831786943_0406
15/01/28 10:51:18 INFO impl.YarnClientImpl: Submitted application application_1415831786943_0406 to ResourceManager at NameNode/152.77.78.100:8032
15/01/28 10:51:18 INFO mapreduce.Job: The url to track the job: http://152.77.78.100:8089/proxy/application_1415831786943_0406/
15/01/28 10:51:18 INFO mapreduce.Job: Running job: job_1415831786943_0406
15/01/28 10:51:25 INFO mapreduce.Job: Job job_1415831786943_0406 running in uber mode : false
15/01/28 10:51:25 INFO mapreduce.Job:  map 0% reduce 0%
15/01/28 10:51:32 INFO mapreduce.Job:  map 20% reduce 0%
15/01/28 10:51:33 INFO mapreduce.Job:  map 40% reduce 0%
15/01/28 10:51:34 INFO mapreduce.Job:  map 60% reduce 0%
15/01/28 10:51:35 INFO mapreduce.Job:  map 100% reduce 0%
15/01/28 10:51:40 INFO mapreduce.Job:  map 100% reduce 100%
15/01/28 10:51:40 INFO mapreduce.Job: Job job_1415831786943_0406 completed successfully
15/01/28 10:51:40 INFO mapreduce.Job: Counters: 44
	File System Counters
		FILE: Number of bytes read=5063675
		FILE: Number of bytes written=10609045
		FILE: Number of read operations=0
		FILE: Number of large read operations=0
		FILE: Number of write operations=0
		HDFS: Number of bytes read=2536148
		HDFS: Number of bytes written=623905
		HDFS: Number of read operations=18
		HDFS: Number of large read operations=0
		HDFS: Number of write operations=2
	Job Counters 
		Launched map tasks=5
		Launched reduce tasks=1
		Data-local map tasks=4
		Rack-local map tasks=1
		Total time spent by all maps in occupied slots (ms)=272960
		Total time spent by all reduces in occupied slots (ms)=39368
	Map-Reduce Framework
		Map input records=52711
		Map output records=421739
		Map output bytes=4220191
		Map output materialized bytes=5063699
		Input split bytes=610
		Combine input records=0
		Combine output records=0
		Reduce input groups=52555
		Reduce shuffle bytes=5063699
		Reduce input records=421739
		Reduce output records=52555
		Spilled Records=843478
		Shuffled Maps =5
		Failed Shuffles=0
		Merged Map outputs=5
		GC time elapsed (ms)=244
		CPU time spent (ms)=12920
		Physical memory (bytes) snapshot=1581019136
		Virtual memory (bytes) snapshot=7464501248
		Total committed heap usage (bytes)=1226440704
	Shuffle Errors
		BAD_ID=0
		CONNECTION=0
		IO_ERROR=0
		WRONG_LENGTH=0
		WRONG_MAP=0
		WRONG_REDUCE=0
	File Input Format Counters 
		Bytes Read=2535538
	File Output Format Counters 
		Bytes Written=623905
```

On peut voir la ligne suivante, au début de la trace :
```
15/01/28 10:51:18 INFO mapreduce.JobSubmitter: number of splits:5
```
Elle indique que le nombre de splits est de *5*, ceci signifiant que *hdfs* a lus *5* fichiers en input (les 5 tomes des *Misérables*).

1.4 Combiner
------------

On rajoute la classe du `reducer` dans `setCombinerClass`.
La trace obtenue est la suivante :

```
odinpi@NameNode:~$ hadoop jar tpC.jar WordCount /data/miserables wordcountC
15/01/28 11:10:04 WARN util.NativeCodeLoader: Unable to load native-hadoop library for your platform... using builtin-java classes where applicable
15/01/28 11:10:05 INFO client.RMProxy: Connecting to ResourceManager at NameNode/152.77.78.100:8032
15/01/28 11:10:05 INFO input.FileInputFormat: Total input paths to process : 5
15/01/28 11:10:05 INFO mapreduce.JobSubmitter: number of splits:5
15/01/28 11:10:05 INFO Configuration.deprecation: user.name is deprecated. Instead, use mapreduce.job.user.name
15/01/28 11:10:05 INFO Configuration.deprecation: mapred.jar is deprecated. Instead, use mapreduce.job.jar
15/01/28 11:10:05 INFO Configuration.deprecation: mapred.output.value.class is deprecated. Instead, use mapreduce.job.output.value.class
15/01/28 11:10:05 INFO Configuration.deprecation: mapred.mapoutput.value.class is deprecated. Instead, use mapreduce.map.output.value.class
15/01/28 11:10:05 INFO Configuration.deprecation: mapreduce.combine.class is deprecated. Instead, use mapreduce.job.combine.class
15/01/28 11:10:05 INFO Configuration.deprecation: mapreduce.map.class is deprecated. Instead, use mapreduce.job.map.class
15/01/28 11:10:05 INFO Configuration.deprecation: mapred.job.name is deprecated. Instead, use mapreduce.job.name
15/01/28 11:10:06 INFO Configuration.deprecation: mapreduce.reduce.class is deprecated. Instead, use mapreduce.job.reduce.class
15/01/28 11:10:06 INFO Configuration.deprecation: mapreduce.inputformat.class is deprecated. Instead, use mapreduce.job.inputformat.class
15/01/28 11:10:06 INFO Configuration.deprecation: mapred.input.dir is deprecated. Instead, use mapreduce.input.fileinputformat.inputdir
15/01/28 11:10:06 INFO Configuration.deprecation: mapred.output.dir is deprecated. Instead, use mapreduce.output.fileoutputformat.outputdir
15/01/28 11:10:06 INFO Configuration.deprecation: mapreduce.outputformat.class is deprecated. Instead, use mapreduce.job.outputformat.class
15/01/28 11:10:06 INFO Configuration.deprecation: mapred.map.tasks is deprecated. Instead, use mapreduce.job.maps
15/01/28 11:10:06 INFO Configuration.deprecation: mapred.output.key.class is deprecated. Instead, use mapreduce.job.output.key.class
15/01/28 11:10:06 INFO Configuration.deprecation: mapred.mapoutput.key.class is deprecated. Instead, use mapreduce.map.output.key.class
15/01/28 11:10:06 INFO Configuration.deprecation: mapred.working.dir is deprecated. Instead, use mapreduce.job.working.dir
15/01/28 11:10:06 INFO mapreduce.JobSubmitter: Submitting tokens for job: job_1415831786943_0421
15/01/28 11:10:06 INFO impl.YarnClientImpl: Submitted application application_1415831786943_0421 to ResourceManager at NameNode/152.77.78.100:8032
15/01/28 11:10:06 INFO mapreduce.Job: The url to track the job: http://152.77.78.100:8089/proxy/application_1415831786943_0421/
15/01/28 11:10:06 INFO mapreduce.Job: Running job: job_1415831786943_0421
15/01/28 11:10:12 INFO mapreduce.Job: Job job_1415831786943_0421 running in uber mode : false
15/01/28 11:10:12 INFO mapreduce.Job:  map 0% reduce 0%
15/01/28 11:10:18 INFO mapreduce.Job:  map 20% reduce 0%
15/01/28 11:10:24 INFO mapreduce.Job:  map 100% reduce 0%
15/01/28 11:10:27 INFO mapreduce.Job:  map 100% reduce 100%
15/01/28 11:10:27 INFO mapreduce.Job: Job job_1415831786943_0421 completed successfully
15/01/28 11:10:27 INFO mapreduce.Job: Counters: 44
	File System Counters
		FILE: Number of bytes read=1303818
		FILE: Number of bytes written=3090255
		FILE: Number of read operations=0
		FILE: Number of large read operations=0
		FILE: Number of write operations=0
		HDFS: Number of bytes read=2536148
		HDFS: Number of bytes written=623905
		HDFS: Number of read operations=18
		HDFS: Number of large read operations=0
		HDFS: Number of write operations=2
	Job Counters 
		Launched map tasks=5
		Launched reduce tasks=1
		Data-local map tasks=4
		Rack-local map tasks=1
		Total time spent by all maps in occupied slots (ms)=308360
		Total time spent by all reduces in occupied slots (ms)=35944
	Map-Reduce Framework
		Map input records=52711
		Map output records=421739
		Map output bytes=4220191
		Map output materialized bytes=1303842
		Input split bytes=610
		Combine input records=421739
		Combine output records=85301
		Reduce input groups=52555
		Reduce shuffle bytes=1303842
		Reduce input records=85301
		Reduce output records=52555
		Spilled Records=170602
		Shuffled Maps =5
		Failed Shuffles=0
		Merged Map outputs=5
		GC time elapsed (ms)=222
		CPU time spent (ms)=15400
		Physical memory (bytes) snapshot=1653313536
		Virtual memory (bytes) snapshot=7361413120
		Total committed heap usage (bytes)=1226899456
	Shuffle Errors
		BAD_ID=0
		CONNECTION=0
		IO_ERROR=0
		WRONG_LENGTH=0
		WRONG_MAP=0
		WRONG_REDUCE=0
	File Input Format Counters 
		Bytes Read=2535538
	File Output Format Counters 
		Bytes Written=623905
```

 1. Les compteurs qui permettent de savoir que le `combiner` a fonctionné sont les suivants : `Reduce input record`.
 2. Les compteurs qui permettent d’estimer le gain effectivement apporté par le `combiner` sont les suivants : `Combine input records=421739` et  `Combine output records=85301`.

En compariason :

Compteur | Avec `combiner` | Sans `combiner`
--- | --- | ---
Reduce input records | 85301 | 421739

Pour information le terme le plus utilisé dans *Les Misérables* est **de** (**16757** fois).

1.5 Nombre de reducers
----------------------

Par défaut, un seul `reducer` est instancié, donc il génère un seul fichier de sortie `part-r-00000`. Quand on augmente le nombre de `reducer` à 3, il est normal que le système génère 3 fichiers de sortie, correspondant aux trois exécutions. On notera que la concaénation des 3 fichiers corresponds au fichier obtenue avec un seul `reducer` modulo l'ordre des lignes. 

```
odinpi@NameNode:~$ wc -l wordcountC3R/part-r-00000
17401 wordcountC3R/part-r-00000
odinpi@NameNode:~$ wc -l wordcountC3R/part-r-00001
17491 wordcountC3R/part-r-00001
odinpi@NameNode:~$ wc -l wordcountC3R/part-r-00002
17663 wordcountC3R/part-r-00002
odinpi@NameNode:~$ wc -l wordcount/part-r-00000 
52555 wordcount/part-r-00000
```

1.7 Compteur
------------

```
public static enum COUNTER_EMPTY_LINE { N; }

// Dans le mapper		
if (word.length() == 0)
 context.getCounter(COUNTER_EMPTY_LINE.N).increment(1);

// Dans le main		
job.waitForCompletion(true);
Counters counters = job.getCounters();
Counter c1 = counters.findCounter(COUNTER_EMPTY_LINE.N);
System.out.println(c1.getDisplayName()+":"+c1.getValue());
```

2.1 Map et Reduce
-----------------

2.2 Combiner
------------

Le type de données intermédiaires : le réduceur doit prend en entré un ```Text/StringAndInt``` (doit implémenter ```Writable``` ) et le combineur doit prendre en entrée ```Text/Text``` provenant du mappeur et doit fournir un ```Text/StringAndInt``` au réduceur.

* ```Text/Text```  : Un pays / un tag
* ```Text/StringAndInt``` : Un pays / (un tag, nombre d'occurences)

Les tags les plus utilisés en France sont :
```
odinpi@NameNode:~$ cat flickr/part-r-00000  | grep FR
FR	france 563
FR	spain 113
FR	europe 75
FR	españa 70
FR	bretagne 67
```

Dans le reducer, nous avons une structure en mémoire dont la taille dépend du nombre de tags distincts : on ne le connaît pas a priori, et il y en a potentiellement beaucoup, ceci n'est pas un problème, car plusieurs machines vont traiter notre problème, de plus en cas de baisse de performance, nous pouvons toujours ajouter à notre Hadoop.

3 Top-tags Flickr par pays, avec tri par Hadoop
-----------------------------------------------

Pour résoudre notre problème, nous allons utiliser 2 jobs (donc 2 couples ```Map/Reducer```). La sortie du premier job sera l'entré du deuxième job. En outre, la spécification des jobs est la suivante :
* Job1 -> Map : Cherche les tag est créer la stucture StringAndInt (idem question précédente).
* Job1 -> Reducer : Regroupe les StringAndInt de même tag (clef) en un (idem question preécédente). 
* Job2 -> Map : Renvoie les données.
* Job2 -> secondarySort : trie par nombre d'occurence.
* Job2 -> grouping : groupe les données par country.
* Job2 -> Reducer : traite le nombre de données souhaité.

L'avantage ici, c'est que le réduceur n'a pas besoin de gérer une autre structure (de map) comme pour les questions précédente. Il a juste besoin de retourner les K premiers éléments des groupes qu'ils recoient.

S’il existe des tags classés ex aequo dans le top-K d’un pays, alors rien ne garatie que le résultat soit identique. En effet, le problème vient de l'ordre d'arrivé des tags ex aequo, il s'agit d'un probème classique dans les système de trie en concurence, néamoins, c'est Hadoop qui gère cette partie donc nous n'avons aucune idée du résultat.
En réalité sur une expérimentation (5 éxecutions) le résultat est identique, donc potentiellement, dans notre cas d'execution, hadoop le résultat semble déterministe. On pourrai vérifier cette hypothèse en modifiant l'ordre du fichier d'entrée est vérifier que les résultats soient différents. 
Il est cependant possible de forcer (l'execution déterministe) en rajoutant le trie par tag quand les tag sont ex eaquo en terme de fréquence.

