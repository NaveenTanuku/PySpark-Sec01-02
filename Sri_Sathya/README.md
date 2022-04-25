### This project is to find the page rank for the given folder which is web04.


#### Check your java version using:
```
java --version

```




#### Generate a maven project by using the following command: 

```
mvn archetype:generate -D archetypeGroupId=org.apache.beam -D archetypeArtifactId=beam-sdks-java-maven-archetypes-examples -D archetypeVersion=2.36.0 -D groupId=org.example -D artifactId=word-count-beam -D version="0.1" -D package=org.apache.beam.examples -D interactiveMode=false

```

#### Run the minimal page rank with the following command: 

```
mvn compile exec:java -D exec.mainClass=edu.nwmissouri.bigdatapy.sathya.MinimalPageRankSat

```


### Links: 

[wiki](https://github.com/NaveenTanuku/PySpark-Sec01-02/wiki/sri-sathya-mamidala)

[issues](https://github.com/NaveenTanuku/PySpark-Sec01-02/issues)

[Minimal page rank file](https://github.com/NaveenTanuku/PySpark-Sec01-02/blob/main/Sri_Sathya/src/main/java/org/apache/beam/examples/MinimalPageRankSat.java)
