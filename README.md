# Task3

1 - Make sure that you have JDK 8, Maven and Scala 12.4

Then you can compile with:
```sh
mvn clean install
```

2 - Copy Task3 and lib folder to your spark cluster and run with

```
spark-submit --class br.gsj.Task3 --jars $(echo lib/*.jar | tr ' ' ',') --master <your_master> TalpaTask-1.0.jar <folder where files will be stored>
```
