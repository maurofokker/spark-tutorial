# sparkTutorial
Project source code for James Lee's Aparch Spark with Java course.

Check out the full list of DevOps and Big Data courses that James and Tao teach.

https://www.level-up.one/courses/

## Transformation vs Actions

`Transformations` returns RDDs, whereas `actions` return some other data type.

### Transformation

```java
  JavaRDD<String> linesWithFriday = lines.filter(line -> line.contains("Friday"));

  JavaRDD<Integer> lengths = lines.map(line -> line.length());
```

### Actions

```java
  List<String> words = wordRdd.collect();

  String firstLine = lines.first();
```