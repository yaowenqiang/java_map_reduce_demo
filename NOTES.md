hadoop jar WordCount.jar WordCount word.txt output

# HDFS

set replication numbers

hdfs-site.xml

> dfs.replication

https://hadoop.apache.org/

java api doc

https://hadoop.apache.org/docs/stable/api/index.html

file system shell

https://hadoop.apache.org/docs/stable/hadoop-project-dist/hadoop-common/FileSystemShell.html

job history url

localhost:8080


Map-Reduce Process Flow

map -> combine -> shuffle -> reduce


the in put and out put format in combine must match

```java

import org.apache.hadoop.io.LongWritable;

public class Map extends
        Reduce<Text, Text, Text, LongWritable> {
}

public class Combine extends
        Reduce<Text, LongWritable, Text, LongWritable> {
}

public class Combine extends
        Reduce<Text, LongWritable, Text, Text> {
}
```