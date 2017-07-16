# presto-sample-udf
Sample UDF Plugin for Presto

## How to start Presto UDF

1. Read these documents.  
   - [10\.1\. SPI Overview — Presto 0\.180 Documentation](https://prestodb.io/docs/current/develop/spi-overview.html)
   - [10\.5\. Functions — Presto 0\.180 Documentation](https://prestodb.io/docs/current/develop/functions.html)
   
## Build

```bash
% ./mvnw clean compile package
```
   
## deploy

Copy jar file to plugin dir. And, you restart presto server.
   
```bash
% mkdir /usr/lib/presto/lib/plugin/hello/
% cp target/presto-sample-udf-1.0-SNAPSHOT.jar /usr/lib/presto/lib/plugin/hello/
% /usr/lib/presto/bin/launcher restart
```
   
## Examples

- [prestodb\-rocks/presto\-example\-udf: An example implentantion of Presto UDF functions](https://github.com/prestodb-rocks/presto-example-udf)
  Basic UDF sample code and unittest.
- [aaronshan/presto\-third\-functions: Some useful presto custom udf functions](https://github.com/aaronshan/presto-third-functions)
- [qubole/presto\-udfs: Plugin for Presto to allow addition of user functions easily](https://github.com/qubole/presto-udfs)
  Various UDF Sample Repository.