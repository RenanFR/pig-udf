# pig-udf
**Copiar o dataset e o jar da UDF para o diretório tmp no HDFS**
```
hadoop fs -put /tmp/fifa19.csv /tmp
```
```
hadoop fs -put /tmp/BestLeftFootStriker.jar /tmp
```
**Executar o script**
```
pig -f /home/cloudera/Documents/get_best_lfst.pig
```
