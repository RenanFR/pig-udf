# pig-udf
*Copiar os arquivos do diretório resources para o tmp do Linux*

**Copiar o dataset e o jar da UDF para o diretório tmp no HDFS**
```
hadoop fs -put /tmp/fifa19.csv /tmp
```
```
hadoop fs -put /tmp/BestLeftFootStriker.jar /tmp
```
**Executar o script**
```
pig -f /tmp/get_best_lfst.pig
```
