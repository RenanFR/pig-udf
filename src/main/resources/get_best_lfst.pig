REGISTER /tmp/BestLeftFootStriker.jar;

jogadores = LOAD '/tmp/fifa19.csv' using PigStorage(';') AS (nome:chararray, idade:int, nacionalidade:chararray, nota:int, potencial:int, clube:chararray, valor:chararray, salario:chararray, pe_fav:chararray, pe_fraco:chararray, habilidades:int, tipo_corpo:chararray, posicao:chararray, nr_camisa:int, ano_val_contrato:int, peso:chararray, altura:chararray, val_rescisao:chararray);

melhores = FILTER jogadores BY pig.udf.BestLeftFootStriker(nome, pe_fav, nota, posicao);

dump melhores;
