from neo4j.v1 import GraphDatabase, basic_auth
import itertools
import subprocess
import sys
import re

SEPARATORE = '\t'
dizionario_pesi = {}

#restituisce il campo dato in input nel formato "Xxxxxx" cioe' con la prima lettera maiuscola
def formatta_campo(campo):
	campo = campo.lower()
	campo = campo[0].upper() + campo[1:]
	return campo

#restituisce tutti i nodi del tipo "target" che sono direttamente connessi al nodo avente "node_id"
def ricerca_nodi(node_id, target):
	elenco = ''	
	campo_target = formatta_campo(sys.argv[1])														#ottiene campo nel formato "Xxxxxx"
	target = campo_target.lower()																			#ottiene campo nel formato "xxxxxx"
	query2 = 'MATCH (nodo_intermedio) WHERE ID(nodo_intermedio) = ' + str(node_id) + ' WITH nodo_intermedio MATCH (' + str(target) + ':' + str(campo_target) + ')-[r]-(nodo_intermedio) RETURN ' + str(target)

	result = session.run(query2)

	for element in result:
		elenco += str(element)
		elenco += '\t'	
		
	return elenco

#per ogni elemento del file di output del mapper calcola lo score moltiplicando il peso del nodo per il numero di clienti che condividono quel nodo
def calcola_punteggio():
	nodi_pesati = open("risultati/nodi_pesati.txt", "w")
	
	with open('risultati/out_mr/part-00000') as nodi:
		lines = nodi.read().split("\n")	

	for row in lines[:-1]:
		elements = row.split("\t")
		nodo = elements[0]
		n = int(elements[1])
		
		categoria = nodo.split(",")
		categoria = categoria[1].strip("]")
		score = int(dizionario_pesi[categoria])
		
		if score == 11:																						#in questo caso si tratta di un nodo che se condiviso implica necessariamente truffa (es. docId condiviso)
			totale = 10000
	
		else:	
			totale = score * n
		stringa = str(nodo) + "\t" + str(totale) + "\n"
		nodi_pesati.write(stringa)
		
	nodi_pesati.close()


#legge il file di testo "pesi.txt" e inserisce il contenuto in un dizionario
def leggi_pesi():
	with open('risultati/pesi.txt') as pesi:
		lines = pesi.read().split("\n")
	 
	for row in lines[:-1]:
		elements = row.split(" ")
		key = elements[0]
		value = elements[1]
		dizionario_pesi[key] = value
		
	pesi.close()	

#stampa in un file di testo i pesi di ogni nodo
def stampa_pesi():
	pesi = open("risultati/pesi.txt", "w")	

	i = 0	
	for element in sys.argv[2:]:																			#questo ciclo serve a stampare a coppie gli argomenti sulla stessa riga
		if not i % 2:
			pesi.write(element + ' ')
		else:
			pesi.write(element)		
		
		if i % 2:																								#se ha stampato due argomenti (nome e peso) va a capo 
			pesi.write("\n")
			
		i += 1
		
	pesi.close()		

#a partire dall'input dato sulla stringa di comando crea la query per il db e la restituisce
def crea_query():
	campi_da_includere = ''
	campo_target = formatta_campo(sys.argv[1])

	argomenti_pari = sys.argv[2::2]
	
	for element in argomenti_pari:
		campi_da_includere += "'" + formatta_campo(element) + "'"
	
	campi_da_includere = campi_da_includere.replace("''", "','")
	campi_da_includere = "[" + campi_da_includere + "]" 	
	target1 = (campo_target + "1").lower() 
	target2 = (campo_target + "2").lower()

	query = 'MATCH (' + target1 + ':' + campo_target + ')-[r1]-(intermedio)-[r2]-(' + target2 + ':' + campo_target + ') WHERE labels(intermedio) IN ' + campi_da_includere + ' AND id(' + target1 + ') > id(' + target2 + ') RETURN ' + target1 + ', ' + target2 + ', r1, r2, intermedio'
		
	stampa_pesi()	
		
	return query, target1, target2
	
#connessione a Neo4j
driver = GraphDatabase.driver("bolt://localhost:7687", auth=basic_auth("neo4j", "password"))
session = driver.session()

#crea cartella per tutti i risultati
subprocess.call("mkdir risultati", shell=True)

#crea la query in base all'input che e' stato dato e la esegue
query, target1, target2 = crea_query()
result = session.run(query)

input_mapper = open("risultati/input_mapper.txt", "w")

for element in result:
	n1 = str(element[target1])
	n2 = str(element[target2])
	intermedio = str(element['intermedio']) 
	
	n1_id = n1.split("id=")
	n1_id = n1_id[1].split(" ", 1)
	n1_id = n1_id[0]
	
	n2_id = n2.split("id=")
	n2_id = n2_id[1].split(" ", 1)
	n2_id = n2_id[0]

	intermedio_id = intermedio.split("id=")
	intermedio_id = intermedio_id[1].split(" ", 1)
	intermedio_id = intermedio_id[0]

	intermedio_type = intermedio.split("set([u'")
	intermedio_type = intermedio_type[1].split("']) ", 1)
	intermedio_type = intermedio_type[0]
	
	if re.search(intermedio_type, "IdentificativoAnagrafico', u'CodiceFiscale", re.IGNORECASE):
		intermedio_type = "CodiceFiscale"

	if re.search(intermedio_type, "PartitaIva', u'IdentificativoAnagrafico", re.IGNORECASE):		
		intermedio_type = "PartitaIva"

	row = '[' + intermedio_id + ',' + intermedio_type + ']' + SEPARATORE + n1_id + SEPARATORE + n2_id
	input_mapper.write(row + '\n')	

input_mapper.close()


subprocess.call("$HADOOP_HOME/bin/hdfs dfs -put risultati/input_mapper.txt $INPUT_HADOOP", shell=True)
subprocess.call("/usr/local/hadoop/bin/hadoop jar /usr/local/hadoop/contrib/hadoop-streaming-2.7.3.jar -file mapper_1.py -mapper mapper_1.py -file reducer_1.py -reducer reducer_1.py -input $INPUT_HADOOP/input_mapper.txt -output $OUTPUT_HADOOP/out_mr", shell=True)
subprocess.call("hdfs dfs -get $OUTPUT_HADOOP/out_mr risultati", shell=True)

leggi_pesi()
calcola_punteggio()
subprocess.call("$HADOOP_HOME/bin/hdfs dfs -put risultati/nodi_pesati.txt $INPUT_HADOOP", shell=True)
subprocess.call("/usr/local/hadoop/bin/hadoop jar /usr/local/hadoop/contrib/hadoop-streaming-2.7.3.jar -D mapred.output.key.comparator.class=org.apache.hadoop.mapred.lib.KeyFieldBasedComparator -D  mapred.text.key.comparator.options=-nr -input $INPUT_HADOOP/nodi_pesati.txt -output $OUTPUT_HADOOP/out_pesato -file mapper_ordina.py -mapper mapper_ordina.py -file reducer_ordina.py -reducer reducer_ordina.py", shell=True)
subprocess.call("hdfs dfs -get $OUTPUT_HADOOP/out_pesato risultati", shell=True)
subprocess.call("cat risultati/out_pesato/part* > risultati/elenco_punteggi_per_nodo_ordinato.txt", shell=True)



#legge riga per riga l'elenco dei nodi ordinati in base allo score
with open('risultati/elenco_punteggi_per_nodo_ordinato.txt') as elenco_ordinato:
	lines = elenco_ordinato.read().split("\n")

risultato_finale = open("risultati/risultato_finale.txt", "w")	

#legge ogni riga ed estrae per ognuna lo score, id e tipo di nodo	
for row in lines[:-1]:
	score, data = row.split("\t")
	data = data.lstrip("[")
	data = data.rstrip("]")
	node_id, node_type = data.split(",")
	node_id = int(node_id)
	
	#per ogni node_id va a prendere tutti i nodi ad esso collegati
	elements = ricerca_nodi(node_id, target1)
	
	risultato_finale.write("score: " + str(score) + "\n")
	risultato_finale.write("node_id: " + str(node_id) + "\n")
	risultato_finale.write("node_type: " + str(node_type) + "\n")
	risultato_finale.write("elements: " + elements + "\n")
	risultato_finale.write("\n")
	

session.close