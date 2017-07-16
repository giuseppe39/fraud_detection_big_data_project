# fraud_detection_big_data_project
Secondo progetto corso di Big Data

Istruzioni per l'esecuzione degli script.


Script "classificatore.py"
________________________________________________________________________________________________________________________________

######################
#Variabili di ambiente
######################
Per l'esecuzione dello script è necessario impostare le variabili di ambiente 
INPUT_HADOOP=/.../.../.../			path in cui si trovano i file di input in HDFS
OUTPUT_HADOOP=/.../.../.../			path in cui si vogliono salvare i file di output in HDFS


#############
#Prerequisiti
#############
Per l'esecuzione bisogna installare Hadoop, scaricando l'estensione "Hadoop Streaming" per l'esecuzione con Python.
Fare il download di Hadoop Streaming nella posizione "/usr/local/hadoop/contrib/hadoop-streaming-2.7.3.jar" oppure modificare il path nello script
Avviare Hadoop e Neo4j.
Le API di Neo4j devono essere raggiungibili all'indirizzo bolt://localhost:7687 (oppure cambiarlo nello script).
E' necessario inserire nello script la password di connessione al db (riga 111 dello script).


######
#Input
######
L'input dello script da linea di comando è del tipo:
python classificatore.py nodo_interesse [nodo_da_ricercare peso_del_nodo]

Dove:
- il primo argomento dopo il nome dello script indica il tipo del nodo di interesse, ad esempio in questo caso, l'obiettivo è di trovare indicazioni relative ai Clienti fraudolenti e nel db ci sono dei nodi appunto del tipo "Clienti";
- gli altri argomenti sono le etichette dei nodi intermedi su cui andare a fare la ricerca con associato uno score di importanza, tutti separati da uno spazio.

I nodi intermedi su cui andare a fare la ricerca sono quelli che condivisi fanno presumere una frode; i loro score indicano quante probabilità ci sono che, se quel nodo è condiviso, ci si trova in presenza di una frode.

Esempio:
python classificatore.py Cliente CodiceFiscale 11  DocId 11 Documento 11 Email 1 IdentificativoAnagrafico 11 Indirizzo 1 Ordine 8 PartitaIVA 11 Referente 5 Telefono 3 Via 1


#######
#Output
#######
Lo script restituisce i file di seguito elencati; gli stessi vengono salvati automaticamente nella cartella "Risultati" appositamente creata dallo script nella posizione in cui viene lo stesso eseguito.

pesi.txt
File contenente labels e pesi inseriti in input allo script, vengono salvati in un file per essere riutilizzati dallo script.

nodi_pesati.txt
File contenente l'elenco di tutti i nodi, con relativo score.

elenco_punteggi_per_nodo_ordinato.txt
File contenente la lista di tutti i nodi con relativo punteggio in ordine decrescente.

input_mapper.txt
File contenente l'elenco di tutti i nodi di interesse con coppie di altri nodi ad esso collegati.

out_mr
Cartella di output del primo MapReduce

out_pesato
Cartella di output del secondo MapReduce

risultato_finale.txt
File contenenre il risultato dell'intera elaborazione consistente nell'elenco di tutti i nodi con relativo id, tipo, score e lista di utenti ad esso collegati.




script "query_3_modificata.py"
________________________________________________________________________________________________________________________________

#############
#Prerequisiti
#############
E' necessario inserire nello script la password di connessione al db (riga 7 dello script).

#######
#Output
#######
Viene restituito in quattro file creati nella stessa posizione dello script.





Script "mapper_1.py", "reducer_1.py", "mapper_ordina.py", "reducer_ordina.py"
________________________________________________________________________________________________________________________________
Vengono eseguiti in autonomia dallo script "classificatore.py".

