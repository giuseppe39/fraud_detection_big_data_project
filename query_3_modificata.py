from neo4j.v1 import GraphDatabase, basic_auth
import itertools
import difflib
import json
import ast

driver = GraphDatabase.driver("bolt://localhost:7687", auth=basic_auth("neo4j", "password"))
session = driver.session()
result = session.run("MATCH (cliente)-[:HA_INDIRIZZO_EMAIL]->(email:Email) WITH email, count(cliente) AS clienti WHERE clienti > 1 WITH email MATCH (c:Cliente)-[HA_INDIRIZZO_EMAIL]->(email) WITH c, email MATCH (c)-[:HA_RESIDENZA_O_SEDE_IN]->(indirizzo:Indirizzo) RETURN c, email, indirizzo")
							 
sostituzioni = {'v.':'via ','l.go':'largo ', 'p.zza':'piazza ', 'c.so':'corso ', 'staz.':'stazione ', 'staz':'stazione ', 'v.le':'viale '}
interrogazioni = {}
email_non_sospette = []
email_sospette = []
cf_sospetti = []
cf_non_sospetti = []

i = 0

#estrapola solo i valori di interesse e li restituisce sottoforma di dizionario
def formatta(stringa):
        elements = str(stringa).split("properties=")              
        stringa = elements[1].strip(">")
        stringa = ast.literal_eval(stringa)                       
        stringa = json.dumps(stringa)
        return stringa



#verifica somiglianza tra due stringhe (effettuando una sorta di normalizzazione preventiva)dia_comune, media_via = somiglianze_coppie(interrogazioni[key]
def verifica_stringhe_simili(s1,s2):										
	s1 = s1.lower()							
	s2 = s2.lower()

	res_1 = difflib.SequenceMatcher(None, s1, s2)
	res_1 = res_1.quick_ratio()	

	#esegue un minimo di normalizzazione e ricalcola la somiglianza
	for key in sostituzioni:													
		if key in s1:
			s1 = s1.replace(key, sostituzioni[key])
			
	for key in sostituzioni:
		if key in s2:
			s2 = s2.replace(key, sostituzioni[key])
	
	s1 = s1.replace(' ', '')
	s2 = s2.replace(' ', '')	

	res_2 = difflib.SequenceMatcher(None, s1, s2)
	res_2 = res_2.quick_ratio()

	return max(res_1, res_2)													#restituisce il valore migliore (tra quello calcolato sulla stringa normalizzata e quello sulla stringa non normalizzata)



#per ogni coppia di clienti che usano la stessa email verifica se abitano nella stessa citta' e nello stesso indirizzo (confronto tra citta' e confronto tra indirizzi)
def somiglianze_coppie(clients):
	count_comune = 0
	tot_comune = 0
	count_via = 0
	tot_via = 0	
		
	for c in itertools.combinations(clients, 2):																			#per tutte le coppie calcola l'indice di somiglianza
		element_1 = json.loads(c[0][1])
		element_2 = json.loads(c[1][1])
	
		tot_comune += verifica_stringhe_simili(element_1['comune'], element_2['comune'])						#tra tutti gli indici calcolati (coppie che usano la stessa email) calcola la media
		count_comune += 1		
		
		tot_via += verifica_stringhe_simili(element_1['via'], element_2['via'])
		count_via += 1			

	return tot_comune/count_comune, tot_via/count_via 



#ad ogni email associa l'elenco dei clienti (dati_cliente, indirizzo) che l'hanno utilizzata
def salva_record(email, query_result):
	if interrogazioni.has_key(email):
		interrogazioni[email].append(query_result)

	else:
		interrogazioni[email] = [query_result]



#per ogni record dei risultati legge solo le informazioni di interesse (quelle in properties)
def leggi_record(record):
	query_result = []
		
	cliente = formatta(record[0])											#prende il primo client dal result
	email = formatta(record[1])											#prende l'email
	indirizzo = formatta(record[2])										#prende l'indirizzo del cliente

	query_result.append(cliente)
  	query_result.append(indirizzo)

	email = json.loads(email)
	salva_record(email['indirizzo'], query_result)


#memorizza il cf nell'elenco dei sospetti/non sospetti (a seconda del valore True/False di ctrl)
def salva_cf(element, ctrl):
	for i in element:
		i = json.loads(i[0])		
		cf = i['cf']

		if ctrl == True:														#se derivano da email sospette (argomento passato True)
			cf_sospetti.append(cf)
			
		else:																		#se derivano da email non sospette (argomento passato False)
			cf_non_sospetti.append(cf)



for record in result:
	leggi_record(record)       



for key in interrogazioni:
	dim = len(interrogazioni[key])
	if dim > 1:																	#capita nei riusultati il caso in cui un cliente ha attivato piu' contratti a nome suo (con gli stessi dati), in questo caso per la query fatta al graphdb la sua anagrafica compare una sola volta associata all'email: questo caso non interessa (difficilmente nelle frodi si attivano tanti contratti con gli stessi identific dati), del resto nel metodo "somiglianze_coppie" determinerebbe una divisione per zero  
		media_comune, media_via = somiglianze_coppie(interrogazioni[key])

	
	if dim > 6:																	#se con un'email sono stati attivati piu' di 7 contratti, a priori sono considerati come fraudolenti (personalizzabile))
		email_sospette.append(key)		
		salva_cf(interrogazioni[key], True)
	
	else:
		if media_comune < 0.7:												#se i comuni sono diversi allora quei contratti sono sospetti (0.7 per prevedere un piccolo margine di errore o di variabilita' nella digitazione dello stesso comune)
			email_sospette.append(key)
			salva_cf(interrogazioni[key], True)
			
	 	else:
	 		if media_via < 0.7:												#se le strade sono sospette, allora quei contratti sono sospetti (previsto margine di errore piu' ampio)
	 			email_sospette.append(key)
				salva_cf(interrogazioni[key], True)			 			
	 			
	 		else:
	 			email_non_sospette.append(key)							#se con un'email sono stati attivati pochi contratti, se sono riferite tutte a persone che abitano nello stesso comune e stessa via allora non sono sospetti
				salva_cf(interrogazioni[key], False)

email_sospette_to_file = open('email_sospette_query_3_1.txt', 'w')
email_non_sospette_to_file = open('email_non_sospette_query_3_1.txt', 'w')
cf_sospetti_to_file = open('cf_sospetti_query_3_1.txt', 'w')
cf_non_sospetti_to_file = open('cf_non_sospetti_query_3_1.txt', 'w')

cf_non_sospetti = list(set(cf_non_sospetti))
cf_sospetti = list(set(cf_sospetti))

for item in email_non_sospette:
  email_non_sospette_to_file.write("%s\n" % item)

email_non_sospette_to_file.close()

for item in email_sospette:
  email_sospette_to_file.write("%s\n" % item)

email_sospette_to_file.close()
  
for item in cf_sospetti:
  cf_sospetti_to_file.write("%s\n" % item)
  
cf_sospetti_to_file.close()  
  
for item in cf_non_sospetti:
  cf_non_sospetti_to_file.write("%s\n" % item)

cf_non_sospetti_to_file.close()

session.close()
