#!/usr/bin/env python
import sys

SEPARATORE = '\t'
dizionario = {}

def unisci(intermedio, lunghezza):								
	if dizionario.has_key(intermedio):					
		dizionario[intermedio] += lunghezza

	else:
		dizionario[intermedio] = lunghezza	

for row in sys.stdin:
	intermedio, lunghezza = row.split("\t", 1)
	lunghezza = int(lunghezza)
	unisci(intermedio, lunghezza)
	
for key in dizionario:
	print '%s%s%s' % (key, SEPARATORE, dizionario[key])