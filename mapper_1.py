#!/usr/bin/env python
import sys

SEPARATORE = '\t'
dizionario = {}										#dai test su per questo job: OrderedDict piu' veloce dei dizionari classici non ordinati


def combiner(intermedio, n1, n2):	
	if dizionario.has_key(intermedio):					
		dizionario[intermedio].append([n1, int(n2)])
			
	else:
		dizionario[intermedio] = [[n1, int(n2)]]	

for row in sys.stdin:
	elements = row.split("\t")

	intermedio = elements[0]	
	n1 = elements[1]
	n2 = elements[2]

	combiner(intermedio, n1, n2)

for key in dizionario.keys():
	print "%s%s%s" % (key, SEPARATORE, len(dizionario[key]))
