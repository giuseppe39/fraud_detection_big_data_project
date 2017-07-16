#!/usr/bin/env python
import sys

SEPARATORE = '\t'

for row in sys.stdin:
	elements = row.split("\t")

	identificativo = elements[0]
	score = int(elements[1])

	print "%s%s%s" % (score, SEPARATORE, identificativo)
