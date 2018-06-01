#!/usr/bin/env bash

echo "Killing antidote"
ssh jpdsousa@$1 "pkill -f antidote"
ssh jpdsousa@$2 "pkill -f antidote"
ssh jpdsousa@$3 "pkill -f antidote"
echo "Done"

echo "Killing aql"
ssh jpdsousa@$1 "pkill -f aql"
ssh jpdsousa@$2 "pkill -f aql"
ssh jpdsousa@$3 "pkill -f aql"
echo "Done"
