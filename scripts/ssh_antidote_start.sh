#!/usr/bin/env bash

echo "Starting antidote instances"
ssh jpdsousa@$1 "epmd -daemon && screen -S antidote -d -m ./init_antidote.sh"
ssh jpdsousa@$2 "epmd -daemon && screen -S antidote -d -m ./init_antidote.sh"
ssh jpdsousa@$3 "epmd -daemon && screen -S antidote -d -m ./init_antidote.sh"
echo "Done"

sleep 2m
echo "Connecting data centers"
./scripts/connect_dcs.escript
echo "Done"

echo "Starting AQL instances"
ssh jpdsousa@$1 "screen -S aql -d -m ./init_aql.sh"
ssh jpdsousa@$2 "screen -S aql -d -m ./init_aql.sh"
ssh jpdsousa@$3 "screen -S aql -d -m ./init_aql.sh"
echo "Done"

echo "Starting AQL dummy instances"
ssh jpdsousa@$1 "screen -S dummy -d -m ./init_aql_dummy.sh"
ssh jpdsousa@$2 "screen -S dummy -d -m ./init_aql_dummy.sh"
ssh jpdsousa@$3 "screen -S dummy -d -m ./init_aql_dummy.sh"
echo "Done"

echo "Starting AQL dummy_cascade instances"
ssh jpdsousa@$1 "screen -S cascade -d -m ./init_aql_no_cascade.sh"
ssh jpdsousa@$2 "screen -S cascade -d -m ./init_aql_no_cascade.sh"
ssh jpdsousa@$3 "screen -S cascade -d -m ./init_aql_no_cascade.sh"
echo "Done"
