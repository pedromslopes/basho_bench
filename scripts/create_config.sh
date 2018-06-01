#!/usr/bin/env bash


echo "Creating config..."
read -p "Name: " name
echo "-- Available Drivers --"
drivers=( "basho_bench_driver_antidote_aql" "basho_bench_driver_antidote" "basho_bench_driver_aql_client" )
for driver in "${drivers[@]}"
do
echo $driver
done
read -p "Driver index (start on 0): " driver_index
if [ $driver_index -eq 0 ]
then
read -p "Use dummy? [y/d/n] " dummy
else
dummy="aql"
fi
read -p "Token char: " token_char
read -p "Duration(minutes): " duration
read -p "Min Clients: " min_clients
read -p "Gap Clients: " gap_clients
read -p "Max Clients: " max_clients
echo "-- Workload configuration --"
read -p "Put: " w_put
read -p "Get: " w_get
read -p "Delete: " w_del

if [ "$dummy" == "y" ]
then
dummy="aqldummy"
elif [ "$dummy" == "d" ]
then
dummy="aqlcascade"
else
dummy="aql"
fi

mkdir ./config/$name
clients=$min_clients
while [ $clients -le $max_clients ]
do
w_str=$w_put"-"$w_get"-"$w_del
config_file=./config/$name/$token_char$clients"C"$w_str".config"
cat > $config_file <<- EOM
{mode, max}.
{duration, $duration}.
{concurrent, $clients}.

{driver, ${drivers[$driver_index]}}.

{aql_shell, "$dummy"}.

{key_generator, {pareto_int, 500000000}}.

{value_generator, {uniform_int, 3}}.

{aql_actors, Nodes}.

{operations, [{put, $w_put},{get, $w_get}, {delete, $w_del}]}.
EOM
clients=$[$clients+$gap_clients]
done
