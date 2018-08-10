#!/usr/bin/env bash

function join_by { local IFS="$1"; shift; echo "$*"; }

echo "$@"

POSITIONAL=()
while [[ $# -gt 0 ]]
do
	key="$1"

	case $key in
		  -i|--ip)
		  IPS+=("$2")
		  shift # past argument
		  shift # past value
		  ;;
		  -p|--port)
		  PORTS+=("$2")
		  shift # past argument
		  shift # past value
		  ;;
		  *)    # unknown option
		  POSITIONAL+=("$1") # save it in an array for later
		  shift # past argument
		  ;;
	esac
done

PREPARE_IPS=($(for ip in ${IPS[@]}; do echo "\"$ip\""; done))
PREPARE_ANTIDOTENODES=($(for ip in ${IPS[@]}; do echo "\"antidote@$ip\""; done))

JOIN_NODES=$(join_by "," ${PREPARE_IPS[@]})
JOIN_ANTIDOTENODES=$(join_by "," ${PREPARE_ANTIDOTENODES[@]})


echo "Writting ips to config files: ${JOIN_NODES[@]}"
sed -i "s/Nodes/[${JOIN_NODES[@]}]/g" ./config/**/*.config
sed -i "s/AntidoteNames/[${JOIN_ANTIDOTENODES[@]}]/g" ./config/**/*.config
echo "Done"

if [ "${#PORTS[@]}" -gt 0 ]; then
	JOIN_PORTS=$(join_by "," ${PORTS[@]})
	echo "Writting ports to config files: ${JOIN_PORTS[@]}"
	sed -i "s/Ports/[${JOIN_PORTS[@]}]/g" ./config/**/*.config
	echo "Done"
fi


echo "Fetching self public ip..."
MY_IP=$(curl v4.ifconfig.co)
BASHO_NODE_NAME="basho@$MY_IP"
sed -i "s/BASHOIP/$MY_IP/g" ./scripts/connect_dcs.escript
echo "Done"

echo "Writing ips to escript file"
sed -i "s/Nodes/[${JOIN_NODES[@]}]/g" ./scripts/connect_dcs.escript
echo "Done"
