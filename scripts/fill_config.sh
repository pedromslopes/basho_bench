#!/usr/bin/env bash

echo "Writting ips to config files..."
sed -i "s/Nodes/[\"$1\", \"$2\", \"$3\"]/g" ./config/**/*.config
sed -i "s/AntidoteNames/[\"antidote@$1\", \"antidote@$2\", \"antidote@$3\"]/g" ./config/**/*.config
echo "Done"

echo "Fetching self public ip..."
MY_IP=$(curl v4.ifconfig.co)
BASHO_NODE_NAME="basho@$MY_IP"
sed -i "s/BASHOIP/$MY_IP/g" ./scripts/connect_dcs.escript
echo "Done"

echo "Writing ips to escript file"
sed -i "s/Nodes/[\"$1\", \"$2\", \"$3\"]/g" ./scripts/connect_dcs.escript
echo "Done"
