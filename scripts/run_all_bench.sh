#!/usr/bin/env bash

NOW=$(date +"%I%M%S")
LOG_FILE=./$NOW.log
MSG="Resetting repo and pulling last version"
echo $MSG
git reset --hard
git pull
echo $MSG >> $LOG_FILE
MSG="Preparing ssh connections"
echo $MSG
./scripts/ssh_setup.sh $1 $2 $3 $5 $6
echo $MSG >> $LOG_FILE
echo "Done"

MSG="Writing ips to config files..."
echo $MSG
echo $MSG >> $LOG_FILE
sed -i "s/Nodes/[\"$1\"]/g" ./config/**/*.config
SED5="\"s/Nodes/[\\\"$2\\\"]/g\""
SED6="\"s/Nodes/[\\\"$3\\\"]/g\""
ssh jpdsousa@$5 "cd basho_bench && git reset --hard && git pull && make distclean all && sed -i $SED5 ./config/**/*.config"
ssh jpdsousa@$6 "cd basho_bench && git reset --hard && git pull && make distclean all && sed -i $SED6 ./config/**/*.config"
echo "Done"

BASHO_NODE_NAME="basho@$4"
sed -i "s/BASHOIP/$4/g" ./scripts/connect_dcs.escript
echo "Done"

MSG="Wrtiting ips to escript file"
echo $MSG
echo $MSG >> $LOG_FILE
sed -i "s/Nodes/[\"$1\", \"$2\", \"$3\"]/g" ./scripts/connect_dcs.escript
echo "Done"

for f in ${7:-./config/**/*.config}
do
  MSG="Starting antidote and AQL"
  echo $MSG
  echo $MSG >> $LOG_FILE
  ./scripts/ssh_antidote_start.sh $1 $2 $3
  MSG="Running $f"
  echo $MSG
  echo $MSG >> $LOG_FILE
  ssh jpdsousa@$5 "epmd -daemon && cd basho_bench && screen -S basho -d -m ./basho_bench $f -N basho@$5 -C antidote"
  ssh jpdsousa@$6 "epmd -daemon && cd basho_bench && screen -S basho -d -m ./basho_bench $f -N basho@$6 -C antidote"
  screen -S basho -d -m ./basho_bench $f -N $BASHO_NODE_NAME -C antidote
  sleep 6m
  ssh jpdsousa@$5 "pkill -f basho"
  ssh jpdsousa@$6 "pkill -f basho"
  pkill -f basho
  MSG="Benchmark complete. Stopping antidote and AQL"
  echo $MSG
  echo $MSG >> $LOG_FILE
  ./scripts/ssh_antidote_kill.sh $1 $2 $3
  echo "Done. Cooling off for next round" >> $LOG_FILE
  echo "[5] Cooling down..."
  sleep 1m
  echo "[4] Cooling down..."
  sleep 1m
  echo "[3] Cooling down..."
  sleep 1m
  echo "[2] Cooling down..."
  sleep 1m
  echo "[1] Cooling down..."
  sleep 1m
done
