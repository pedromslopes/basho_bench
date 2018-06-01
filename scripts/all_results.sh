#!/usr/bin/env bash

mkdir tests
for f in $(find ./tests -maxdepth 1 -mindepth 1 -type d)
do
  Rscript --vanilla priv/summary.r -i $f
done