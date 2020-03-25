#!/bin/bash -x
cd $1
echo $1 $2 $3 $4 $5
python3 -s -S entrypoint.py $2 $3 $4 $5;
