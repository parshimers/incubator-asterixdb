#!/bin/bash
cd $1;
source bin/activate;
bin/python3 entrypoint.py $2 $3 $4 $5;
