#!/bin/bash
/usr/bin/env python3 -m venv $1;
cd $1;
source bin/activate;
bin/pip3  install pyro4;
unzip -d lib/python*/ $2;
