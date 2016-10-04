#!/usr/bin/env bash
if grep --quiet -m 1 'java.lang.ArrayIndexOutOfBoundsException' target/asterix-installer-*-binary-assembly/clusters/local/working_dir/logs/*.log ; then
    echo "ERROR"
fi
