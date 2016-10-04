#!/usr/bin/env bash
if [ `ls target/tweets.json` ]; then
    rm target/tweets.json
fi
touch target/tweets.json