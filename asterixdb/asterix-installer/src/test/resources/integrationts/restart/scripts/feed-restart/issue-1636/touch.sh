#!/usr/bin/env bash
if [ -f target/tweets.json ]; then
    rm -f target/tweets.json
fi
touch target/tweets.json