#! /bin/sh
echo ${EMBULK_CONFIG} > config.json
yq config.json -o yaml > config.yml
embulk $@