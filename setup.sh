#!/bin/bash
set -e

mkdir -p /data/bronze
mkdir -p /data/silver
mkdir -p /data/gold

chown -R 1000:1000 /data/bronze
chown -R 1000:1000 /data/silver
chown -R 1000:1000 /data/gold