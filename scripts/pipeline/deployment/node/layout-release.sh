#!/bin/sh
# Organize an extracted Git release without changing worker import paths.
set -eu
cd "$1"
mkdir engine events-extraction state-replication revert-extraction hf-computation
mv scripts contracts engine/
cp engine/scripts/pipeline/deployment/node/run-research.py main.py
cp engine/scripts/pipeline/deployment/node/DSC-SCRIPTS.md README.md

ln -s ../engine/scripts/event-cryo-extraction events-extraction/extraction
ln -s ../engine/scripts/decoding events-extraction/decoding
ln -s ../engine/scripts/protocols events-extraction/protocols
ln -s ../engine/scripts/registry events-extraction/registry
ln -s ../engine/scripts/reconstruction state-replication/reconstruction
ln -s ../engine/scripts/automation state-replication/workers
ln -s ../engine/scripts/reverted revert-extraction/workers
ln -s ../engine/scripts/user-history hf-computation/workers
