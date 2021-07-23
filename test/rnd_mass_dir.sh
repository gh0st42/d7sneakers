#!/bin/sh

if [ $# -ne 2 ]
then 
    echo "$0: <output-dir> <num bundles>"
fi

mkdir -p $1
for i in $(seq 1 $2); do
    echo "Creating bundle $i"
    TEMPNAME=$(mktemp -p $1 -t XXXXXXXXXX.bundle)
    bp7 rnd -r > $TEMPNAME
done