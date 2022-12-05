#!/bin/bash

cd {path} || exit

export SBT_OPTS="-Xms12g -Xmx18g -XX:+UseG1GC"

sbt "runMain Main $1 $2 $3 $4 $5 $6 $7"
ret="$?"

cd - || exit

exit $ret
