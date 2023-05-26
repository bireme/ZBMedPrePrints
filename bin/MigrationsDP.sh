#!/bin/bash

cd /home/javaapps/sbt-projects/zbmed-mongodb/ || exit

SBT_HOME=/home/users/operacao/.local/share/coursier/bin

export SBT_OPTS="-Xms12g -Xmx18g -XX:+UseG1GC"

$SBT_HOME/sbt "runMain Main $1 $2 $3 $4 $5 $6 $7"
ret="$?"

cd - || exit

exit $ret