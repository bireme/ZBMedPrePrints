#!/usr/bin/env bash

cd /home/javaapps/sbt-projects/ZBMedPre2Mongo || exit

NOW=$(date +"%Y%m%d%H%M%S")
SBT_HOME=/home/users/operacao/.local/share/coursier/bin

$SBT_HOME/sbt 'runMain zbmp2m.ZBMedPre2Mongo -database:ZBMED_PPRINT -collection:preprints_full_2 --reset -fromDate:2019-01-01 -host:172.17.1.230 --importByMonth' &> logs/ZBMedPre2Mongo$NOW.log

ret="$?"

if [ "$?" -ne 0 ]; then
	sendemail -f appofi@bireme.org -u "ZBMedPre2Mongo ERROR - $(date '+%Y%m%d') (executado em : ${HOSTNAME})" -m "Error to import preprints from ZBMED into MongoDB" -t barbieri@paho.org,ofi@paho.org -s esmeralda.bireme.br
fi  

cd - || exit

exit $ret
