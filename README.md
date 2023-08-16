# ZBMedPrePrints-processing project

The project gets documents from mongodb and converts to xml utf-8, specific to ZBMedPrePrints.

## versions
* Scala 2.13.10
* JDK 19
* MongoDB 5.0.13

## Set the settings
1. In `ZBMedPrePrints/bin/MigrationsDP.sh` set the root directory of the scala folder. Ex.: `/home/{MYUSER}/ZBMedPrePrints/scala`.
2. In `ZBMedPrePrints/log.properties` define the output directory (LOG_PATH) of the log file and file name (FILENAME).
3. Run the project commands from the command line in the `bin` directory: (-xmlout=<path>, -database=<name>, -collection=<name>, [-host=<name>], [-port=<number>], [-user=<name>], [-password=<pwd>]).
Ex.: `ZBMedPrePrints/scala/bin$ ./MigrationsDP.sh -xmlout=/home/myuser/Documents/ppzbmed_regional.xml -database=mydatabase -collection=mycollection`
