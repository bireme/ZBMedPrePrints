# ZBMedPrePrints-processing

This project retrieves documents from MongoDB and converts them into XML format with UTF-8 encoding, specifically tailored for ZBMedPrePrints.

### App versions
* Scala 2.13.10
* JDK 19
* MongoDB 5.0.13

### Installing

1. Clone repository
2. Open the file .../bin/MigrationsDP.sh and set the path to the Scala folder. For example: /home/{YOUR_USER}/ZBMedPrePrints/scala. This is necessary to ensure that the project runs correctly.
3. Set your path for log file generation in ./log.properties under the 'LOG_PATH=' property.

### Execution parameters

1. Navigate to the 'bin' directory and run the 'ZBMedPre2Mongo.sh' script, providing the required parameters and, if necessary, the optional ones, example:

        ..\bin $ ./MigrationsDP.sh -xmlout=/home/myuser/Documents/ppzbmed_regional.xml -database=mydatabase -collection=mycollection

   Parameters:

        -xmlout=<path>: Path to the output XML file.
        -database=<name>: Name of the MongoDB database.
        -collection=<name>: Name of the MongoDB collection.
        [-host=<name>] (optional): MongoDB host name. The default is localhost.
        [-port=<number>] (optional): MongoDB port number. The default is 27017.
        [-user=<name>] (optional): MongoDB user name.
        [-password=<pwd>] (optional): MongoDB user password.