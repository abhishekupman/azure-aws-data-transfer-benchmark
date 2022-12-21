# azure-aws-data-transfer-benchmark
****

## Maven Build: 
mvn clean install

## Create Single Jar with All dependencies
mvn package

## Command to Run Jar
java -jar  <jar-path> -b "<blob-url>" -s "<s3-url>" -c <chunk-size-in-mb>