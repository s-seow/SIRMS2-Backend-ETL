# SIRMS2 Backend ETL Program

This is a repository for the SIRMS2 ETL Program. This program functions as the backend and performs Extract, Transform and Load of data as the backend of the Safety Incident Reporting Management System. 

Last used in December 2024.

Last updated December 2025: Cleaned up repository and rewrote readme.

### What this program does:
1. Runs an AMQP 2.0 (Qpid JMS) consumer service that provides access to flight data. 
2. Transforms flight data (FIXM, IWXXM) in XML format to JSON for postprocessing.
3. Parses flight data (MET) in JSON for postprocessing.
4. Processes the standardized data by adding logTimestamp; a part of the object's primary key.
5. Loads the processed data into AWS DynamoDB for storage and querying.

### Structure of the program:
- Java + Spring Boot app
- Package: ``DynamoDB_ETL``
- Main class: ``DynamoDB_ETL.DynamoDbETLApplication``
- Uses:
    - AWS DynamoDB (AWS SDK v2 + Enhanced Client)
    - Solace / SWIM AMQP via Apache Qpid JMS 2.0
    - Spring Boot 3.3.2 (spring-boot-starter-web, scheduling enabled)
- Entrypoints:
 - ``DynamoDbETLApplication`` – Spring Boot main() + @EnableScheduling
 - ``DynamoDBConfig`` – configures DynamoDbClient & DynamoDbEnhancedClient
 - ``Main_QueueConsumer_Service`` – connects to Solace, consumes JMS messages, calls:
    - ``FIXM_DataLoader_Service``
    - ``IWXXM_DataLoader_Service``
    - ``METReport_DataLoader_Service``
 - ``util/*DataConverter`` – parse XML/strings and build JSON to store in DynamoDB

### Dependencies required:
- Install JDK 22
- Install Maven 3.8+
- AWS SDK: To access DynamoDB via ``aws configure``
    - ``AWS_ACCESS_KEY_ID``
    - ``AWS_SECRET_ACCESS_KEY``
- Solace: To access SWIM AMQP, recreated in ``src/main/resources/solace.properties``
    - Solace host URL (AMQP endpoint)
    - Solace username
    - Solace password

## How to run
- mvn clean package
- mvn spring-boot:run -Dspring-boot.run.main-class=DynamoDB_ETL.DynamoDbETLApplication
