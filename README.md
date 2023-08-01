# JPMC Internship 2023 source code
For instructions on running the source code, see below.

# Prerequisites
Before running the code, you must first have all these installed in your computer:
Apache Cassandra v3.11, 
Java Development Kit v1.8.0_251,
Apache Maven v3.9.*, 
AWS CLI

Once finished installing all, add paths to environment variables.

# Setting up workspace
1. Create a Maven project in Intellij or IDE of choice (Intellij was used in the project development).
2. Create a new Apache Maven project using command prompt (See google).
2. Import source code from github to project folder.
3. Ensure dependencies are running in pom.xml (Reload maven)
4. Replace keyspace names with your own Cassandra Keyspace name
5. Replace AWS S3 bucket name with your own bucket
6. Update config file inside of your AWS folder to hold your own access keys and region
5. Ensure you have Cassandra tables set up within a keyspace and store data (not null).

# Running program
1. Open command prompt and type "cassandra" to run cassandra. (Optional: To work with CQL commands, type cqlsh in another command prompt after cassandra has finished startup)
2. Run Testing.java

Disclaimer: If there was any error running your program, an intermediate step was missed. Please refer to AWS website or Google to resolve errors.
