![lana_logo](img/logo_lana.png) 
# Code Challenge for Data Engineers
One possible solution for the challenge explained in https://github.com/cabify/dataeng_challenge  
This development was written entirely in Java, using the following set of tools/platforms:
- OpenJDK 12
- Maven 3.6.3
- Apache Beam 2.16.0
- Apache Spark 2.4.4  

The input set of data was uploaded to S3, in a dedicated bucket created only for this exercise (lana-code-challenge)

## Requirements
- Apache Spark 2.4.4
- OpenJDK 12

## Proposed architecture
In order to give solution to the need of having an architecture to compute 

## Installation
1. Clone the repository in your local machine
    ```sh
    $ git clone https://github.com/maunrod/lana-code-challenge.git
    ``` 
2. Go to the root folder of the repository and build the package (jar file will be created in the target folder)
    ```sh
    $ cd LanaCodeChallenge
    $ ./mvnw clean package
    ``` 
3. Two JAR files will be generated in the target folder of the project. Use the file called _LanaCodeChallenge-1.0-SNAPSHOT-shaded.jar_ (contains all dependencies required)

## Environment Variables
In order to make the executions more flexible and customizable, some environment variables were added.

| Variable | Description | Value | Mandatory | Default |
| -------- | ----------- | ----- | --------- | ------- |
| MAX_OUTPUT_LINES | Max Number of Results to show/write |  | NO | 5 |
| AWS_ACCESS_KEY | AWS Access Key | AKIAQPRE4WXIZHFDQGNY | YES | |
| AWS_SECRET_ACCESS_KEY | AWS Access Key | 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 | YES | |
| AWS_DEFAULT_REGION | AWS Default Region Name |  | NO | us-east-2 |
| S3_SEARCH_PATTERN | S3 prefix where the input data is (glob wildcards allowed) |  | NO | s3://lana-code-challenge/in/ShakespearePlaysPlus/\*\*/\*\_characters/\* |
| S3_TMP_PREFIX | S3 prefix to save temporal files |  | NO |s3://lana-code-challenge/tmp/ |

## Assumptions


## Execution
First of all, set/unset environment variables to customize the executions. Remember to set those mandatory variables using the value provided in the column named _Value_!
```sh
export AWS_ACCESS_KEY=AKIAQPRE4WXIZHFDQGNY
export AWS_SECRET_ACCESS_KEY=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
```

#### N Most common words
This job calculates the N most common words

#### N Longest words

#### N Longest sentences

