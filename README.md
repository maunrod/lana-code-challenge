![lana_logo](img/logo_lana.png) 
# Code Challenge for Data Engineers
This project is the solution (only one of the possible solutions!) for the challenge explained in https://github.com/cabify/dataeng_challenge  
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
In order to give solution to the need of having an architecture to compute statistics from files, in a distributed way, the following architecture is proposed, using Amazon services:
![architecture_aws](img/aws_architecture.png) 

* Input data is stored in S3
* Job are defined using Apache Beam. Apache Spark is used for computing in a distributed way (EMR is the Amazon service that support this step)
* Result of processing is saved as parquet in S3. A semantic layer is created by using the Glue service.
* In case we need to add to this architecture the ability to process data coming from Streaming, we can do it by adding 2 components (from the AWS tools ecosystem):
    * Input data: AWS Kinesis Streams (streams with data to be processed)
    * Output data: data generated by the job could be saved in S3 directly; or, to another stream and then move the data to S3 with Kinesis Firehose. In both cases, data will be stored in parquet format and after that, mapped to the Glue catalog.
    * From Apache Beam side, we must to include the library KinesisIO, to Read/Write data from/to Kinesis Stream.

## Installation
1. Clone the repository in your local machine
    ```sh
    $ git clone https://github.com/maunrod/lana-code-challenge.git
    ``` 
2. Go to the root folder of the repository and build the package (JARs files will be created in the target folder)
    ```sh
    $ cd LanaCodeChallenge
    $ ./mvnw clean package
    ``` 
3. Two JAR files will be generated in the target folder of the project. Use the file called _LanaCodeChallenge-1.0-shaded.jar_ (contains all dependencies required)

## Environment Variables
In order to make the executions more flexible and customizable, some environment variables were added.

| Variable | Description | Mandatory | Default |
| -------- | ----------- | --------- | ------- |
| MAX_OUTPUT_LINES | Max Number of Results to show/write | NO | 5 |
| AWS_ACCESS_KEY | AWS Access Key | YES | - |
| AWS_SECRET_ACCESS_KEY | AWS Access Key | YES | - |
| AWS_DEFAULT_REGION | AWS Default Region Name | NO | us-east-2 |
| S3_SEARCH_PATTERN | S3 prefix where the input data is (glob wildcards allowed) | NO | s3://lana-code-challenge/in/ShakespearePlaysPlus/\*\*/\*\_characters/\* |
| S3_TMP_PREFIX | S3 prefix to save temporal files | NO |s3://lana-code-challenge/tmp/ |
| S3_OUT_PREFIX | S3 prefix to write final files | NO |s3://lana-code-challenge/out/ |


## Execution
First of all, set/unset environment variables to customize the executions. Remember to set the mandatory variables to avoid errors!
```sh
export AWS_ACCESS_KEY=<YOUR-ACCESS-KEY>
export AWS_SECRET_ACCESS_KEY=<YOUR-SECRET-ACCESS-KEY>
```

#### N Most common words
This job calculates the N most common words found in the files provided. To change the N value, setup the environment variable MAX_OUTPUT_LINES.

###### Execution
```sh
spark-submit --master local --class com.lana.challenge.pipeline.MostCommonWords target/LanaCodeChallenge-1.0-shaded.jar
```

###### Output

| word | occurrences | 
| ---- | ----------- | 
|the   | 26591       |
|and   | 23925       |
|i     | 22329       |
|to    | 19074       |
|of    | 15888       |

> Results are written on S3 as CSV (pipe delimited), in the path S3_OUT_PREFIX (or its default value) under the folder MostCommonWordsJob

###### Assumptions
* Regular expression used to define what is considered a word: [^\\p{L}]+
* Contractions are splitted and considered as different words. Example: touch'd -> [touch,d]
* In order to process ONLY dialogs between characters, only files within folders with suffix _characters were considered.

#### N Longest words
This job calculates the N longest words found in the files provided. To change the N value, setup the environment variable MAX_OUTPUT_LINES.

###### Execution
```sh
spark-submit --master local --class com.lana.challenge.pipeline.LongestWords target/LanaCodeChallenge-1.0-shaded.jar
```

###### Output
| word | length | 
| ---- | ------ | 
|honorificabilitudinitatibus|27|
|undistinguishable|17|
|indistinguishable|17|
|anthropophaginian|17|
|superserviceable|16|

> Results are written on S3 as CSV (pipe delimited), in the path S3_OUT_PREFIX (or its default value) under the folder LongestWordsJob

###### Assumptions
* Regular expression used to define what is considered a word: [^\\p{L}]+
* Contractions are splitted and considered as different words. Example: i'm -> [i,m]
* In order to process ONLY dialogs between characters, only files within folders with suffix _characters were considered.


#### N Longest sentences
This job calculates the N longest sentences found in the files provided. To change the N value, setup the environment variable MAX_OUTPUT_LINES.

###### Execution
```sh
spark-submit --master local --class com.lana.challenge.pipeline.LongestWords target/LanaCodeChallenge-1.0-shaded.jar
```

###### Output
| sentence | length | 
| ---- | ------ | 
|Why, Petruchio is coming, in a new hat and an old jerkin|56|
|This royal throne of kings, this scepter'd isle|47|
|John, to stop Arthur's title in the whole|41|
|Therefore the winds, piping to us in vain|41|
|To this we swore our aid|24|

> Results are written on S3 as CSV (pipe delimited), in the path S3_OUT_PREFIX (or its default value) under the folder LongestSentencesJob

###### Assumptions
* Delimiters used to define a a sentence are ".|!|?"
* All empty spaces are removed both left and right.
* Multiples spaces in the middle of the sentences are replaced by a single space.
* In order to process ONLY dialogs between characters, only files within folders with suffix _characters were considered.
