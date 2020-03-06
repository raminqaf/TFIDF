# TFIDF

This repository of the TFIDF coding challenge for [Bakdata](https://github.com/bakdata)

## Getting Started

These instructions will get you a copy of the project up and running on your local machine for development and testing purposes.

### Prerequisites

1. Install ZooKeeper. You can find a guide [here](https://www.tutorialspoint.com/zookeeper/zookeeper_installation.htm).
2. Install Kafka. You can find a guide [here](https://kafka.apache.org/quickstart).<br/>

* Here also some installation examples for Mac OSX users:

    ```console
    $ brew cask install java
    $ brew install zookeeper
    $ brew install kafka
    ```

### Installing and running the program

1. Create a directory called `data` in the root of the project.

    ```
    $ mkdir data
    ```

2. Download and unzip the [vep_big_names_of_science_v2_txt](http://vep.cs.wisc.edu/VEPCorporaRelease/zips/vep_big_names_of_science_v2_txt.zip)

    ```
    $ curl -O http://vep.cs.wisc.edu/VEPCorporaRelease/zips/vep_big_names_of_science_v2_txt.zip
    $ unzip vep_big_names_of_science_v2_txt.zip -d vep_big_names_of_science_v2_txt
    ```
   
3. Move the `vep_big_names_of_science_v2_txt` folder inside the `data` directory that you created in step 1.

4. Open the `scripts` folder and run the `start-zookeeper.sh`. This script will start ZooKeeper.

5. In the same folder, run the `start-kafka-server.sh`. This script will run the Kafka server on your machine.

6. Now run the `org.bakdata.kafka.challenge.consumer.TFIDFConsumer`. By default, the consumer is Running for one hour. You can pass the timeout as seconds in the program arguments.

7. Run `org.bakdata.kafka.challenge.TFIDFApplication`. This class will run the will create the input/output topics and run the producer (You can find the producer in `org.bakdata.kafka.challenge.producer.TFIDFProducer`).
The producer will read each file name from the metadata CSV file, `VEP_Big_Names_of_Science_Metadata.csv`, and send the file name and file content to the stream processor for calculating the TFIDF.

8. After all the data is sent, and the stream processor finished processing the data, you can find a report `output.csv` inside the `data` directory.
Besides that, you can see all the activities from the console.

## Example output
* This is what the `producer` logs on the terminal when it sends the documents:
    ```
    [20-Mar-06 11:49:55:601] [INFO] [TFIDFProducer:83] - Sent: A00429.headed.txt
    [20-Mar-06 11:49:55:618] [INFO] [TFIDFProducer:83] - Sent: A01014.headed.txt
    [20-Mar-06 11:49:55:632] [INFO] [TFIDFProducer:83] - Sent: A01089.headed.txt
    [20-Mar-06 11:49:55:643] [INFO] [TFIDFProducer:83] - Sent: A01185.headed.txt
    ...
    [20-Mar-06 11:51:32:389] [INFO] [TFIDFProducer:83] - Sent: K084724.000.txt
    [20-Mar-06 11:51:32:394] [INFO] [TFIDFProducer:83] - Sent: K088587.000.txt
    ```
* This is the output of the `consumer`:
    ```
    ...
    [20-Mar-06 11:54:56:416] [INFO] [ConsumerTask:78] - Consumer Record:(really, TFIDFResult{documentName='K088587.000.txt', tfidf=2.292379255671519E-5, overallDocumentCount=252.0})
    [20-Mar-06 11:54:56:416] [INFO] [ConsumerTask:78] - Consumer Record:(westward, TFIDFResult{documentName='K088587.000.txt', tfidf=7.749641050412507E-5, overallDocumentCount=252.0})
    [20-Mar-06 11:54:56:416] [INFO] [ConsumerTask:78] - Consumer Record:(round, TFIDFResult{documentName='K088587.000.txt', tfidf=7.2739186459018475E-6, overallDocumentCount=252.0})
    [20-Mar-06 11:54:56:416] [INFO] [ConsumerTask:78] - Consumer Record:(corresponding, TFIDFResult{documentName='K088587.000.txt', tfidf=1.8929645955659432E-4, overallDocumentCount=252.0})
    [20-Mar-06 11:54:56:416] [INFO] [ConsumerTask:78] - Consumer Record:(1201, TFIDFResult{documentName='K088587.000.txt', tfidf=2.2650448413332806E-4, overallDocumentCount=252.0})
    [20-Mar-06 11:54:56:416] [INFO] [ConsumerTask:78] - Consumer Record:(variable, TFIDFResult{documentName='K088587.000.txt', tfidf=5.947448869452933E-5, overallDocumentCount=252.0})
    [20-Mar-06 11:54:56:416] [INFO] [ConsumerTask:78] - Consumer Record:(intend, TFIDFResult{documentName='K088587.000.txt', tfidf=2.1801436086265487E-5, overallDocumentCount=252.0})
    [20-Mar-06 11:54:56:416] [INFO] [ConsumerTask:78] - Consumer Record:(excentricity, TFIDFResult{documentName='K088587.000.txt', tfidf=1.531078372380589E-4, overallDocumentCount=252.0})
    ```

## Code style

IntelliJ IDEA [code style](https://github.com/bakdata/bakdata-code-styles) settings for Bakdata

## Built With

* [Java 8](https://www.oracle.com/technetwork/java/javase/overview/java8-2100321.html) - Java version used
* [Maven](https://maven.apache.org/) - Dependency Management
* [Kafka Streams 2.4.0](https://www.apache.org/dist/kafka/2.4.0/RELEASE_NOTES.html) - Used for stream processing
* [kafka-s3-backed-serde](https://github.com/bakdata/kafka-s3-backed-serde) - A Kafka Serde that reads and writes records from and to S3 transparently

## Authors

* **Ramin Gharib** - *Initial work* - [raminqaf](https://github.com/raminqaf)
