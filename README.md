# flink-money-enrichment
**A streaming data enrichment application powered by Apache Flink.**

This project is an extensive POC of using Apache Flink (DataStream API) to implement a streaming
data enrichment.

## Requirements

#### Non-functional

- Scalability, low latency and high throughput. Provided by Flink out of the box. 
- High availability and fault tolerance. Provided by Flink and RocksDB as state backend.
- Exactly once end-to-end processing. Achieved by using Flink Kafka Source/Sink connectors.
- Application evolution without reprocessing all data from the beginning. Achieved by using AVRO as data type for Flink state.
- Easy to test. Provided so-called Flink harnesses and MiniCluster .
- Observability of Flink internal state. Achieved by using Flink QueryableStateClient. 
- Bootstrapping state based on historical data. Provided by Flink State Processor API.

<br />

#### Functional

- It should enrich a stream of transactions events (money) coming from a Kafka topic with data from two other Kafka topics (session event stream + user event stream) 
- It should process events out of order.
- It should deduplicate money events.
- It should detect first occurrence of money events per user.
- It should process each event exactly once. 
- It should clean session info state after session expiration.  
- It should produce same results in real time as in backfill scenarios. (See Inspirations section)
- It should enrich late money events with data from an external service without blocking. (Flink AsyncFunction)
- It should use Avro as a serialization system and Scala case classes representing the domain. (No Java generated POJOS) (See Inspirations section)

<br />

#### BTW, Apache Flink:

- It's Battle tested. (https://flink.apache.org/poweredby.html)
- It's good supported. Flink's community is huge and very active. Moreover, Ververica offers great courses and production supporting.
- It offers a Scala API.


## Usage

**To run the tests**
```
cd .../money-enrichment/
sbt test
```

**To run and test the application locally**

We have 3 options:

- SBT. Run `sbt run` from the application root folder.
- IDE. Run org.kindofdev.MoneyEnrichmentApp class. 
- Flink cluster. 
  * Package the application into a fat jar with `sbt clean assembly` from the application root folder and then ...
  * Submit it to Flink with `./bin/flink run .../money-enrichment/target/scala-2.12/money-enrichment-assembly-0.1-SNAPSHOT.jar`
    
    
Note that before bringing up the application, we need Kafka and Zookeeper running
```
cd .../money-enrichment/docker
docker-compose up
```

**To query internal state**

We can use `org.kindofdev.query.QueryClient`. There's a Scala worksheet with an example called `query_state_worksheet.sc`

## To-do list

- Create performance tests + Tune parallelism and maxParallelism.
- Create automated tests for state migration/evolution. (It was tested manually, and it worked properly).
- Play with Flink State Processor API. 
- Find out why the test "not deduplicate events if ttl finished" fails. (It works correctly when it is tested manually) (this test is ignored currently).
- Find out why the test "fail due to a timeout in async session enrichment" fails. (this test is ignored currently).



## Inspirations

- The approach followed to achieve application evolution using Scala case classes and AVRO is based on ING WBAA team approach shown in the following article.
  https://medium.com/wbaa/making-sense-of-apache-flink-state-migration-with-scala-and-avro-69091c232646
  
- Determinism in backfill scenarios vs real time is based on Bird Engineering's article
https://medium.com/bird-engineering/replayable-process-functions-in-flink-time-ordering-and-timers-28007a0210e1

## License
[MIT](https://choosealicense.com/licenses/mit/)  

 <br />

<!-- Example #2 - inline-styled ❤ -->
Made with <span style="color: #e25555;">&#9829;</span> in Malaga by Jose Velasco# flink-money-enrichment
