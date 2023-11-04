import io.confluent.kafka.streams.serdes.avro.SpecificAvroSerde
import nl.swapscaps.bonusbox.CheckoutEventV1
import nl.swapscaps.bonusbox.PerWeekAggregationV1
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.serialization.Serdes
import org.apache.kafka.streams.*
import org.apache.kafka.streams.kstream.Consumed
import org.apache.kafka.streams.kstream.Materialized
import org.apache.kafka.streams.kstream.Produced
import org.apache.kafka.streams.kstream.Suppressed.BufferConfig.unbounded
import org.apache.kafka.streams.kstream.Suppressed.untilWindowCloses
import org.apache.kafka.streams.kstream.TimeWindows
import org.apache.kafka.streams.processor.TimestampExtractor
import java.lang.Exception
import java.time.Duration
import java.util.Calendar
import java.util.Properties
import java.util.concurrent.atomic.AtomicBoolean


class CheckoutEventTimestampExtractor : TimestampExtractor {
  override fun extract(record: ConsumerRecord<Any, Any>, partitionTime: Long): Long {
    val event = record.value() as CheckoutEventV1
    return event.timestamp
  }
}

data class ProcessorConfig(
  val inputTopic: String = "checkout-events",
  val outputTopic: String = "per-week-aggregation",
)

class Processor(
  val config: ProcessorConfig = ProcessorConfig(),
  val kafkaStreamsProps: Properties = Properties().let {
    it[StreamsConfig.APPLICATION_ID_CONFIG] = "bonusbox-kstreams"
    it[StreamsConfig.BOOTSTRAP_SERVERS_CONFIG] = "localhost:9092"
    it[StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG] = Serdes.String().javaClass
    it[StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG] = SpecificAvroSerde::class.java
    it["schema.registry.url"] = "http://localhost:8081"
    it[StreamsConfig.DEFAULT_TIMESTAMP_EXTRACTOR_CLASS_CONFIG] = CheckoutEventTimestampExtractor::class.java
    it
  }
) {
  val isStarted = AtomicBoolean(false)

  fun start() {
    val streams = KafkaStreams(getTopology(), kafkaStreamsProps)

    val isStreamsStarted = AtomicBoolean(false)
    streams!!.setStateListener { newState, _ ->
      if (newState == KafkaStreams.State.RUNNING) {
        isStreamsStarted.set(true)
      }
    }

    streams.start()

    val ival = 1000L
    var maxWait = 120000L
    while (!isStreamsStarted.get()) {
      Thread.sleep(ival)
      maxWait -= ival
      if (maxWait <= 0) {
        throw Exception("Streams failed to start!")
      }
      println("Not started yet...")
    }

    isStarted.set(true)

  }

  fun getTopology(): Topology {
    val checkoutEventV1Serde = SpecificAvroSerde<CheckoutEventV1>()
    val perWeekAggregationV1Serde = SpecificAvroSerde<PerWeekAggregationV1>()

    val streamsBuilder = StreamsBuilder()

    streamsBuilder
      .stream(
        config.inputTopic,
        Consumed
          .with(Serdes.String(), checkoutEventV1Serde)
          .withTimestampExtractor(CheckoutEventTimestampExtractor())
      )
      .groupByKey()
      .windowedBy(TimeWindows.ofSizeAndGrace(Duration.ofDays(7), Duration.ofDays(1)))
      .aggregate(
        {
          PerWeekAggregationV1
            .newBuilder()
            .build()
        },
        { key: String, value: CheckoutEventV1, aggregate: PerWeekAggregationV1 ->
          val calendar = Calendar.getInstance()
          calendar.setTimeInMillis(value.timestamp)

          val articleCount = aggregate.articleCount.toMutableMap()

          for (article in value.articles) {
            val count = articleCount.getOrDefault(article.name, 0)
            articleCount[article.name] = count + 1
          }

          PerWeekAggregationV1.newBuilder()
            .setBonuskaartId(value.bonuskaartId)
            .setYear(calendar.get(Calendar.YEAR))
            .setWeekNr(calendar.get(Calendar.WEEK_OF_YEAR))
            .setArticleCount(articleCount)
            .build()

        },
        Materialized.with(Serdes.String(), perWeekAggregationV1Serde)
      )
      .suppress(untilWindowCloses(unbounded()))
      .toStream()
      .map { k, v -> KeyValue(v.bonuskaartId.toString(), v) }
      .peek { key, value ->
        println("key: $key, value: $value")
      }
      .to(config.outputTopic, Produced.with(Serdes.String(), perWeekAggregationV1Serde))

    return streamsBuilder.build()
  }
}
