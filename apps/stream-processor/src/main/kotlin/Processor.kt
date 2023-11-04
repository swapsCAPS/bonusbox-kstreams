import io.confluent.kafka.streams.serdes.avro.SpecificAvroSerde
import nl.swapscaps.bonusbox.CheckoutEventV1
import nl.swapscaps.bonusbox.PerWeekAggregationV1
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.serialization.Serdes
import org.apache.kafka.streams.KafkaStreams
import org.apache.kafka.streams.StreamsBuilder
import org.apache.kafka.streams.StreamsConfig
import org.apache.kafka.streams.Topology
import org.apache.kafka.streams.kstream.Consumed
import org.apache.kafka.streams.kstream.Produced
import org.apache.kafka.streams.processor.TimestampExtractor
import java.util.Properties


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
    it[StreamsConfig.BOOTSTRAP_SERVERS_CONFIG] = "localhost:9092"
    it[StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG] = Serdes.String().javaClass
    it[StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG] = SpecificAvroSerde::class.java
    it["schema.registry.url"] = "http://localhost:8081"
    it[StreamsConfig.DEFAULT_TIMESTAMP_EXTRACTOR_CLASS_CONFIG] = CheckoutEventTimestampExtractor::class.java
    it
  }
) {
  fun start() {
    val streams = KafkaStreams(getTopology(), kafkaStreamsProps)

    streams.start()
  }

  fun getTopology(): Topology {
    val checkoutEventV1Serde = SpecificAvroSerde<CheckoutEventV1>()
    val perWeekAggregationV1Serde = SpecificAvroSerde<PerWeekAggregationV1>()

    val streams =
      StreamsBuilder()
        .stream(config.inputTopic, Consumed.with(Serdes.String(), checkoutEventV1Serde))
        // TODO windowing
        .to(config.outputTopic, Produced.with(Serdes.String(), perWeekAggregationV1Serde))

    streams.build()
  }
}
