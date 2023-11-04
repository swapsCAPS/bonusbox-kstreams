import io.confluent.kafka.streams.serdes.avro.SpecificAvroSerializer
import nl.swapscaps.bonusbox.CheckoutEventV1
import nl.swapscaps.bonusbox.CheckoutEventV1Article
import org.apache.kafka.clients.producer.KafkaProducer
import org.apache.kafka.clients.producer.ProducerConfig
import org.apache.kafka.common.serialization.StringSerializer
import java.util.Properties
import java.time.LocalDate
import java.time.Period
import java.time.ZoneOffset
import java.time.temporal.ChronoUnit

data class Article(
  val name: String,
  val price: Float,
) {
  fun toAvro(): CheckoutEventV1Article {
    return CheckoutEventV1Article.newBuilder()
      .setName(this.name)
      .setPrice(this.price)
      .build()
  }
}

data class CheckoutProducerConfig(
  val years: Int = 10,
  val maxEventsPerWeek: Int = 3,
  val numBonuskaartIds: Long  = 1e2.toLong(),
  val articles: List<Article> = listOf(
    Article("Apple", 1.0f),
    Article("Banana", 1.0f),
    Article("Bread", 1.0f),
    Article("Coffee", 1.0f),
    Article("Flour", 1.0f),
    Article("KaaS", 2.0f),
    Article("Orange", 1.0f),
    Article("Sugar", 1.0f),
    Article("Tea", 1.0f),
  ),

  val producerProps: Properties = Properties().let {
    it[ProducerConfig.BOOTSTRAP_SERVERS_CONFIG] = "localhost:9092"
    it[ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG] = StringSerializer::class.java
    it[ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG] = SpecificAvroSerializer::class.java
    it["schema.registry.url"] = "http://localhost:8085"
    it
  },
)

class CheckoutProducer(
  val config: CheckoutProducerConfig = CheckoutProducerConfig(),
  val producer: KafkaProducer<String, String>? = KafkaProducer(config?.producerProps)
) {
  fun start() {
    val weeks = Period.ofWeeks(config.years * 52)

    val start = LocalDate.now() - weeks

    val maxEventsPerWeek = config.maxEventsPerWeek - 1

    var currentWeek = start
    while (currentWeek < LocalDate.now()) {
      val from: Long = config.numBonuskaartIds
      val to: Long  = (config.numBonuskaartIds * 2)

      val startOfWeek = currentWeek.atStartOfDay(ZoneOffset.UTC)

      for (bonuskaartId in from..to) {
        val numEvents = 1 + (Math.random() * (maxEventsPerWeek)).toInt()

        for (i in 0..numEvents) {
          val randomDay = (Math.random() * 7).toLong()
          val randomHour = 8 + (Math.random() * 14).toLong()
          val timestamp = startOfWeek.plusDays(randomDay).plus(randomHour, ChronoUnit.HOURS).toEpochSecond() * 1000

          println("Sending event $i for bonuskaartId $bonuskaartId at $timestamp")

          val articles = config.articles.shuffled().take((Math.random() * config.articles.size).toInt())

          val event = CheckoutEventV1.newBuilder()
            .setBonuskaartId(bonuskaartId.toString())
            .setArticles(articles.map { it.toAvro() })
            .build()
        }
      }

      currentWeek += Period.ofWeeks(1)
    }

  }

  fun stop() {

  }
}
