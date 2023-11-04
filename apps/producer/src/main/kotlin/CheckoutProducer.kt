import io.confluent.kafka.streams.serdes.avro.SpecificAvroSerializer
import nl.swapscaps.bonusbox.CheckoutEventV1
import nl.swapscaps.bonusbox.CheckoutEventV1Article
import org.apache.kafka.clients.admin.Admin
import org.apache.kafka.clients.admin.AdminClientConfig
import org.apache.kafka.clients.admin.KafkaAdminClient
import org.apache.kafka.clients.admin.NewTopic
import org.apache.kafka.clients.producer.KafkaProducer
import org.apache.kafka.clients.producer.ProducerConfig
import org.apache.kafka.clients.producer.ProducerRecord
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
  val topic: String = "checkout-events",
  val years: Int = 10,
  val maxEventsPerWeek: Int = 3,
  val startBonuskaartId: Long  = 0,
  val numBonuskaartIds: Long  = 1e4.toLong(),
  val numPartitions: Int = 100,
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

  val adminProps: Properties = Properties().let {
    it[AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG] = "localhost:9092"
    it
  },

  val producerProps: Properties = Properties().let {
    it[ProducerConfig.BOOTSTRAP_SERVERS_CONFIG] = "localhost:9092"
    it[ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG] = StringSerializer::class.java
    it[ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG] = SpecificAvroSerializer::class.java
    it["schema.registry.url"] = "http://localhost:8081"
    it
  },
)

class CheckoutProducer(
  val config: CheckoutProducerConfig = CheckoutProducerConfig(),
  val producer: KafkaProducer<String, CheckoutEventV1> = KafkaProducer(config.producerProps),
  val admin: Admin = Admin.create(config.adminProps),
) {
  fun start() {
    val topics = admin.listTopics().names().get()

    if (!topics.contains(config.topic)) {
      admin.createTopics(listOf(NewTopic(config.topic, config.numPartitions, -1))).all().get()
    }

    val weeks = Period.ofWeeks(config.years * 52)

    val start = LocalDate.now() - weeks

    val maxEventsPerWeek = config.maxEventsPerWeek - 1

    var currentWeek = start
    while (currentWeek < LocalDate.now()) {
      val startOfWeek = currentWeek.atStartOfDay(ZoneOffset.UTC)

      for (bonuskaartId in config.startBonuskaartId..config.startBonuskaartId + config.numBonuskaartIds) {
        val numEvents = 1 + (Math.random() * (maxEventsPerWeek)).toInt()

        for (i in 0..numEvents) {
          val randomDay = (Math.random() * 7).toLong()
          val randomHour = 8 + (Math.random() * 14).toLong()
          val timestamp = startOfWeek.plusDays(randomDay).plus(randomHour, ChronoUnit.HOURS).toEpochSecond() * 1000

          println("Sending event $i for bonuskaartId $bonuskaartId at $timestamp")

          val articles = config.articles.shuffled().take((Math.random() * 4).toInt())

          val event = CheckoutEventV1.newBuilder()
            .setTimestamp(timestamp)
            .setBonuskaartId(bonuskaartId.toString())
            .setArticles(articles.map { it.toAvro() })
            .build()


          producer.send(ProducerRecord(config.topic, bonuskaartId.toString(), event)).get()
        }
      }

      currentWeek += Period.ofWeeks(1)
    }

  }

  fun stop() {

  }
}
