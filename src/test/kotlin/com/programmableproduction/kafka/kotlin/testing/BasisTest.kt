package com.programmableproduction.kafka.kotlin.testing

import arrow.core.Option
import arrow.core.Some
import io.confluent.kafka.serializers.KafkaAvroDeserializer
import io.confluent.kafka.serializers.KafkaAvroSerializer
import org.apache.avro.Schema
import org.apache.avro.generic.GenericData
import org.apache.kafka.clients.consumer.Consumer
import org.apache.kafka.clients.consumer.KafkaConsumer
import org.apache.kafka.clients.producer.KafkaProducer
import org.apache.kafka.clients.producer.ProducerConfig
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.common.serialization.BytesDeserializer
import org.apache.kafka.common.serialization.IntegerSerializer
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.kafka.common.serialization.StringSerializer
import org.assertj.core.api.Assertions.assertThat
import org.jetbrains.spek.api.Spek
import org.jetbrains.spek.api.dsl.given
import org.jetbrains.spek.api.dsl.it
import org.jetbrains.spek.api.dsl.on
import org.junit.jupiter.api.Assertions
import scala.collection.Seq
import java.lang.management.ManagementFactory
import java.net.Socket
import java.net.SocketException
import java.rmi.registry.LocateRegistry
import java.rmi.registry.Registry
import java.rmi.server.UnicastRemoteObject
import java.util.*
import javax.management.remote.JMXConnectorServer
import javax.management.remote.JMXConnectorServerFactory
import javax.management.remote.JMXServiceURL

val createAvroRecord = {
    val userSchema = "{\"namespace\": \"example.avro\", \"type\": \"record\", " + "\"name\": \"User\"," + "\"fields\": [{\"name\": \"name\", \"type\": \"string\"}]}"
    val parser = Schema.Parser()
    val schema = parser.parse(userSchema)
    val avroRecord = GenericData.Record(schema)
    avroRecord.put("name", "testUser")
    avroRecord
}

object KafkaEmbeddedBasicTest: Spek({
    given("KCluster") {
        val kafkaCluster = ClusterTestingCapabilities()
        on("start up and be able to handle avro records being sent ") {
            val topic = "testAvro" + System.currentTimeMillis()
            val avroRecord = createAvroRecord()
            val objects = arrayListOf<Any>(avroRecord)
            val producerProps = kafkaCluster.stringAvroProducerProps
            val producer = kafkaCluster.createProducer<String, Any>(producerProps())

            for (o in objects) {
            val message = ProducerRecord<String, Any>(topic, o)
            producer.send(message)
            }
            val consumerProps = kafkaCluster.stringAvroConsumerProps()
            val consumer = kafkaCluster.createStringAvroConsumer(consumerProps())
            val records = kafkaCluster.consumeStringAvro(consumer, topic, objects.size)
            it("should return the result of adding the first number to the second number") {
                assertThat(records).isEqualTo(objects)
            }
        }
    }
})

class ClusterTestingCapabilities {

    init {
        System.setProperty("http.nonProxyHosts", "localhost|0.0.0.0|127.0.0.1")
    }

    val SCHEMA_REGISTRY_URL = "schema.registry.url"

    lateinit var registry: Registry
    val kafkaCluster: KCluster = KCluster()

    lateinit var jmxConnectorServer: Option<JMXConnectorServer>

    fun startEmbeddedConnect(workerConfig: Properties, connectorConfigs: List<Properties>): Unit {
        kafkaCluster.startEmbeddedConnect(workerConfig, connectorConfigs)
    }

    fun isPortInUse(port: Int): Boolean =
            try {
                val socket = Socket("127.0.0.1", port)
                socket.close()
                true
            } catch (e: Exception) {
                when (e) {
                    is SocketException -> false
                    else -> true
                }
            }


    fun close(): Unit {
        println("Cleaning embedded cluster. Server = $jmxConnectorServer")
        try {
            if (jmxConnectorServer.isDefined()) {
                jmxConnectorServer.get().stop()
            }
            if (Option(registry).isDefined()) {
                registry.list().forEach { s ->
                    registry.unbind(s)
                }
                UnicastRemoteObject.unexportObject(registry, true)
            }
            kafkaCluster.close()
        } catch (e: Exception) {
            when (e) {
                is Throwable -> println("ERROR in closing Embedded Kafka cluster $e")
            }
        }
    }

    /**
     * Run this method to enable JMX statistics across all embedded apps
     *
     * @param port - The JMX port to enable RMI stats
     */
    fun loadJMXAgent(port: Int, retries: Int = 10): Unit {
        var retry = retries > 0
        if (retry) {
            if (isPortInUse(port)) {
                println("JMX Port $port already in use")
                Thread.sleep(2000)
                loadJMXAgent(port, retries - 1)
            } else {
                println("Starting JMX Port of embedded Kafka system $port")
                registry = LocateRegistry.createRegistry(port)
                val env = hashMapOf(
                        "com.sun.management.jmxremote.authenticate" to "false",
                        "com.sun.management.jmxremote.ssl" to "false")

                val jmxServiceURL = JMXServiceURL("service:jmx:rmi:///jndi/rmi://:" + port + "/jmxrmi")
                val mbeanServer = ManagementFactory.getPlatformMBeanServer()
                jmxConnectorServer = Some(JMXConnectorServerFactory.newJMXConnectorServer(jmxServiceURL, env, mbeanServer))
                jmxConnectorServer.get().start()
                retry = false
                Thread.sleep(2000)
            }
        }
        if (retries == 0) {
            println("| Could not load JMX agent")
        }
    }

    /** Helpful Producers **/
    val avroAvroProducerProps: () -> Properties = {
        val props = Properties().apply {
            put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, KafkaAvroSerializer::class.java)
            put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, KafkaAvroSerializer::class.java)
            put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaCluster.BrokersList)
            put(SCHEMA_REGISTRY_URL, kafkaCluster.SchemaRegistryService.get().Endpoint)
        }
        props
    }

    val intAvroProducerProps: () -> Properties = {
        val props = Properties().apply {
            put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, IntegerSerializer::class.java)
            put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, KafkaAvroSerializer::class.java)
            put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaCluster.BrokersList)
            put(SCHEMA_REGISTRY_URL, kafkaCluster.SchemaRegistryService.get().Endpoint)
        }
        props
    }

    fun <T> getAvroProducerProps(ser: Class<T>): () -> Properties = {
        val props = Properties().apply {
            put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, ser)
            put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, KafkaAvroSerializer::class.java)
            put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaCluster.BrokersList)
            put(SCHEMA_REGISTRY_URL, kafkaCluster.SchemaRegistryService.get().Endpoint)
        }
        props
    }

    val stringAvroProducerProps: () -> Properties = {
        val props = Properties().apply {
            put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer::class.java)
            put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, KafkaAvroSerializer::class.java)
            put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaCluster.BrokersList)
            put(SCHEMA_REGISTRY_URL, kafkaCluster.SchemaRegistryService.get().Endpoint)
        }
        props
    }


    val avroStringProducerProps: () -> Properties = {
        val props = Properties().apply {
            put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer::class.java)
            put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, KafkaAvroSerializer::class.java)
            put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaCluster.BrokersList)
            put(SCHEMA_REGISTRY_URL, kafkaCluster.SchemaRegistryService.get().Endpoint)
        }
        props
    }

    val stringstringProducerProps: () -> Properties = {
        val props = Properties().apply {
            put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer::class.java)
            put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer::class.java)
            put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaCluster.BrokersList)
            put(SCHEMA_REGISTRY_URL, kafkaCluster.SchemaRegistryService.get().Endpoint)
        }
        props
    }

    fun createStringAvroConsumer(props: Properties): KafkaConsumer<String, Any> = KafkaConsumer<String, Any>(props)


    fun <K, T> createProducer(props: Properties): KafkaProducer<K, T> = KafkaProducer<K, T>(props)

    /** Helpful Consumers **/
    fun stringAvroConsumerProps(group: String = "stringAvroGroup"): () -> Properties = {
        val props = Properties().apply {
            put("bootstrap.servers", kafkaCluster.BrokersList)
            put("group.id", group)
            put("session.timeout.ms", "6000") // default value of group.min.session.timeout.ms.
            put("heartbeat.interval.ms", "2000")
            put("enable.auto.commit", "false")
            put("auto.offset.reset", "earliest")
            put("key.deserializer", StringDeserializer::class.java)
            put("value.deserializer", KafkaAvroDeserializer::class.java)
            put(SCHEMA_REGISTRY_URL, kafkaCluster.SchemaRegistryService.get().Endpoint)
        }
        props
    }

    fun stringstringConsumerProps(group: String = "stringstringGroup"): () -> Properties = {
        val props = Properties().apply {
            put("bootstrap.servers", kafkaCluster.BrokersList)
            put("group.id", group)
            put("session.timeout.ms", "6000") // default value of group.min.session.timeout.ms.
            put("heartbeat.interval.ms", "2000")
            put("enable.auto.commit", "false")
            put("auto.offset.reset", "earliest")
            put("key.deserializer", StringDeserializer::class.java)
            put("value.deserializer", StringDeserializer::class.java)
        }
        props
    }

    fun bytesbytesConsumerProps(group: String = "bytes2bytesGroup"): () -> Properties = {
        val props = Properties().apply {
            put("bootstrap.servers", kafkaCluster.BrokersList)
            put("group.id", group)
            put("session.timeout.ms", "6000") // default value of group.min.session.timeout.ms.
            put("heartbeat.interval.ms", "2000")
            put("auto.commit.interval.ms", "1000")
            put("auto.offset.reset", "earliest")
            put("key.deserializer", BytesDeserializer::class.java)
            put("value.deserializer", BytesDeserializer::class.java)
        }
        props
    }

    //    fun createStringAvroConsumer(props: () -> Properties): KafkaConsumer<String, Any> = KafkaConsumer(props)
    fun consumeStringAvro(consumer: Consumer<String, Any>, topic: String, numMessages: Int): List<Any> {
        consumer.subscribe(Arrays.asList(topic))

        fun accum(records: List<Any>): List<Any> {
            if (records.size < numMessages) {
                val consumedRecords = consumer.poll(1000)
                return accum(records + consumedRecords.map { r -> r.value() })
            } else {
                consumer.close()
                return records
            }
        }

//    fun accum(records: List<Any>): List<Any> {
//        if (records.size < numMessages) {
//            val consumedRecords = consumer.poll(1000)
//            accum(records + consumedRecords)
//        } else {

//        }
//    }

        return accum(listOf())
    } }


//        return accum(listOf() )
//    }
//}

//    /** Consume **/
//    fun consumeStringAvro(consumer: Consumer<String, Any>, topic: String, numMessages: Int): List<Any> {

//    }
//
//        accum(Vector.empty)
//    }
//
//    fun <K, V> consumeRecords(consumer: Consumer<K, V>, topic: String): Iterator<ConsumerRecord<K, V>> = {
//        consumer.subscribe(Arrays.asList(topic))
//        val result = Iterator .continually {
//            consumer.poll(1000)
//        }.flatten
//        result
//    }
