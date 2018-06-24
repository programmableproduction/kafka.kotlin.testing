package com.programmableproduction.kafka.kotlin.testing

import arrow.core.None
import arrow.core.Option
import arrow.core.Some
import arrow.core.none
import io.confluent.kafka.schemaregistry.avro.AvroCompatibilityLevel
import io.confluent.kafka.schemaregistry.rest.SchemaRegistryConfig
import kafka.admin.AdminUtils
import kafka.admin.RackAwareMode
import kafka.server.KafkaConfig
import kafka.server.KafkaServer
import kafka.utils.CoreUtils
import kafka.utils.TestUtils
import kafka.utils.TestUtils.getBrokerListStrFromServers
import kafka.utils.ZkUtils
import kafka.zk.EmbeddedZookeeper
import org.apache.kafka.common.security.auth.SecurityProtocol
import org.apache.kafka.common.utils.SystemTime
import scala.collection.JavaConversions
import scala.collection.JavaConverters

import java.io.File
import java.util.*


val DEFAULT_NUM_BROKERS: Int = 1
val KAFKASTORE_TOPIC: String = SchemaRegistryConfig.DEFAULT_KAFKASTORE_TOPIC
val EMPTY_SASL_PROPERTIES: Option<Properties> = None
//System.setProperty("http.nonProxyHosts", "localhost|0.0.0.0|127.0.0.1")

// a larger connection timeout is required for SASL tests
val ZKConnectionTimeout: Int = 30000

// SASL connections tend to take longer.
val ZKSessionTimeout:Int = 6000

class KCluster(val brokersNumber: Int = DEFAULT_NUM_BROKERS,
               val schemaRegistryEnabled: Boolean = true,
               val avroCompatibilityLevel: AvroCompatibilityLevel = AvroCompatibilityLevel.NONE) :AutoCloseable {

    private val Zookeeper = EmbeddedZookeeper()
    val ZookeeperConnection = "localhost:${Zookeeper.port()}"

    lateinit var Connect: EmbeddedConnect
    var kafkaConnectEnabled: Boolean = false

    private val getSecurityProtocol = SecurityProtocol.PLAINTEXT


    val BrokersConfig= generateSequence(0, {it})
            .take(brokersNumber)
            .map { id -> getKafkaConfig(id)}
            .toList()

    val Brokers: List<KafkaServer> = BrokersConfig.map {config -> TestUtils.createServer(config , SystemTime())}
    val BrokersList: String
        get() = getBrokerListStrFromServers(JavaConverters.asScalaIteratorConverter( Brokers.iterator()).asScala().toSeq(), getSecurityProtocol)

    private val ZookeeperUtils = ZkUtils.apply(
            ZookeeperConnection,
            ZKSessionTimeout,
            ZKConnectionTimeout,
            setZkAcls())

    val SchemaRegistryService: Option<SchemaRegistryService> =
        if (schemaRegistryEnabled) {
            val schemaRegPort = PortProvider.one
            val schemaRegService = SchemaRegistryService(schemaRegPort,
            ZookeeperConnection,
            KAFKASTORE_TOPIC,
            avroCompatibilityLevel,
            true)

            Some(schemaRegService)
        } else {
            None
        }

    private fun setZkAcls():Boolean {
        return getSecurityProtocol == SecurityProtocol.SASL_PLAINTEXT ||
                getSecurityProtocol == SecurityProtocol.SASL_SSL
    }

    fun createTopic(topicName: String, partitions: Int = 1, replication: Int = 1):Unit {
        val rackAware : RackAwareMode = RackAwareMode.`Enforced$`.`MODULE$`
        AdminUtils.createTopic(ZookeeperUtils, topicName, partitions, replication, Properties(), rackAware)
    }

    fun startEmbeddedConnect(workerConfig: Properties, connectorConfigs: List<Properties>): Unit {
        kafkaConnectEnabled = true
        Connect = EmbeddedConnect(workerConfig, connectorConfigs)
        Connect.start()
    }
//
    private fun buildBrokers() {
    generateSequence(0, { it })
            .take(brokersNumber)
            .map {getKafkaConfig(it)}
            .map { Pair(it, TestUtils.createServer(it, SystemTime()))}
            .unzip()
    }

    private fun injectProperties(props: Properties): Unit {
        props.setProperty("auto.create.topics.enable", "true")
        props.setProperty("num.partitions", "1")
        /*val folder = new File("kafka.cluster")
        if (!folder.exists())
          folder.mkdir()

        val logDir = new File("kafka.cluster", UUID.randomUUID().toString)
        logDir.mkdir()

        props.setProperty("log.dir", logDir.getAbsolutePath)*/
    }

    private fun getKafkaConfig(brokerId: Int): KafkaConfig {
        val props: Properties = TestUtils.createBrokerConfig(
                brokerId,
                ZookeeperConnection,
                false,
                false,
                TestUtils.RandomPort(),
                none<SecurityProtocol>().toScaleOption(),
                none<File>().toScaleOption(),
                EMPTY_SASL_PROPERTIES.toScaleOption(),
                true,
                false,
                TestUtils.RandomPort(),
                false,
                TestUtils.RandomPort(),
                false,
                TestUtils.RandomPort(),
                none<String>().toScaleOption(),
                1,
                false)
        injectProperties(props)
        return KafkaConfig.fromProps(props)
    }


    fun <T> Option<T>.toScaleOption(): scala.Option<T> =
        when(this){
            is None -> scala.Option.apply(null)
            is Some -> scala.Some(this.t)
        }


    override fun close() {
        if (kafkaConnectEnabled) {
            Connect.stop()
        }

        SchemaRegistryService.fold({ Unit }) { it.close() }
        Brokers.forEach { server ->
            server.shutdown()
            CoreUtils.delete(server.config().logDirs())
        }

        ZookeeperUtils.close()
        Zookeeper.shutdown()
    }
}


