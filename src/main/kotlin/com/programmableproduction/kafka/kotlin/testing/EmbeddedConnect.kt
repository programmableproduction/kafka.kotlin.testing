package com.programmableproduction.kafka.kotlin.testing

import org.apache.kafka.common.errors.TimeoutException
import org.apache.kafka.common.utils.SystemTime
import org.apache.kafka.common.utils.Time
import org.apache.kafka.common.utils.Utils
import org.apache.kafka.common.utils.Utils.propsToStringMap
import org.apache.kafka.connect.runtime.ConnectorConfig
import org.apache.kafka.connect.runtime.Herder
import org.apache.kafka.connect.runtime.Worker
import org.apache.kafka.connect.runtime.distributed.DistributedConfig
import org.apache.kafka.connect.runtime.distributed.DistributedHerder
import org.apache.kafka.connect.runtime.isolation.Plugins
import org.apache.kafka.connect.runtime.rest.entities.ConnectorInfo
import org.apache.kafka.connect.storage.*
import org.apache.kafka.connect.util.FutureCallback
import java.util.*
import java.util.concurrent.CountDownLatch
import java.util.concurrent.ExecutionException
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicBoolean

open class EmbeddedConnect(val workerConfig: Properties, val connectorConfigs: List<Properties>){
    private val REQUEST_TIMEOUT_MS:Long = 120000
    private val startLatch: CountDownLatch = CountDownLatch(1)
    private val shutdown: AtomicBoolean = AtomicBoolean(false)
    private val stopLatch: CountDownLatch = CountDownLatch(1)

    private lateinit var worker: Worker
    private lateinit var herder: DistributedHerder

    // ConnectEmbedded - throws Exception
    val time: Time = SystemTime()
    val config: DistributedConfig = DistributedConfig(propsToStringMap(workerConfig))

    val offsetBackingStore : KafkaOffsetBackingStore = get_OffsetBackingStore()
    val workerId: String = UUID.randomUUID().toString()
    val statusBackingStore: StatusBackingStore = KafkaStatusBackingStore(
            time,
            worker.internalValueConverter)

    val configBackingStore: ConfigBackingStore = KafkaConfigBackingStore(
            worker.internalValueConverter, config)


    private fun  get_OffsetBackingStore(): KafkaOffsetBackingStore {
            val obs = KafkaOffsetBackingStore()
            obs.configure(config)
            return obs
        }


    init {
        worker = Worker(
                workerId,
                time,
                Plugins(hashMapOf<String, String>()),
                config,
                offsetBackingStore)
        //advertisedUrl = "" as we don't have the rest server - hopefully this will not break anything
        herder = DistributedHerder(config, time, worker, "KafkaCluster1",statusBackingStore, configBackingStore, "")

        statusBackingStore.configure(config)
    }



    fun start() {

        try {
            println("Kafka ConnectEmbedded starting")
            Runtime.getRuntime().addShutdownHook(Thread {
                @Override
                fun run() {
                println("exiting")
                try {
                    startLatch.await()
                    this.stop()
                } catch (e: Exception){
                    when (e) {
                        is InterruptedException -> println("Interrupted in shutdown hook while waiting for Kafka Connect startup to finish");
                    }
                }
                }
            })
//            sys.ShutdownHookThread {
//                logger.info("exiting")
//                try {
//                    startLatch.await()
//                    EmbeddedConnect.this.stop()
//                } catch {
//                    case e: InterruptedException => logger.error("Interrupted in shutdown hook while waiting for Kafka Connect startup to finish");
//                }
//            }
            worker.start()
            herder.start()

//            logger.info("Kafka ConnectEmbedded started")
//
            this.connectorConfigs.forEach { connectorConfig ->
                val callback = FutureCallback<Herder.Created<ConnectorInfo>>()
                val name = connectorConfig.getProperty(ConnectorConfig.NAME_CONFIG)
                herder.putConnectorConfig(name, Utils.propsToStringMap(connectorConfig), true, callback)
                callback.get(this.REQUEST_TIMEOUT_MS, TimeUnit.MILLISECONDS)
            }
//
        } catch (e: Exception) {
            when(e) {

                is InterruptedException -> println("Starting interrupted ")
                is ExecutionException -> println("Submitting connector config failed")
                is TimeoutException -> println("Submitting connector config timed out")
            }
        } finally {
            startLatch.countDown()
        }
    }

    fun stop() {
        try {
            val wasShuttingDown = shutdown.getAndSet(true)
            if (!wasShuttingDown) {
                println("Kafka ConnectEmbedded stopping")
                herder.stop()
                worker.stop()
                println("Kafka ConnectEmbedded stopped")
            }
        } finally {
            stopLatch.countDown()
        }
    }


    fun awaitStop() {
        try {
            stopLatch.await()
        } catch (e: Exception) {
            when (e){
                is InterruptedException -> println("Interrupted waiting for Kafka Connect to shutdown")
            }

        }
    }
}