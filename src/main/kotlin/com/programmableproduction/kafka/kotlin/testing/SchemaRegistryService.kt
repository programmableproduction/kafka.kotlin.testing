package com.programmableproduction.kafka.kotlin.testing

import arrow.*
import arrow.core.*
import java.net.Socket
import java.net.SocketException
import java.util.Properties

import com.typesafe.scalalogging.StrictLogging
import io.confluent.kafka.schemaregistry.avro.AvroCompatibilityLevel
import io.confluent.kafka.schemaregistry.client.rest.RestService
import io.confluent.kafka.schemaregistry.rest.SchemaRegistryConfig
import io.confluent.kafka.schemaregistry.rest.SchemaRegistryRestApplication
import io.confluent.kafka.schemaregistry.storage.SchemaRegistry
import io.confluent.kafka.schemaregistry.storage.SchemaRegistryIdentity
import org.eclipse.jetty.server.Server

class SchemaRegistryService(val port: Int,
                            val zookeeperConnection: String,
                            val kafkaTopic: String,
                            val avroCompatibilityLevel: AvroCompatibilityLevel,
                            val masterEligibility: Boolean){

    val schemaRegistryProp = {
        val prop = Properties()
        prop.setProperty("port", port.toString())
        prop.setProperty(SchemaRegistryConfig.KAFKASTORE_CONNECTION_URL_CONFIG, zookeeperConnection)
        prop.put(SchemaRegistryConfig.KAFKASTORE_TOPIC_CONFIG, kafkaTopic)
        prop.put(SchemaRegistryConfig.COMPATIBILITY_CONFIG, avroCompatibilityLevel.toString())
        prop.put(SchemaRegistryConfig.MASTER_ELIGIBILITY, masterEligibility)
        prop
    }

    private val app = SchemaRegistryRestApplication(schemaRegistryProp())

    val restServer = startServer(port)

    var Endpoint: String = getEndpoint(restServer)

    val restClient: RestService = RestService(Endpoint)

    fun startServer(port: Int, retries: Int = 5): Option<Server> {
        var retry = retries > 0
        var restServer: Option<Server> = None

        if (retry) {
            if (isPortInUse(port)) {
                println("Schema Registry Port $port is already in use")
                Thread.sleep(2000)
                startServer(port, retries - 1)
            } else {
                val server: Server = app.createServer()
                server.start()
                restServer = Some(server)
            }
        }
        return restServer
    }

    fun getEndpoint(restServer: Option<Server>)=
        when(restServer) {
            is Some -> {
                val uri = restServer.t.uri.toString()
                if (uri.endsWith("/")) {
                    uri.substring(0, uri.length - 1)
                } else {
                    uri
                }
            }
            is None -> ""
        }

    private fun isPortInUse(port: Int)= try {
                Socket("127.0.0.1", port).close()
                true
            }
            catch (e: Exception){
                when(e) {
                    is SocketException -> false
                    else -> true
                }
            }


    fun close() =
        when (restServer) {
            is Some -> {
                restServer.t.stop()
                restServer.t.join()
            }

                    is None -> Unit
        }

    val isMaster: Boolean = app.schemaRegistry().isMaster

    fun setMaster(schemaRegistryIdentity: SchemaRegistryIdentity) = app.schemaRegistry().setMaster(schemaRegistryIdentity)

    val myIdentity: SchemaRegistryIdentity = app.schemaRegistry().myIdentity()

    val masterIdentity: SchemaRegistryIdentity = app.schemaRegistry().masterIdentity()

    val schemaRegistry: SchemaRegistry = app.schemaRegistry()
}
