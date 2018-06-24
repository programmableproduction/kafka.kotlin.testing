package com.programmableproduction.kafka.kotlin.testing

import org.apache.kafka.connect.runtime.distributed.DistributedConfig
import org.apache.kafka.connect.runtime.ConnectorConfig
import org.apache.kafka.connect.runtime.WorkerConfig
import java.util.*

fun WorkerConfig(bootstapServers: String, schemaRegistryUrl: String) = hashMapOf<String, Any>(
        DistributedConfig.GROUP_ID_CONFIG to  "testing-group-id",
        WorkerConfig.BOOTSTRAP_SERVERS_CONFIG to bootstapServers,
        WorkerConfig.KEY_CONVERTER_CLASS_CONFIG to "org.apache.kafka.connect.json.JsonConverter",
        WorkerConfig.VALUE_CONVERTER_CLASS_CONFIG to "org.apache.kafka.connect.json.JsonConverter",
        WorkerConfig.VALUE_CONVERTER_CLASS_CONFIG to "com.qubole.streamx.ByteArrayConverter",

        DistributedConfig.OFFSET_STORAGE_TOPIC_CONFIG to "connect-offsets",
        DistributedConfig.CONFIG_TOPIC_CONFIG to "connect-configs",
        DistributedConfig.STATUS_STORAGE_TOPIC_CONFIG to "connect-status",
        WorkerConfig.INTERNAL_KEY_CONVERTER_CLASS_CONFIG to "org.apache.kafka.connect.json.JsonConverter",
        WorkerConfig.INTERNAL_VALUE_CONVERTER_CLASS_CONFIG to "org.apache.kafka.connect.json.JsonConverter",
        "schema.registry.url" to schemaRegistryUrl)

val SourceConfig = hashMapOf(
    ConnectorConfig.NAME_CONFIG to "file-source-connector",
    ConnectorConfig.CONNECTOR_CLASS_CONFIG to "org.apache.kafka.connect.file.FileStreamSourceConnector",
    ConnectorConfig.TASKS_MAX_CONFIG to "1",
    "topic" to "file-topic",
    "file" to "/var/log/*"
)

fun WorkerProperties(bootstapServers: String, schemaRegistryUrl: String): Properties {
    val props = Properties()
    props.putAll(WorkerConfig(bootstapServers, schemaRegistryUrl))
    return props
}

fun SourceProperties(): Properties {
    val props = Properties()
    props.putAll(SourceConfig)
    return props
}