package com.programmableproduction.kafka.kotlin.testing


import java.net.InetAddress
import java.net.ServerSocket


object PortProvider {
    fun appy(count: Int): Array<Int>{
        return Array<Int>(count) { _ ->
            val serverSocket =ServerSocket(0, 0, InetAddress.getLocalHost())
            val port = serverSocket.localPort
            serverSocket.close()
            port
        }
    }

    val one: Int = appy(1).first()
}
