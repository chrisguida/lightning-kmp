package fr.acinq.lightning.blockchain.knots

import fr.acinq.bitcoin.Transaction
import fr.acinq.bitcoin.TxId
import fr.acinq.lightning.blockchain.electrum.WalletState
import fr.acinq.lightning.io.TcpSocket
import fr.acinq.lightning.io.send
import fr.acinq.lightning.io.receiveAvailable
import fr.acinq.lightning.logging.LoggerFactory
import fr.acinq.lightning.utils.ServerAddress
import kotlinx.coroutines.*
import kotlinx.coroutines.flow.*
import kotlinx.serialization.json.*
import kotlin.time.Duration.Companion.seconds

/**
 * A wallet backed by a Bitcoin Knots descriptor-electrum server.
 *
 * Instead of subscribing to individual scripthashes (standard Electrum), this wallet
 * imports a descriptor to a Knots-managed Core wallet and receives push notifications
 * for all wallet activity. UTXOs and transactions are queried at the wallet level.
 */
class KnotsDescriptorWallet(
    private val serverAddress: ServerAddress,
    private val scope: CoroutineScope,
    private val loggerFactory: LoggerFactory,
) {
    private val logger = loggerFactory.newLogger(this::class)

    private val _walletStateFlow = MutableStateFlow(WalletState.empty)
    val walletStateFlow: StateFlow<WalletState> = _walletStateFlow.asStateFlow()

    private val _connected = MutableStateFlow(false)
    val connected: StateFlow<Boolean> = _connected.asStateFlow()

    private var socket: TcpSocket? = null
    private var requestId = 0
    private var walletId: String? = null
    private val pendingRequests = mutableMapOf<Int, CompletableDeferred<JsonElement>>()
    private val json = Json { ignoreUnknownKeys = true }

    /**
     * Connect to the Knots server and set up a wallet with the given descriptor.
     */
    suspend fun connect(
        socketBuilder: TcpSocket.Builder,
        walletName: String,
        descriptor: String,
        range: Pair<Int, Int> = 0 to 1000,
        singleAddresses: List<String> = emptyList(),
    ) {
        logger.info { "connecting to Knots server at ${serverAddress.host}:${serverAddress.port}" }

        val sock = socketBuilder.connect(
            serverAddress.host, serverAddress.port,
            tls = serverAddress.tls,
            loggerFactory = loggerFactory
        )
        socket = sock
        _connected.value = true

        // Start reading responses/notifications in background
        scope.launch { readLoop(sock) }

        // Handshake
        val version = rpcCall("server.version", buildJsonArray {
            add("phoenixd-knots/0.1")
            add(buildJsonArray { add("0.1") })
        })
        logger.info { "server version: $version" }

        // Create or open wallet
        try {
            rpcCall("wallet.create", buildJsonObject { put("wallet_id", walletName) })
            logger.info { "created wallet: $walletName" }
        } catch (_: Exception) {
            logger.info { "wallet already exists, opening: $walletName" }
        }

        rpcCall("wallet.open", buildJsonObject { put("wallet_id", walletName) })
        walletId = walletName

        // Import descriptor
        rpcCall("wallet.import_descriptor", buildJsonObject {
            put("wallet_id", walletName)
            put("descriptor", descriptor)
            put("range", buildJsonArray { add(range.first); add(range.second) })
            put("timestamp", "now")
        })
        logger.info { "imported descriptor range=${range.first}-${range.second}" }

        // Subscribe to wallet notifications
        rpcCall("wallet.subscribe", buildJsonObject { put("wallet_id", walletName) })
        logger.info { "subscribed to wallet notifications" }

        // Subscribe to headers
        rpcCall("blockchain.headers.subscribe", JsonNull)

        // Initial UTXO fetch
        refreshWalletState()
    }

    /**
     * Refresh wallet state by querying UTXOs from the Knots wallet.
     */
    suspend fun refreshWalletState() {
        val wid = walletId ?: return

        val utxosJson = rpcCall("wallet.get_utxos", buildJsonObject { put("wallet_id", wid) })
        val utxoArray = utxosJson.jsonArray

        // Cache of fetched transactions to avoid re-fetching
        val txCache = mutableMapOf<String, Transaction>()

        val utxos = utxoArray.mapNotNull { elem ->
            try {
                val obj = elem.jsonObject
                val txHash = obj["tx_hash"]!!.jsonPrimitive.content
                val txPos = obj["tx_pos"]!!.jsonPrimitive.int
                val height = obj["height"]?.jsonPrimitive?.longOrNull ?: 0L

                // Fetch parent transaction (from cache or server)
                val previousTx = txCache.getOrPut(txHash) {
                    val txJson = rpcCall("wallet.get_transaction", buildJsonObject {
                        put("wallet_id", wid)
                        put("txid", txHash)
                    })
                    val rawHex = txJson.jsonObject["hex"]!!.jsonPrimitive.content
                    Transaction.read(rawHex)
                }

                WalletState.Utxo(
                    txId = TxId(txHash),
                    outputIndex = txPos,
                    blockHeight = height,
                    previousTx = previousTx,
                    addressMeta = WalletState.AddressMeta.Single
                )
            } catch (e: Exception) {
                logger.warning { "failed to process UTXO: ${e.message}" }
                null
            }
        }

        // Build WalletState from UTXOs grouped by scriptPubKey
        val addressStates = utxos.groupBy { utxo ->
            utxo.previousTx.txOut[utxo.outputIndex].publicKeyScript.toHex()
        }.map { (spkHex, utxoList) ->
            spkHex to WalletState.AddressState(
                meta = WalletState.AddressMeta.Single,
                alreadyUsed = true,
                utxos = utxoList
            )
        }.toMap()

        _walletStateFlow.value = WalletState(addressStates)
        logger.info { "wallet updated: ${utxos.size} utxos, balance=${_walletStateFlow.value.totalBalance}" }
    }

    /**
     * Fetch a raw transaction from the wallet.
     */
    suspend fun getTransaction(txId: TxId): Transaction? {
        val wid = walletId ?: return null
        return try {
            val txJson = rpcCall("wallet.get_transaction", buildJsonObject {
                put("wallet_id", wid)
                put("txid", txId.toString())
            })
            Transaction.read(txJson.jsonObject["hex"]!!.jsonPrimitive.content)
        } catch (_: Exception) {
            null
        }
    }

    /**
     * Broadcast a transaction.
     */
    suspend fun broadcastTransaction(tx: Transaction): TxId {
        val result = rpcCall("blockchain.broadcast", buildJsonObject {
            put("tx_hex", Transaction.write(tx).toHex())
        })
        return TxId(result.jsonObject["txid"]!!.jsonPrimitive.content)
    }

    fun stop() {
        socket?.close()
        _connected.value = false
    }

    // --- JSON-RPC transport ---

    private suspend fun rpcCall(method: String, params: JsonElement): JsonElement {
        val id = requestId++
        val request = buildJsonObject {
            put("jsonrpc", "2.0")
            put("id", id)
            put("method", method)
            put("params", params)
        }

        val deferred = CompletableDeferred<JsonElement>()
        pendingRequests[id] = deferred

        val msg = json.encodeToString(JsonObject.serializer(), request) + "\n"
        socket!!.send(msg.encodeToByteArray())

        return withTimeout(30.seconds) { deferred.await() }
    }

    private suspend fun readLoop(sock: TcpSocket) {
        val buffer = StringBuilder()
        val readBuf = ByteArray(4096)
        try {
            while (true) {
                val n = sock.receiveAvailable(readBuf)
                if (n <= 0) {
                    logger.warning { "connection closed by server" }
                    break
                }
                buffer.append(readBuf.decodeToString(0, n))

                while (true) {
                    val newline = buffer.indexOf('\n')
                    if (newline == -1) break
                    val line = buffer.substring(0, newline)
                    buffer.delete(0, newline + 1)
                    if (line.isNotBlank()) processMessage(line)
                }
            }
        } catch (e: TcpSocket.IOException) {
            logger.warning { "connection error: ${e.message}" }
        } finally {
            _connected.value = false
        }
    }

    private fun processMessage(line: String) {
        try {
            val msg = json.parseToJsonElement(line).jsonObject
            val id = msg["id"]?.jsonPrimitive?.intOrNull

            if (id != null) {
                // Response
                val error = msg["error"]
                if (error != null && error !is JsonNull) {
                    val errorMsg = error.jsonObject["message"]?.jsonPrimitive?.content ?: "Unknown error"
                    pendingRequests.remove(id)?.completeExceptionally(Exception("Server error: $errorMsg"))
                } else {
                    pendingRequests.remove(id)?.complete(msg["result"] ?: JsonNull)
                }
            } else {
                // Notification
                val method = msg["method"]?.jsonPrimitive?.content
                when (method) {
                    "wallet.tx_added", "wallet.tx_confirmed", "wallet.balance_changed" -> {
                        logger.info { "wallet notification: $method" }
                        scope.launch { refreshWalletState() }
                    }
                    "wallet.scan_progress" -> {
                        val params = msg["params"]?.jsonObject
                        val progress = params?.get("progress")?.jsonPrimitive?.floatOrNull
                        logger.info { "scan progress: ${progress?.let { "${(it * 100).toInt()}%" } ?: "?"}" }
                    }
                    "wallet.scan_complete" -> {
                        logger.info { "scan complete" }
                        scope.launch { refreshWalletState() }
                    }
                }
            }
        } catch (e: Exception) {
            logger.warning { "failed to parse message: ${e.message}" }
        }
    }
}
