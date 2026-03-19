package fr.acinq.lightning.blockchain.knots

import fr.acinq.bitcoin.Bitcoin
import fr.acinq.bitcoin.Chain
import fr.acinq.bitcoin.Script
import fr.acinq.bitcoin.Transaction
import fr.acinq.bitcoin.TxId
import fr.acinq.bitcoin.utils.Either
import fr.acinq.lightning.blockchain.electrum.WalletState
import fr.acinq.lightning.crypto.SwapInOnChainKeys
import fr.acinq.lightning.io.TcpSocket
import fr.acinq.lightning.io.send
import fr.acinq.lightning.io.receiveAvailable
import fr.acinq.lightning.logging.LoggerFactory
import fr.acinq.lightning.logging.info
import fr.acinq.lightning.logging.warning
import fr.acinq.lightning.logging.debug
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
    private val chain: Chain,
    private val swapInKeys: SwapInOnChainKeys,
    private val scope: CoroutineScope,
    private val loggerFactory: LoggerFactory,
    private val lookAhead: Int = 3,
) {
    private val logger = loggerFactory.newLogger(this::class)

    private val _walletStateFlow = MutableStateFlow(WalletState.empty)
    val walletStateFlow: StateFlow<WalletState> = _walletStateFlow.asStateFlow()

    /** Flow of (address, index) for the current unused swap-in address. */
    val swapInAddressFlow = MutableStateFlow<Pair<String, Int>?>(null)

    /** Flow of new/confirmed transactions (parsed from notifications that include raw hex). */
    private val _newTransactionsFlow = MutableSharedFlow<Transaction>()
    val newTransactionsFlow: Flow<Transaction> = _newTransactionsFlow

    private val _connected = MutableStateFlow(false)
    val connected: StateFlow<Boolean> = _connected.asStateFlow()

    private var socket: TcpSocket? = null
    private var readLoopJob: Job? = null
    private var requestId = 0
    private var walletId: String? = null
    var currentTipHeight: Int = 0
        private set
    private val pendingRequests = mutableMapOf<Int, CompletableDeferred<JsonElement>>()
    private val json = Json { ignoreUnknownKeys = true }

    // Lookup from scriptPubKey hex → (address, derivation index)
    private val scriptToAddressIndex = mutableMapOf<String, Pair<String, Int>>()
    // How many addresses we've derived so far
    private var derivedCount = 0

    init {
        // Watch for address rotation
        scope.launch {
            walletStateFlow
                .map { it.firstUnusedDerivedAddress }
                .filterNotNull()
                .distinctUntilChanged()
                .collect { (address, derived) ->
                    logger.info { "setting current swap-in address=$address index=${derived.index}" }
                    swapInAddressFlow.emit(address to derived.index)
                }
        }
    }

    /** Derive addresses from startIndex..endIndex and add to the lookup map. */
    private fun deriveAddresses(startIndex: Int, endIndex: Int) {
        for (i in startIndex..endIndex) {
            val protocol = swapInKeys.getSwapInProtocol(i)
            val address = protocol.address(chain)
            val spkHex = protocol.serializedPubkeyScript.toHex()
            scriptToAddressIndex[spkHex] = address to i
        }
        derivedCount = maxOf(derivedCount, endIndex + 1)
        logger.debug { "derived addresses $startIndex..$endIndex (total: $derivedCount)" }
    }

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
        // Clean up any previous connection
        readLoopJob?.cancelAndJoin()
        readLoopJob = null
        try { socket?.close() } catch (_: Exception) {}
        socket = null
        _connected.value = false
        // Cancel any pending RPC calls from the old connection
        pendingRequests.values.forEach { it.cancel() }
        pendingRequests.clear()

        logger.info { "connecting to Knots server at ${serverAddress.host}:${serverAddress.port}" }

        val sock = socketBuilder.connect(
            serverAddress.host, serverAddress.port,
            tls = serverAddress.tls,
            loggerFactory = loggerFactory
        )
        socket = sock
        _connected.value = true

        // Start reading responses/notifications in background
        readLoopJob = scope.launch { readLoop(sock) }

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
        } catch (e: Exception) {
            logger.info { "wallet already exists, opening: $walletName (create error: ${e.message})" }
        }

        try {
            rpcCall("wallet.open", buildJsonObject { put("wallet_id", walletName) })
        } catch (e: Exception) {
            // Wallet may already be loaded by the node at startup — that's OK
            logger.info { "wallet.open: ${e.message} (may be already loaded)" }
        }
        walletId = walletName
        logger.info { "wallet opened: $walletName" }

        // Import descriptor (may already be imported from a previous run)
        try {
            rpcCall("wallet.import_descriptor", buildJsonObject {
                put("wallet_id", walletName)
                put("descriptor", descriptor)
                put("range", buildJsonArray { add(range.first); add(range.second) })
                put("timestamp", "now")
            })
            logger.info { "imported descriptor range=${range.first}-${range.second}" }
        } catch (e: Exception) {
            logger.info { "descriptor may already be imported: ${e.message}" }
        }

        // Pre-derive addresses for the imported range to enable UTXO → index lookup
        deriveAddresses(range.first, lookAhead - 1)

        // Subscribe to wallet notifications
        rpcCall("wallet.subscribe", buildJsonObject { put("wallet_id", walletName) })
        logger.info { "subscribed to wallet notifications" }

        // Subscribe to headers
        val headerResult = rpcCall("blockchain.headers.subscribe", JsonNull)
        headerResult.jsonObject["height"]?.jsonPrimitive?.intOrNull?.let {
            currentTipHeight = it
            logger.info { "current tip height: $it" }
        }

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

                // Look up derivation index from scriptPubKey
                val spkHex = previousTx.txOut[txPos].publicKeyScript.toHex()
                val addressIndex = scriptToAddressIndex[spkHex]
                val meta = if (addressIndex != null) {
                    WalletState.AddressMeta.Derived(addressIndex.second)
                } else {
                    WalletState.AddressMeta.Single
                }

                WalletState.Utxo(
                    txId = TxId(txHash),
                    outputIndex = txPos,
                    blockHeight = height,
                    previousTx = previousTx,
                    addressMeta = meta
                )
            } catch (e: Exception) {
                logger.warning { "failed to process UTXO: ${e.message}" }
                null
            }
        }

        // Track which addresses have UTXOs (used addresses)
        val usedAddresses = mutableSetOf<String>()
        val addressStates = mutableMapOf<String, WalletState.AddressState>()

        // Group UTXOs by address
        for (utxo in utxos) {
            val spkHex = utxo.previousTx.txOut[utxo.outputIndex].publicKeyScript.toHex()
            val addressIndex = scriptToAddressIndex[spkHex]
            val address = addressIndex?.first ?: spkHex
            usedAddresses.add(address)
            val existing = addressStates[address]
            if (existing != null) {
                addressStates[address] = existing.copy(utxos = existing.utxos + utxo)
            } else {
                addressStates[address] = WalletState.AddressState(
                    meta = utxo.addressMeta,
                    alreadyUsed = true,
                    utxos = listOf(utxo)
                )
            }
        }

        // Find the highest used index to ensure look-ahead extends beyond it
        val highestUsedIndex = usedAddresses.mapNotNull { addr ->
            scriptToAddressIndex.values.firstOrNull { it.first == addr }?.second
        }.maxOrNull() ?: -1

        // Ensure we have enough derived addresses for look-ahead
        val neededUpTo = highestUsedIndex + lookAhead
        if (neededUpTo >= derivedCount) {
            deriveAddresses(derivedCount, neededUpTo)
        }

        // Add unused derived addresses (for look-ahead / firstUnusedDerivedAddress)
        for ((_, addressAndIndex) in scriptToAddressIndex) {
            val (address, index) = addressAndIndex
            if (address !in usedAddresses && index <= neededUpTo) {
                addressStates[address] = WalletState.AddressState(
                    meta = WalletState.AddressMeta.Derived(index),
                    alreadyUsed = false,
                    utxos = emptyList()
                )
            }
        }

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
            put("tx_hex", fr.acinq.secp256k1.Hex.encode(Transaction.write(tx)))
        })
        return TxId(result.jsonObject["txid"]!!.jsonPrimitive.content)
    }

    /**
     * Estimate fee rate in BTC/kB for a given confirmation target.
     * Returns null if estimation is not available.
     */
    suspend fun estimateFee(numBlocks: Int): Double? {
        return try {
            val result = rpcCall("blockchain.estimatefee", buildJsonArray { add(numBlocks) })
            val fee = result.jsonPrimitive.double
            if (fee < 0) null else fee
        } catch (_: Exception) {
            null
        }
    }

    /**
     * Get the number of confirmations for a wallet transaction.
     * Returns null if the transaction is not found in the wallet.
     */
    suspend fun getTransactionConfirmations(txId: TxId): Int? {
        val wid = walletId ?: return null
        return try {
            val txJson = rpcCall("wallet.get_transaction", buildJsonObject {
                put("wallet_id", wid)
                put("txid", txId.toString())
            })
            val height = txJson.jsonObject["height"]?.jsonPrimitive?.intOrNull ?: return 0
            if (height <= 0) return 0
            if (currentTipHeight <= 0) return 1 // confirmed but tip unknown
            currentTipHeight - height + 1
        } catch (_: Exception) {
            null
        }
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
                    buffer.deleteRange(0, newline + 1)
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
                        // Parse raw tx hex from notification if present
                        val params = msg["params"]
                        val txHex = when {
                            params is JsonArray && params.size > 1 -> params[1].jsonObject["hex"]?.jsonPrimitive?.contentOrNull
                            params is JsonObject -> params["hex"]?.jsonPrimitive?.contentOrNull
                            else -> null
                        }
                        scope.launch {
                            if (txHex != null) {
                                try {
                                    _newTransactionsFlow.emit(Transaction.read(txHex))
                                } catch (e: Exception) {
                                    logger.warning { "failed to parse tx from notification: ${e.message}" }
                                }
                            }
                            refreshWalletState()
                        }
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
                    "blockchain.headers.subscribe" -> {
                        val params = msg["params"]
                        val height = when {
                            params is JsonArray && params.size > 0 -> params[0].jsonObject["height"]?.jsonPrimitive?.intOrNull
                            params is JsonObject -> params["height"]?.jsonPrimitive?.intOrNull
                            else -> null
                        }
                        if (height != null) {
                            currentTipHeight = height
                            logger.info { "new block: height=$height" }
                        }
                    }
                }
            }
        } catch (e: Exception) {
            logger.warning { "failed to parse message: ${e.message}" }
        }
    }
}
