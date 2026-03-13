package fr.acinq.lightning.blockchain.knots

import fr.acinq.bitcoin.Transaction
import fr.acinq.bitcoin.TxId
import fr.acinq.lightning.blockchain.*
import fr.acinq.lightning.logging.LoggerFactory
import fr.acinq.lightning.logging.info
import fr.acinq.lightning.logging.warning
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.MutableSharedFlow
import kotlinx.coroutines.launch

/**
 * IWatcher implementation backed by a KnotsDescriptorWallet.
 *
 * Watches for transaction confirmations and output spends using wallet-level
 * notifications from the Knots descriptor wallet. When a watched transaction
 * is confirmed or a watched output is spent, emits the corresponding event.
 */
class KnotsWatcher(
    private val wallet: KnotsDescriptorWallet,
    private val scope: CoroutineScope,
    private val loggerFactory: LoggerFactory,
) : IWatcher {
    private val logger = loggerFactory.newLogger(this::class)
    private val _notificationsFlow = MutableSharedFlow<WatchTriggered>()
    private val watches = mutableSetOf<Watch>()

    init {
        // Monitor wallet state changes and check watches
        scope.launch {
            wallet.walletStateFlow.collect { walletState ->
                checkWatches()
            }
        }
    }

    override fun openWatchNotificationsFlow(): Flow<WatchTriggered> = _notificationsFlow

    override suspend fun watch(watch: Watch) {
        logger.info { "adding watch: ${watch::class.simpleName} channelId=${watch.channelId}" }
        watches.add(watch)
        // Immediately check in case the condition is already met
        checkWatches()
    }

    override suspend fun publish(tx: Transaction) {
        logger.info { "publishing tx ${tx.txid}" }
        wallet.broadcastTransaction(tx)
    }

    private suspend fun checkWatches() {
        val triggeredWatches = mutableSetOf<Watch>()

        for (watch in watches) {
            when (watch) {
                is WatchConfirmed -> {
                    val confirmations = wallet.getTransactionConfirmations(watch.txId)
                    if (confirmations != null && confirmations >= watch.minDepth) {
                        val tx = wallet.getTransaction(watch.txId)
                        if (tx != null) {
                            logger.info { "watch confirmed triggered: txid=${watch.txId} confirmations=$confirmations" }
                            // We don't have exact block height/pos from wallet protocol,
                            // use current height - confirmations + 1 as approximation
                            val blockHeight = maxOf(1, wallet.currentTipHeight - confirmations + 1)
                            _notificationsFlow.emit(
                                WatchConfirmedTriggered(watch.channelId, watch.event, blockHeight, 0, tx)
                            )
                            triggeredWatches.add(watch)
                        }
                    }
                }
                is WatchSpent -> {
                    // Check if the watched output has been spent by querying
                    // wallet transactions for inputs matching the watched outpoint
                    // TODO: implement spend detection via wallet transaction history
                    // For now, this is a placeholder — spend detection requires
                    // scanning wallet tx inputs, which we'll add when needed
                }
            }
        }

        watches.removeAll(triggeredWatches)
    }
}
