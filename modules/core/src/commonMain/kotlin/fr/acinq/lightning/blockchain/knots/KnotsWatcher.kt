package fr.acinq.lightning.blockchain.knots

import fr.acinq.bitcoin.Transaction
import fr.acinq.lightning.blockchain.IWatcher
import fr.acinq.lightning.blockchain.Watch
import fr.acinq.lightning.blockchain.WatchTriggered
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.MutableSharedFlow

/**
 * Minimal IWatcher implementation backed by a KnotsDescriptorWallet connection.
 * Publishes transactions and watches for confirmations/spends via wallet notifications.
 */
class KnotsWatcher(private val wallet: KnotsDescriptorWallet) : IWatcher {

    private val _notifications = MutableSharedFlow<WatchTriggered>()

    override fun openWatchNotificationsFlow(): Flow<WatchTriggered> = _notifications

    override suspend fun watch(watch: Watch) {
        // TODO: the Knots wallet tracks all wallet transactions automatically.
        // We may need to add specific watch tracking for channel funding txs.
    }

    override suspend fun publish(tx: Transaction) {
        wallet.broadcastTransaction(tx)
    }
}
