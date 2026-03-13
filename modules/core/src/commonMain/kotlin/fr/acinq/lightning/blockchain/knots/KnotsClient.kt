package fr.acinq.lightning.blockchain.knots

import fr.acinq.bitcoin.TxId
import fr.acinq.lightning.blockchain.Feerates
import fr.acinq.lightning.blockchain.IClient
import fr.acinq.lightning.blockchain.fee.FeeratePerByte
import fr.acinq.lightning.blockchain.fee.FeeratePerKw

/**
 * Minimal IClient implementation backed by a KnotsDescriptorWallet connection.
 * Provides fee rate estimation and transaction confirmation lookups.
 */
class KnotsClient(private val wallet: KnotsDescriptorWallet) : IClient {

    override suspend fun getConfirmations(txId: TxId): Int? {
        // The wallet tracks its own transactions; for now return null (unknown)
        // TODO: add confirmation query to the wallet protocol
        return null
    }

    override suspend fun getFeerates(): Feerates? {
        // TODO: query blockchain.estimatefee from the Knots server
        // For now return null to let the node use default feerates
        return null
    }
}
