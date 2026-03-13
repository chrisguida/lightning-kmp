package fr.acinq.lightning.blockchain.knots

import fr.acinq.bitcoin.TxId
import fr.acinq.lightning.blockchain.Feerates
import fr.acinq.lightning.blockchain.IClient
import fr.acinq.lightning.blockchain.fee.FeeratePerByte
import fr.acinq.lightning.blockchain.fee.FeeratePerKw
import fr.acinq.lightning.utils.sat

/**
 * IClient implementation backed by a KnotsDescriptorWallet connection.
 * Provides fee rate estimation and transaction confirmation lookups.
 */
class KnotsClient(private val wallet: KnotsDescriptorWallet) : IClient {

    override suspend fun getConfirmations(txId: TxId): Int? {
        return wallet.getTransactionConfirmations(txId)
    }

    override suspend fun getFeerates(): Feerates? {
        // Electrum returns BTC/kB, we need to convert to sat/byte
        fun btcKbToFeeratePerByte(btcPerKb: Double): FeeratePerByte {
            val satPerKb = (btcPerKb * 100_000_000).toLong()
            val satPerByte = satPerKb / 1000
            return FeeratePerByte(satPerByte.coerceAtLeast(1).sat)
        }

        return Feerates(
            minimum = wallet.estimateFee(144)?.let { btcKbToFeeratePerByte(it) } ?: return null,
            slow = wallet.estimateFee(18)?.let { btcKbToFeeratePerByte(it) } ?: return null,
            medium = wallet.estimateFee(6)?.let { btcKbToFeeratePerByte(it) } ?: return null,
            fast = wallet.estimateFee(2)?.let { btcKbToFeeratePerByte(it) } ?: return null,
            fastest = wallet.estimateFee(1)?.let { btcKbToFeeratePerByte(it) } ?: return null,
        )
    }
}
