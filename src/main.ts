import {In} from 'typeorm'
import {DataHandlerContext, assertNotNull} from '@subsquid/evm-processor'
import {TypeormDatabase} from '@subsquid/typeorm-store'
import * as erc20 from './abi/erc20'
import {Account, Transfer} from './model'
import {Block, Context, Log, Transaction, processor} from './processor'
import {DeferredValue, StoreWithCache} from './store'
import {string} from './model/generated/marshal'

processor.run(new TypeormDatabase({supportHotBlocks: true}), async (ctx) => {
    const newCtx = {
        ...ctx,
        store: new StoreWithCache(ctx.store),
    }

    let transfers: TransferEvent[] = []

    for (let block of ctx.blocks) {
        for (let log of block.logs) {
            if (log.topics[0] === erc20.events.Transfer.topic) {
                transfers.push(getTransfer(newCtx, log))
            }
        }
    }

    await processTransfers(newCtx, transfers)
})

interface TransferEvent {
    id: string
    block: Block
    transaction: Transaction
    fromId: string
    from: DeferredValue<Account>
    toId: string
    to: DeferredValue<Account>
    amount: bigint
}

function getTransfer(ctx: DataHandlerContext<StoreWithCache>, log: Log): TransferEvent {
    let event = erc20.events.Transfer.decode(log)

    let from = event.from.toLowerCase()
    let to = event.to.toLowerCase()
    let amount = event.value

    let transaction = assertNotNull(log.transaction, `Missing transaction`)

    ctx.log.debug({block: log.block, txHash: transaction.hash}, `Transfer from ${from} to ${to} amount ${amount}`)

    return {
        id: log.id,
        block: log.block,
        transaction,
        fromId: from,
        from: ctx.store.defer(Account, from),
        toId: to,
        to: ctx.store.defer(Account, to),
        amount,
    }
}

async function processTransfers(ctx: DataHandlerContext<StoreWithCache>, transfersData: TransferEvent[]) {
    let transfers: Transfer[] = []

    for (let t of transfersData) {
        let {id, block, transaction, amount} = t

        let from = await t.from.get()
        if (from == null) {
            from = await createAccount(ctx, t.fromId)
        }
        let to = await t.to.get()
        if (to == null) {
            to = await createAccount(ctx, t.toId)
        }

        transfers.push(
            new Transfer({
                id,
                blockNumber: block.height,
                timestamp: new Date(block.timestamp),
                txHash: transaction.hash,
                from,
                to,
                amount,
            })
        )
    }

    await ctx.store.insert(transfers)
}

async function createAccount(ctx: DataHandlerContext<StoreWithCache>, id: string): Promise<Account> {
    const acc = new Account({id})
    await ctx.store.insert(acc)

    return acc
}
