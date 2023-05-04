import {In} from 'typeorm'
import {lookupArchive} from '@subsquid/archive-registry'
import {
    EvmBatchProcessor,
    DataHandlerContext,
    EvmBatchProcessorFields,
    Log as _Log,
    Transaction as _Transaction,
    BlockHeader,
    assertNotNull,
} from '@subsquid/evm-processor'
import {Store, TypeormDatabase} from '@subsquid/typeorm-store'
import * as erc20 from './abi/erc20'
import {Account, Transfer} from './model'

const processor = new EvmBatchProcessor()
    .setDataSource({
        archive: 'https://v2.archive.subsquid.io/network/ethereum-mainnet',
        chain: 'https://rpc.ankr.com/eth',
    })
    .setFields({
        log: {
            topics: true,
            data: true,
        },
        transaction: {
            hash: true,
        },
    })
    .addLog({
        range: {from: 6_082_465},
        address: ['0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48'],
        topic0: [erc20.events.Transfer.topic],
        transaction: true,
    })

type Fields = EvmBatchProcessorFields<typeof processor>
type Context = DataHandlerContext<Store, Fields>
type Block = BlockHeader<Fields>
type Log = _Log<Fields>
type Transaction = _Transaction<Fields>

processor.run(new TypeormDatabase({supportHotBlocks: true}), async (ctx) => {
    let transfersData: TransferEvent[] = []

    for (let block of ctx.blocks) {
        for (let log of block.logs) {
            if (log.topics[0] === erc20.events.Transfer.topic) {
                transfersData.push(decodeTransfer(ctx, log))
            }
        }
    }

    await processTransfers(ctx, transfersData)
})

async function processTransfers(ctx: Context, transfersData: TransferEvent[]) {
    let accountIds = new Set<string>()
    for (let t of transfersData) {
        accountIds.add(t.from)
        accountIds.add(t.to)
    }

    let accounts = await ctx.store
        .findBy(Account, {id: In([...accountIds])})
        .then((q) => new Map(q.map((i) => [i.id, i])))

    let transfers: Transfer[] = []

    for (let t of transfersData) {
        let {id, block, transaction, amount} = t

        let from = getAccount(accounts, t.from)
        let to = getAccount(accounts, t.to)

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

    await ctx.store.save(Array.from(accounts.values()))
    await ctx.store.insert(transfers)
}

interface TransferEvent {
    id: string
    block: Block
    transaction: Transaction
    from: string
    to: string
    amount: bigint
}

function decodeTransfer(ctx: Context, log: Log): TransferEvent {
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
        from,
        to,
        amount,
    }
}

function getAccount(m: Map<string, Account>, id: string): Account {
    let acc = m.get(id)
    if (acc == null) {
        acc = new Account()
        acc.id = id
        m.set(id, acc)
    }
    return acc
}
