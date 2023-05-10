import {EntityManager, FindOptionsWhere, In} from 'typeorm'
import assert from 'assert'
import {Entity as _Entity, EntityClass, FindManyOptions, FindOneOptions, Store} from '@subsquid/typeorm-store'
import {Graph} from 'graph-data-structure'
import {def} from '@subsquid/util-internal'

export interface Entity extends _Entity {
    [k: string]: any
}

export type DeferList = Map<string, Set<string>>
export type CacheMap<E extends Entity> = Map<string, Map<string, E | null>>

export class StoreWithCache {
    private em: EntityManager

    private deferList: DeferList = new Map()
    private cacheMap: CacheMap<any> = new Map()
    private classes: Map<string, EntityClass<any>> = new Map()

    private insertList: Map<string, Map<string, Entity>> = new Map()
    private upsertList: Map<string, Map<string, Entity>> = new Map()

    constructor(private store: Store) {
        this.em = (this.store as any).em()
    }

    async insert<E extends _Entity>(entity: E): Promise<void>
    async insert<E extends _Entity>(entities: E[]): Promise<void>
    async insert<E extends _Entity>(e: E | E[]): Promise<void> {
        const entities = Array.isArray(e) ? e : [e]
        if (entities.length == 0) return

        const entityName = entities[0].constructor.name
        const _deferList = this.getDeferList(entityName)
        const _insertList = this.getInsertList(entityName)
        const _upsertList = this.getUpsertList(entityName)
        const _cacheMap = this.getCacheMap(entityName)
        for (const entity of entities) {
            _deferList.delete(entity.id)

            assert(!_insertList.has(entity.id))
            assert(!_upsertList.has(entity.id))

            const cached = _cacheMap.get(entity.id)
            assert(cached == null)

            _insertList.set(entity.id, entity)
            _cacheMap.set(entity.id, entity)
        }
    }

    async upsert<E extends _Entity>(entity: E): Promise<void>
    async upsert<E extends _Entity>(entities: E[]): Promise<void>
    async upsert<E extends _Entity>(e: E | E[]): Promise<void> {
        const entities = Array.isArray(e) ? e : [e]
        if (entities.length == 0) return

        const entityName = entities[0].constructor.name
        const _deferList = this.getDeferList(entityName)
        const _insertList = this.getInsertList(entityName)
        const _upsertList = this.getUpsertList(entityName)
        const _cacheMap = this.getCacheMap(entityName)
        for (const entity of entities) {
            _deferList.delete(entity.id)

            if (!_insertList.has(entity.id)) {
                _upsertList.set(entity.id, entity)
            }

            _cacheMap.set(entity.id, entity)
        }
    }

    async save<E extends _Entity>(entity: E): Promise<void>
    async save<E extends _Entity>(entities: E[]): Promise<void>
    async save<E extends _Entity>(e: E | E[]): Promise<void> {
        return await this.upsert(e as any)
    }

    remove<E extends Entity>(entity: E): Promise<void>
    remove<E extends Entity>(entities: E[]): Promise<void>
    remove<E extends Entity>(entityClass: EntityClass<E>, id: string | string[]): Promise<void>
    async remove(entityClass: any, id?: any): Promise<void> {
        // await this.flush(entityClass)
        // await this.store.remove(entityClass, id)
        throw new Error('not implemented')
    }

    async count<E extends Entity>(entityClass: EntityClass<E>, options?: FindManyOptions<E>): Promise<number> {
        await this.flush(entityClass)
        return await this.store.count(entityClass, options)
    }

    async countBy<E extends Entity>(
        entityClass: EntityClass<E>,
        where: FindOptionsWhere<E> | FindOptionsWhere<E>[]
    ): Promise<number> {
        await this.flush(entityClass)
        return await this.store.countBy(entityClass, where)
    }

    async find<E extends Entity>(entityClass: EntityClass<E>, options?: FindManyOptions<E>): Promise<E[]> {
        await this.flush(entityClass)
        return await this.store.find(entityClass, options).then((v) => this.cache(v))
    }

    async findBy<E extends Entity>(
        entityClass: EntityClass<E>,
        where: FindOptionsWhere<E> | FindOptionsWhere<E>[]
    ): Promise<E[]> {
        await this.flush(entityClass)
        return await this.store.findBy(entityClass, where).then((v) => this.cache(v))
    }

    async findOne<E extends Entity>(entityClass: EntityClass<E>, options: FindOneOptions<E>): Promise<E | undefined> {
        await this.flush(entityClass)
        return await this.store.findOne(entityClass, options).then((v) => v && this.cache(v))
    }

    async findOneBy<E extends Entity>(
        entityClass: EntityClass<E>,
        where: FindOptionsWhere<E> | FindOptionsWhere<E>[]
    ): Promise<E | undefined> {
        await this.flush(entityClass)
        return await this.store.findOneBy(entityClass, where).then((v) => v && this.cache(v))
    }

    async findOneOrFail<E extends Entity>(entityClass: EntityClass<E>, options: FindOneOptions<E>): Promise<E> {
        return await this.findOne(entityClass, options).then(assertNotNull)
    }

    async findOneByOrFail<E extends Entity>(
        entityClass: EntityClass<E>,
        where: FindOptionsWhere<E> | FindOptionsWhere<E>[]
    ): Promise<E> {
        return await this.findOneBy(entityClass, where).then(assertNotNull)
    }

    async get<E extends Entity>(
        entityClass: EntityClass<E>,
        optionsOrId: string | FindOneOptions<E>
    ): Promise<E | undefined> {
        if (typeof optionsOrId === 'string') {
            await this.load()
            const id = optionsOrId
            const _cacheMap = this.getCacheMap(entityClass.name)
            const entity = _cacheMap.get(id)
            if (entity !== undefined) {
                return entity ? structuredClone(entity as E) : undefined
            } else {
                await this.flush(entityClass)
                return await this.store.get(entityClass, id).then((v) => v && this.cache(v))
            }
        } else {
            await this.flush(entityClass)
            return await this.store.get(entityClass, optionsOrId).then((v) => v && this.cache(v))
        }
    }

    async getOrFail<E extends Entity>(
        entityClass: EntityClass<E>,
        optionsOrId: string | FindOneOptions<E>
    ): Promise<E> {
        return await this.get(entityClass, optionsOrId).then(assertNotNull)
    }

    // defer<T extends Entity>(entityClass: EntityClass<T>, idOrList: string): DeferredValue<T | T[]> // defer<T extends Entity>(entityClass: EntityClass<T>, ids: string[]): DeferredValue<T[]>
    defer<T extends Entity>(entityClass: EntityClass<T>, id: string): DeferredValue<T> {
        this.classes.set(entityClass.name, entityClass)

        // const ids = Array.isArray(idOrList) ? idOrList : [idOrList]

        const _deferredList = this.getDeferList(entityClass.name)
        const _cacheMap = this.getCacheMap(entityClass.name)
        // for (const id of ids) {
        if (!_cacheMap.has(id)) {
            _deferredList.add(id)
        }
        // }

        return new DeferredValue(this, entityClass, id)
    }

    async flush<E extends Entity>(entityClass: EntityClass<E>): Promise<void> {
        const entityOrder = await this.getTopologicalOrder()

        for (const name of entityOrder) {
            const _cacheMap = this.getCacheMap(name)
            const _insertList = this.getInsertList(name)
            if (_insertList.size > 0) {
                const entities = _insertList.values()
                await this.store.insert([...entities])
            }
            _insertList.clear()

            const _upsertList = this.getUpsertList(name)
            if (_upsertList.size > 0) {
                const entities = _upsertList.values()
                await this.store.upsert([...entities])
            }
            _upsertList.clear()

            if (entityClass.name === name) break
        }
    }

    private async load(): Promise<void> {
        for (const [name, _deferList] of this.deferList) {
            if (_deferList.size === 0) continue

            const entityClass = this.classes.get(name)
            assert(entityClass != null)

            await this.find(entityClass, {where: {id: In([..._deferList])}})

            const _cacheMap = this.getCacheMap(name)
            for (const id of _deferList) {
                if (_cacheMap.has(id)) continue
                _cacheMap.set(id, null)
            }

            _deferList.clear()
        }
    }

    private cache<E extends Entity>(entity: E): Promise<E>
    private cache<E extends Entity>(entities: E[]): Promise<E[]>
    private async cache<E extends Entity>(e: E | E[]) {
        const entities = Array.isArray(e) ? e : [e]
        if (entities.length == 0) return

        const _deferList = this.getDeferList(entities[0].constructor.name)
        const _cacheMap = this.getCacheMap(entities[0].constructor.name)
        for (const entity of entities) {
            _deferList.delete(entity.id)
            _cacheMap.set(entity.id, entity)

            const _em = await this.em
            const metadata = _em.connection.entityMetadatasMap.get(entity.constructor)
            assert(metadata != null)
            for (const relation of metadata.relations) {
                const value = entity[relation.propertyName]
                if (value == null) continue
                await this.cache(value)
            }
        }

        return Array.isArray(e) ? entities : entities[0]
    }

    @def
    private async getTopologicalOrder() {
        const graph = Graph()
        for (const metadata of this.em.connection.entityMetadatas) {
            graph.addNode(metadata.name)
            for (const foreignKey of metadata.foreignKeys) {
                graph.addEdge(metadata.name, foreignKey.referencedEntityMetadata.name)
            }
        }
        return graph.topologicalSort().reverse()
    }

    private getDeferList(name: string) {
        let list = this.deferList.get(name)
        if (list == null) {
            list = new Set()
            this.deferList.set(name, list)
        }

        return list
    }

    private getCacheMap<E extends Entity>(name: string): Map<string, E | null> {
        let map = this.cacheMap.get(name)
        if (map == null) {
            map = new Map()
            this.cacheMap.set(name, map)
        }

        return map
    }

    private getInsertList(name: string): Map<string, Entity> {
        let list = this.insertList.get(name)
        if (list == null) {
            list = new Map()
            this.insertList.set(name, list)
        }

        return list
    }

    private getUpsertList(name: string): Map<string, Entity> {
        let list = this.upsertList.get(name)
        if (list == null) {
            list = new Map()
            this.upsertList.set(name, list)
        }

        return list
    }
}

function assertNotNull<T>(val: T | null | undefined): T {
    assert(val != null)
    return val
}

export class DeferredValue<E extends Entity> {
    constructor(private store: StoreWithCache, private entityClass: EntityClass<E>, private id: string) {}

    @def
    async get(): Promise<E | undefined> {
        return await this.store.get(this.entityClass, this.id)
    }
}
