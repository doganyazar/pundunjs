'use strict'
const assert = require('assert')
const debug = require('debug')('client')
const Transport = require('./transport.js')
const utils = require('./utils.js')

const Enum = require('./constants.js')

class Client {
  constructor(opts) {
    assert(opts.username)
    assert(opts.password)
    assert(opts.host)
    assert(opts.port)

    this.transport = new Transport(opts)
    this.tid = 0xFFFFFFFF // max value
    this.cid = 0xFFFF // max value
    this.messageMap = new Map()
  }

  _handleIncomingMessage(message) {
    debug('Incoming Buffer', message)
    this._parse(message)
  }

  _waitForMessageWithId(cId, timeout=5000) {
    return new Promise((resolve, reject) => {
      this.messageMap.set(cId, data => { resolve(data) })
      setTimeout(() => {
        this.messageMap.delete(cId)
        reject(new Error(`Timeout waiting for message with corr id ${cId}`))
      }, timeout)
    })
  }

  _parse(buf) {
    if (buf.length < 2) {
      debug('Invalid incoming buffer', buf)
      return
    }

    const cId = buf.readUInt16BE(0)
    const data = buf.slice(2)

    const waitCb = this.messageMap.get(cId)
    if (waitCb) {
      const pdu = utils.type('ApolloPdu').decode(data)
      if (pdu) {
        debug('Incoming message', JSON.stringify(pdu))
        waitCb(pdu)
      } else {
        debug('Discarding incoming message with cId', cId)
      }
    } else {
      debug('No callback is waiting for response with corr id', cId)
    }
  }

  //TODO if response not ok, fail the promise!
  _writePdu(_data) {
    const data = utils.type('ApolloPdu').encode(_data).finish()
    //TODO what is cid?
    this.cid = utils.incrementCid(this.cid)
    const cId = this.cid

    debug('Decode before sending %j', utils.type('ApolloPdu').decode(data))

    const header = Buffer.alloc(6)
    header.writeUInt32BE(data.length + 2, 0)
    header.writeUInt16BE(cId, 4)
    debug('Header', header, data.length)

    const buf = Buffer.concat([header, data])

    debug('Sending buffer', buf)
    return this.transport.send(buf)
      .then(() => this._waitForMessageWithId(cId))
  }

  //procedure should be provided in an object ex: {createTable: {...}}
  _makePdu(procedure) {
    const ApolloPdu = utils.type('ApolloPdu')
    this.tid = utils.incrementTid(this.tid)
    const pdu = ApolloPdu.create({
      version: {major: 0, minor: 1},
      transactionId: this.tid
    })

    Object.assign(pdu, procedure)

    const res = ApolloPdu.verify(pdu)
    if (res) {
      debug('Verification failed for', pdu)
      throw new Error('Verification error')
    }
    return pdu
  }

  connect() {
    return this.transport.connect(this._handleIncomingMessage.bind(this))
      .then(() => debug('Connection is successful!'))
  }

  disconnect() {
    return this.transport.disconnect()
  }

  tableInfo(tableName, attributes) {
    const pdu = this._makePdu({tableInfo: {tableName, attributes}})
    return this._writePdu(pdu).then(r => utils.formatResponse(r))
  }

  //TODO pass enum instead of magic 0
  createTable(tableName, keys, _tableOptions = [{type: Enum.Type.ROCKSDB}]) {
    const tableOptions = _tableOptions.map(opt => {
      debug('opt:', opt)
      return utils.type('TableOption').create(opt)
    })
    debug('tableOptions', tableOptions)
    const pdu = this._makePdu({createTable: {tableName, keys, tableOptions}})
    return this._writePdu(pdu).then(r => utils.formatResponse(r))
  }

  openTable(tableName) {
    const pdu = this._makePdu({openTable: {tableName}})
    return this._writePdu(pdu).then(r => utils.formatResponse(r))
  }

  closeTable(tableName) {
    const pdu = this._makePdu({closeTable: {tableName}})
    return this._writePdu(pdu).then(r => utils.formatResponse(r))
  }

  deleteTable(tableName) {
    const pdu = this._makePdu({deleteTable: {tableName}})
    return this._writePdu(pdu).then(r => utils.formatResponse(r))
  }

  write(tableName, _key, _columns) {
    const key =  _key.map(keyField => utils.makeField(keyField))
    const columns = _columns.map(c => utils.makeField(c))

    const pdu = this._makePdu({write: {tableName, key, columns}})
    return this._writePdu(pdu).then(r => utils.formatResponse(r))
  }

  read(tableName, _key) {
    const key =  _key.map(keyField => utils.makeField(keyField))
    const pdu = this._makePdu({read: {tableName, key}})
    return this._writePdu(pdu).then(r => utils.formatResponse(r))
  }

  readRange(tableName, _startKey, _endKey, limit) {
    const startKey =  _startKey.map(key => utils.makeField(key))
    const endKey =  _endKey.map(key => utils.makeField(key))
    const pdu = this._makePdu({readRange: {tableName, startKey, endKey, limit}})
    return this._writePdu(pdu).then(r => utils.formatResponse(r))
  }

  readRangeN(tableName, _startKey, n) {
    const startKey =  _startKey.map(key => utils.makeField(key))
    const pdu = this._makePdu({readRangeN: {tableName, startKey, n}})
    return this._writePdu(pdu).then(r => utils.formatResponse(r))
  }

  delete(tableName, _key) {
    const key =  _key.map(keyField => utils.makeField(keyField))
    const pdu = this._makePdu({delete: {tableName, key}})
    return this._writePdu(pdu).then(r => utils.formatResponse(r))
  }

  //TODO validate the operations object!
  update(tableName, _key, operations) {
    const makeOp = op => {
      op.updateInstruction.threshold =
      utils.uIntToBinaryDefault(op.updateInstruction.threshold)
      op.updateInstruction.setValue =
      utils.uIntToBinaryDefault(op.updateInstruction.setValue)

      const updateInstruction = utils.type('UpdateInstruction').create(op.updateInstruction)

      const field = op.field
      const value = utils.type('Value').create({int: op.value})
      const defaultValue = utils.type('Value').create({int: op.defaultValue})

      return utils.type('UpdateOperation').create({
        field,
        updateInstruction,
        value,
        defaultValue
      })
    }

    const updateOperation = operations.map(op => makeOp(op))

    const key =  _key.map(keyField => utils.makeField(keyField))
    const pdu = this._makePdu({update: {tableName, key, updateOperation}})
    return this._writePdu(pdu).then(r => utils.formatResponse(r))
  }

  first(tableName) {
    const pdu = this._makePdu({first: {tableName}})
    return this._writePdu(pdu).then(r => utils.formatResponse(r))
  }

  next(it) {
    const pdu = this._makePdu({next: {it}})
    return this._writePdu(pdu).then(r => utils.formatResponse(r))
  }

  prev(it) {
    const pdu = this._makePdu({prev: {it}})
    return this._writePdu(pdu).then(r => utils.formatResponse(r))
  }

  last(tableName) {
    const pdu = this._makePdu({last: {tableName}})
    return this._writePdu(pdu).then(r => utils.formatResponse(r))
  }

  seek(tableName, _key) {
    const key =  _key.map(keyField => utils.makeField(keyField))
    const pdu = this._makePdu({seek: {tableName, key}})
    return this._writePdu(pdu).then(r => utils.formatResponse(r))
  }

  addIndex(tableName, config) {
    const makeConf = conf => {
      const column = conf.column
      if(typeof conf.indexOptions === 'undefined'){
        return utils.type('IndexConfig').create({
          column: column
        })
      }
      const charFilter = conf.indexOptions.charFilter
      const tokenizer =  conf.indexOptions.tokenizer
      const transform = conf.indexOptions.tokenFilter.transform
      const add = conf.indexOptions.tokenFilter.add
      const delete_ = conf.indexOptions.tokenFilter.delete
      const stats = conf.indexOptions.tokenFilter.stats
      const tokenFilter = utils.type('TokenFilter').create({
        transform: transform,
        add: add,
        delete: delete_,
        stats: stats
      })
      debug('AddIndex.. tokenFilter: ', tokenFilter)
      const options = utils.type('IndexOptions').create({
        charFilter: charFilter,
        tokenizer: tokenizer,
        tokenFilter: tokenFilter
      })
      return utils.type('IndexConfig').create({
        column: column,
        options: options
      })
    }
    const indexConfig = config.map(conf => makeConf(conf))
    const pdu = this._makePdu({addIndex: {
      tableName: tableName,
      config: indexConfig}})
    debug('pdu: ', pdu)
    return this._writePdu(pdu).then(r => utils.formatResponse(r))
  }

  removeIndex(tableName, columns) {
    const pdu = this._makePdu({removeIndex: {tableName, columns}})
    return this._writePdu(pdu).then(r => utils.formatResponse(r))
  }

  indexRead(tableName, columnName, term, filter) {
    filter.startTs = utils.uIntToBinaryDefault(filter.startTs)
    filter.endTs = utils.uIntToBinaryDefault(filter.endTs)
    debug('modified filter:', filter)
    const pdu = this._makePdu({indexRead: {
      tableName, columnName, term, filter}})
    return this._writePdu(pdu).then(r => utils.formatResponse(r))
  }

  listTables() {
    const pdu = this._makePdu({listTables: {}})
    return this._writePdu(pdu).then(r => utils.formatResponse(r))
  }
}

module.exports = Client

if (require.main === module) {
  process.on('unhandledRejection', (reason, p) => {
    debug('Unhandled Rejection at: Promise', p, 'reason:', reason)
  })

  const client = new Client({
    host: '127.0.0.1',
    port: '8887',
    username: 'admin',
    password: 'admin'
  })

  const co = require('co')
  co(function*() {
    const tableName = 'pundunjs_test_table'
    const key = [{'id' : '0001'}, {'ts' : Date.now()}]
    const key2 = [{'id': '0001'}, {'ts': Date.now()}]
    yield client.connect()
    let tabExists = yield client.listTables()
      .then(r => {
        return r.find(t => t === tableName) === tableName
      })
    if(tabExists){
      yield client.deleteTable(tableName)
    }
    yield client.createTable(tableName, ['id', 'ts'],
      [{numOfShards: 1}])
    yield client.addIndex(tableName, [
      {
        column: 'name'
      },
      {
        column: 'text',
        indexOptions: {
          charFilter: Enum.CharFilter.NFC,
          tokenizer: Enum.Tokenizer.UNICODE_WORD_BOUNDARIES,
          tokenFilter: {
            transform : Enum.TokenTransform.CASEFOLD,
            add: [],
            delete: [],
            stats: Enum.TokenStats.POSITION,
          }
        }
      }])

    yield client.write(tableName,
      key,
      [ {'name': 'Erdem Aksu'},
        {'text': 'Husband Father and Coder'} ])

    yield client.write(tableName,
      key2,
      [ {'text': 'Some irrelevant text here and there'},
        {'is_data': true},
        {'some_int': 900},
        {'blob': utils.uIntToBinaryDefault(0x123456)},
        {'blank': null},
        {'double': 99.45} ])

    yield client.read(tableName, key)

    yield client.indexRead(tableName, 'name', 'Erdem Aksu', {
      sortBy: Enum.SortBy.RELEVANCE,
      maxPostings: 5
    })

    let epochSeconds = Math.round(Date.now()/1000)
    yield client.indexRead(tableName, 'text', 'here', {
      sortBy: Enum.SortBy.RELEVANCE,
      startTs: epochSeconds - 100,
      endTs: epochSeconds,
      maxPostings: 5
    })

    yield client.readRange(tableName, key2, key, 10)
    yield client.readRangeN(tableName, key2, 2)

    const kcpIt = yield client.first(tableName)
    yield client.next(kcpIt.it)
    const kcpIt2 = yield client.seek(tableName, key2)
    yield client.prev(kcpIt2.it)
    yield client.last(tableName)

    yield client.update(tableName, key, [{
      field: 'abc',
      updateInstruction: {
        instruction: Enum.Instruction.INCREMENT,
        threshold: 10,
        setValue: 0
      },
      value: 1,
      defaultValue: 0
    }])
    yield client.tableInfo(tableName, ['index_on'])
    yield client.removeIndex(tableName, ['name'])
    yield client.tableInfo(tableName, ['index_on'])
    yield client.delete(tableName, key)
  })
}
