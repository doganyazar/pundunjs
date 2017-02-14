'use strict'

const assert = require('assert')
const debug = require('debug')('client')
const Transport = require('./transport.js')

const protobuf = require('protobufjs')
const path = require('path')
const root = protobuf.loadSync(path.join(__dirname, '../doc/apollo.proto'))

function type(typeName) {
  return root.lookup(typeName)
}

class Client {
  constructor(opts) {
    assert(opts.username)
    assert(opts.password)
    assert(opts.host)
    assert(opts.port)

    this.transport = new Transport(opts)
    this.tid = 0
    this.messageMap = new Map()
  }

  _handleIncomingMessage(message) {
    debug('Incoming Buffer', message)
    this._parse(message)
  }

  _waitForMessageWithId(tId, timeout=1000) {
    return new Promise((resolve, reject) => {
      this.messageMap.set(tId, data => {
        if (data.error) {
          const err = new Error('Response error')
          err.response = data
          reject(err)
        } else {
          resolve(data)
        }
      })
      setTimeout(() => {
        this.messageMap.delete(tId)
        reject(new Error(`Timeout waiting for message with id ${tId}`))
      }, timeout)
    })
  }

  _parse(buf) {
    if (buf.length < 6) {
      debug('Invalid incoming buffer', buf)
      return
    }

    let len = buf.readUInt32BE(0) - 2
    if (len < 0) {
      debug('Invalid incoming buf len', len)
      return
    }

    //const cid = buf.readUInt16BE(4) //TODO not used at the moment
    const data = buf.slice(6)

    //TODO not checking buf size at the moment
    const pdu = type('ApolloPdu').decode(data)

    if (pdu) {
      debug('Incoming message', JSON.stringify(pdu))
      const waitCb = this.messageMap.get(pdu.transactionId)
      if (waitCb) {
        waitCb(pdu)
      } else {
        debug('Discarding incoming message with tId', pdu.transactionId)
      }
    }
  }

  //TODO if response not ok, fail the promise!
  _writePdu(_data) {
    const data = type('ApolloPdu').encode(_data).finish()
    //TODO what is cid?
    const cId = 0

    debug('Decode before sending %j', type('ApolloPdu').decode(data))

    const header = Buffer.alloc(6)
    header.writeUInt32BE(data.length + 2, 0)
    header.writeUInt16BE(cId, 4)
    debug('Header', header, data.length)

    const buf = Buffer.concat([header, data])

    debug('Sending buffer', buf)
    return this.transport.send(buf).then(() => this._waitForMessageWithId(_data.transactionId))
  }

  //procedure should be provided in an object ex: {createTable: {...}}
  _makePdu(procedure) {
    const ApolloPdu = type('ApolloPdu')

    const pdu = ApolloPdu.create({
      version: {major: 0, minor: 1},
      transactionId: this.tid++
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
    return this._writePdu(pdu)
  }

  //TODO pass enum instead of magic 0
  createTable(tableName, keys, _tableOptions = [{type: 0}]) {
    const tableOptions = _tableOptions.map(opt => type('TableOption').create(opt))
    const pdu = this._makePdu({createTable: {tableName, keys, tableOptions}})
    return this._writePdu(pdu)
  }

  openTable(tableName) {
    const pdu = this._makePdu({openTable: {tableName}})
    return this._writePdu(pdu)
  }

  closeTable(tableName) {
    const pdu = this._makePdu({closeTable: {tableName}})
    return this._writePdu(pdu)
  }

  deleteTable(tableName) {
    const pdu = this._makePdu({deleteTable: {tableName}})
    return this._writePdu(pdu)
  }

  write(tableName, _key, _columns) {
    const key =  _key.map(keyField => type('Field').create(keyField))
    const columns = _columns.map(c => type('Field').create(c))

    const pdu = this._makePdu({write: {tableName, key, columns}})
    return this._writePdu(pdu)
  }

  read(tableName, _key) {
    const key =  _key.map(keyField => type('Field').create(keyField))
    const pdu = this._makePdu({read: {tableName, key}})
    return this._writePdu(pdu)
  }

  readRange(tableName, _startKey, _endKey, limit) {
    const startKey =  _startKey.map(key => type('Field').create(key))
    const endKey =  _endKey.map(key => type('Field').create(key))
    const pdu = this._makePdu({readRange: {tableName, startKey, endKey, limit}})
    return this._writePdu(pdu)
  }

  readRangeN(tableName, _startKey, n) {
    const startKey =  _startKey.map(key => type('Field').create(key))
    const pdu = this._makePdu({readRangeN: {tableName, startKey, n}})
    return this._writePdu(pdu)
  }

  delete(tableName, _key) {
    const key =  _key.map(keyField => type('Field').create(keyField))
    const pdu = this._makePdu({delete: {tableName, key}})
    return this._writePdu(pdu)
  }

  //TODO validate the operations object!
  update(tableName, _key, operations) {
    const makeOp = op => {

      //TODO only supports values up to 2^32-1 for now
      const thresholdBuf = Buffer.allocUnsafe(4)
      thresholdBuf.writeUInt32BE(op.updateInstruction.threshold, 0)
      const setValueBuf = Buffer.allocUnsafe(4)
      setValueBuf.writeUInt32BE(op.updateInstruction.setValue, 0)

      op.updateInstruction.threshold = thresholdBuf
      op.updateInstruction.setValue = setValueBuf

      const updateInstruction = type('UpdateInstruction').create(op.updateInstruction)

      const field = op.field
      const value = type('Value').create({int: op.value})
      const defaultValue = type('Value').create({int: op.defaultValue})

      return type('UpdateOperation').create({
        field,
        updateInstruction,
        value,
        defaultValue
      })
    }

    const updateOperation = operations.map(op => makeOp(op))

    const key =  _key.map(keyField => type('Field').create(keyField))
    const pdu = this._makePdu({update: {tableName, key, updateOperation}})
    return this._writePdu(pdu)
  }

  first(tableName) {
    const pdu = this._makePdu({first: {tableName}})
    return this._writePdu(pdu).then(r => r.response.kcpIt)
  }

  next(it) {
    const pdu = this._makePdu({next: {it}})
    return this._writePdu(pdu).then(r => r.response.keyColumnsPair)
  }

  prev(it) {
    const pdu = this._makePdu({prev: {it}})
    return this._writePdu(pdu).then(r => r.response.keyColumnsPair)
  }

  last(tableName) {
    const pdu = this._makePdu({last: {tableName}})
    return this._writePdu(pdu).then(r => r.response.kcpIt)
  }

  seek(tableName, _key) {
    const key =  _key.map(keyField => type('Field').create(keyField))
    const pdu = this._makePdu({seek: {tableName, key}})
    return this._writePdu(pdu).then(r => r.response.kcpIt)
  }
}

module.exports = Client

if (require.main === module) {
  const client = new Client({
    host: '192.168.211.150',
    port: '8887',
    username: 'admin',
    password: 'admin'
  })

  const co = require('co')
  co(function*() {
    const tableName = 'dodo-table2'
    const key = [{name: 'key1', string: 'value1'}, {name: 'key2', string: 'value2'}]
    const key2 = [{name: 'key1', string: 'value2'}, {name: 'key2', string: 'value3'}]
    yield client.connect()
    yield client.createTable(tableName, ['key1', 'key2'])
    yield client.write(tableName,
      key,
      [{name: 'column1', string: 'kolum kadar yer'}])
    yield client.write(tableName,
      key2,
      [{name: 'column2', string: 'obur kolum kadar yer'}])

    yield client.read(tableName, key)

    yield client.readRange(tableName, key2, key, 10)
    yield client.readRangeN(tableName, key2, 2)

    const kcpIt = yield client.first(tableName)
    const kvp = yield client.next(kcpIt.it)
    debug('KVP', kvp)
    const kcpIt2 = yield client.seek(tableName, key)
    yield client.prev(kcpIt2.it)
    yield client.last(tableName)

    yield client.update(tableName, key, [{
      field: 'abc',
      updateInstruction: {
        instruction: 0,    //TODO use enum here
        threshold: 10,
        setValue: 0
      },
      value: 1,
      defaultValue: 0
    }])
    yield client.delete(tableName, key)
    yield client.deleteTable(tableName)
  })
}
