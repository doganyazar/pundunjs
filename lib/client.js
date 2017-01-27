'use strict'

const assert = require('assert')
const debug = require('debug')('client')
const Transport = require('./transport.js')

const protobuf = require('protobufjs')

//TODO may not be suitable for production
const root = protobuf.loadSync('./doc/apollo.proto')

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
    debug('Incoming', message)
    this._parse(message)
  }

  _waitForMessageWithId(tId, timeout=1000) {
    return new Promise((resolve, reject) => {
      this.messageMap.set(tId, data => {
        resolve(data)
      })
      setTimeout(() => {
        this.messageMap.delete(tId)
        reject(new Error('Timeout waiting for message with id', tId))
      }, timeout)
    })
  }

  _parse(buf) {
    if (buf.length < 6) {
      debug('Invalid incoming buf', buf)
      return
    }

    let len = buf.readUInt32BE(0) - 2
    if (len < 0) {
      debug('Invalid incoming buf len', len)
      return
    }
    //const cid = buf.readUInt16BE(4) //TODO not used
    const data = buf.slice(6)

    //TODO not checking buf size at the moment
    const pdu = type('ApolloPdu').decode(data)

    if (pdu) {
      debug('Incoming message', pdu)
      const waitCb = this.messageMap.get(pdu.transactionId)
      if (waitCb) {
        waitCb(pdu)
      } else {
        debug('Discarding incoming message with tId', pdu.transactionId)
      }
    }
  }

  _writePdu(_data) {
    const data = type('ApolloPdu').encode(_data).finish()
    //TODO what is cid?
    const cId = 0

    debug('Decode before sending', type('ApolloPdu').decode(data))

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
      throw new Error(res)
    }

    return pdu
  }

  connect() {
    return this.transport.connect(this._handleIncomingMessage.bind(this))
    .then(() => debug('Connection is successful!'))
  }

  tableInfo(tableName, attributes) {
    const pdu = this._makePdu({tableInfo: {tableName, attributes}})
    return this._writePdu(pdu)
  }

  createTable(tableName, keys) {
    const tableOptions = [type('TableOption').create({type: 0})]

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
}

module.exports = Client

if (require.main === module) {
  const client = new Client({
    host: 'localhost',
    port: '32768',
    username: 'admin',
    password: 'admin'
  })

  const co = require('co')
  co(function*() {
    const tableName = 'dodo-table16'
    yield client.connect()
    yield client.createTable(tableName, ['dodo-key'])
    yield client.tableInfo(tableName, ['type', 'key'])
    yield client.openTable(tableName)
    yield client.closeTable(tableName)
  })
}
