'use strict'

const crypto = require('crypto')
const tls = require('tls')
const Scram = require('sasl-scram-sha-1/')
const debug = require('debug')('scram')
const co = require('co')
const utils = require('./utils.js')

const protobuf = require('protobufjs')

//TODO may not be suitable for production
const root = protobuf.loadSync('./doc/apollo.proto')

function type(typeName) {
  return root.lookup(typeName)
}

class Client {
  constructor(connectParams, credentials) {

    this.socket = null
    this.mechanism = null
    this.credentials = credentials
    this.connectParams = connectParams

    this.tid = 0
  }

  openSocket() {
    return new Promise((resolve, reject) => {
      debug('Accessing server', this.connectParams, this.credentials)
      if (this.socket) {
        resolve()
      }
      const socket = tls.connect(this.connectParams, () => {
        debug('Socket is opened successfully!')
        this.socket = socket
        resolve()
      })

      socket.on('error', error => {
        debug('ERROR', error)
        reject(error)
      })
    })
  }

  send(data) {
    return new Promise((resolve, reject) => {
      this.socket.write(data, err => {
        if (err) {
          reject(err)
        } else {
          resolve()
        }
      })
    })
  }

  write(data) {
    //TODO what is cid?
    const cId = 0

    const header = Buffer.alloc(6)
    header.writeUInt32BE(data.length + 2, 0)
    header.writeUInt16BE(cId, 4)
    debug('HEADEr', header, data.length)

    const buf = Buffer.concat([header, data])

    debug('Sending buffer', buf)
    return this.send(buf)
  }

  authenticate() {
    const generateMechanism = len => {
      return new Scram({
        genNonce: function() {
          return crypto.randomBytes(Math.ceil(len * 3 / 4))
            .toString('base64') // convert to base64 format
            .slice(0, len) // return required number of characters
            .replace(/\+/g, '0') // replace '+' with '0'
            .replace(/\//g, '0') // replace '/' with '0'
        }
      })
    }

    const authPromise = new Promise((resolve, reject) => {
      this.mechanism = generateMechanism(12)

      const clientFirstMessage = this.mechanism.response(this.credentials)
      debug('Client First:   ', clientFirstMessage)
      this.send(clientFirstMessage)

      //TODO remove listener
      //TODO how to reject?
      this.socket.on('data', message => {
        console.log('INCOMING MESSAGE:', message)
        var challangeStr
        if (this.mechanism._stage === 'challenge') {
          challangeStr = message.toString()
          debug('Server First:   ', challangeStr)

          this.mechanism.challenge(challangeStr)
          var clientFinalMessage = this.mechanism.response(this.credentials)
          debug('Client Final: ', clientFinalMessage)
          this.send(clientFinalMessage)

        } else if (this.mechanism._stage === 'final') {
          debug('Server Final:   ', message.toString())
          this.authenticated = true
          return resolve()
        }
      })
    })

    return utils.timeout(authPromise, 2000, 'Auth')
  }

  connect() {
    return co(function*() {
      if (this.authenticated) {
        debug('Already authenticated!')
        return
      }

      yield this.openSocket()
      yield this.authenticate()
      debug('Connection successful!')

    }.bind(this))
  }

  createTable(tableName, keys) {
    const ApolloPdu = type('ApolloPdu')
    const tableOptions = [type('TableOption').create({type: 0})]

    const pdu = ApolloPdu.create({
      version: {major: 0, minor: 1},
      transactionId: this.tid++,
      createTable: {tableName, keys, tableOptions}
    })

    const res = ApolloPdu.verify(pdu)
    if (res) {
      console.log('VERIFY RES', res)
      throw new Error(res)
    }

    const encoded = ApolloPdu.encode(pdu).finish()

    debug('DECODE BEFORE SENDING', ApolloPdu.decode(encoded))

    return this.write(encoded)
  }

}

module.exports = Client

if (require.main === module) {
  //TODO passing constructor params
  const client = new Client({
    host: 'localhost',
    port: '32768',
    rejectUnauthorized: false
  }, {
    username: 'admin',
    password: 'admin'
  })

  client.connect().then(() => {
    client.createTable('dodo-table12', ['dodo-key'])
  })
}
