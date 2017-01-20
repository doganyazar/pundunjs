'use strict'

const crypto = require('crypto')
const tls = require('tls')
const Scram = require('sasl-scram-sha-1/')
const debug = require('debug')('scram')
const co = require('co')
const utils = require('./lib/utils.js')
//const apollo = require('./gen/apollo_pb.js')

class Client {
  constructor(connectParams, credentials) {
    this.socket = null
    this.mechanism = null
    this.credentials = credentials || {
      username: 'admin',
      password: 'admin'
    }

    this.connectParams = connectParams || {
      host: 'localhost',
      port: '32768',
      rejectUnauthorized: false
    }
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

  write(data) {
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

  authenticate() {
    const authPromise = new Promise((resolve, reject) => {
      this.mechanism = this.generateMechanism(12)

      const clientFirstMessage = this.mechanism.response(this.credentials)
      debug('Client First:   ', clientFirstMessage)
      this.write(clientFirstMessage)

      //TODO remove listener
      //TODO how to reject?
      this.socket.on('data', message => {
        var challangeStr
        if (this.mechanism._stage === 'challenge') {
          challangeStr = message.toString()
          debug('Server First:   ', challangeStr)

          this.mechanism.challenge(challangeStr)
          var clientFinalMessage = this.mechanism.response(this.credentials)
          debug('Client Final: ', clientFinalMessage)
          this.write(clientFinalMessage)

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
        return resolve()
      }

      yield this.openSocket()
      yield this.authenticate()
      debug('Connection successful!')

    }.bind(this))
  }

  generateMechanism(len) {
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

}

if (require.main === module) {
  new Client().connect()
}
