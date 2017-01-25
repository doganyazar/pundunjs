'use strict'

const crypto = require('crypto')
const tls = require('tls')
const Scram = require('sasl-scram-sha-1/')
const debug = require('debug')('transport')
const utils = require('./utils.js')

class Transport {
  constructor(connectParams, credentials) {
    this.connectParams = connectParams
    this.credentials = credentials
    this.mechanism = null
  }

  connect(cb) {
    return this._openSocket().then(() => this._authenticate()).then(() => {
      this.socket.on('data', cb)
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

  _openSocket() {
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

  _authenticate() {
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

    const authPromise = new Promise((resolve) => {
      this.mechanism = generateMechanism(12)

      const clientFirstMessage = this.mechanism.response(this.credentials)
      debug('Client First:   ', clientFirstMessage)
      this.send(clientFirstMessage)

      this.socket.on('data', message => {
        let challangeStr
        if (this.mechanism._stage === 'challenge') {
          challangeStr = message.toString()
          debug('Server First:   ', challangeStr)

          this.mechanism.challenge(challangeStr)
          const clientFinalMessage = this.mechanism.response(this.credentials)
          debug('Client Final: ', clientFinalMessage)
          this.send(clientFinalMessage)
        } else if (this.mechanism._stage === 'final') {
          debug('Server Final:   ', message.toString())
          resolve()
        }
      })
    })

    //Remove the listener even if the authentication fails
    authPromise.catch(() => null).then(() => this.socket.removeAllListeners('data'))

    return utils.timeout(authPromise, 2000, 'Auth')
  }
}

module.exports = Transport
