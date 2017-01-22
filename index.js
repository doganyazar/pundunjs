'use strict'

const protobuf = require('protobufjs')
const Client = require('./lib/client.js')

protobuf.load('./doc/apollo.proto').then(root => {
  // Obtain a message type
  const ApolloPdu = root.lookup('ApolloPdu')
  const Version = root.lookup('Version')
  const CreateTable = root.lookup('CreateTable')

  //TODO passing constructor params
  const client = new Client({
    ApolloPdu,
    Version,
    CreateTable
  }, {
    host: 'localhost',
    port: '32768',
    rejectUnauthorized: false
  }, {
    username: 'admin',
    password: 'admin'
  })

  module.exports = client

  if (require.main === module) {
    client.connect().then(() => {
      return client.createTable('dodo-table1', ['dodo-key1'])
    })
  }
})
