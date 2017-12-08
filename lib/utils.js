'use strict'
const util = require('util')
const protobuf = require('protobufjs')
const path = require('path')
const root = protobuf.loadSync(path.join(__dirname, '../doc/apollo.proto'))
const debug = require('debug')('utils')

module.exports = {
  timeout,
  type,
  incrementTid,
  incrementCid,
  uIntToBinaryDefault,
  formatResponse,
  makeField
}

function timeout(actionPromise, timeoutInMs, description) {
  description = description || ''
  const timeoutPromise = new Promise((resolve, reject) => {
    setTimeout(reject, timeoutInMs, `${description} timeout after ${timeoutInMs}`).unref()
  })

  return Promise.race([actionPromise, timeoutPromise])
}

function incrementTid(tid){
  if (tid < 0xFFFFFFFF) { return tid + 1 }
  else { return 0 }
}

function incrementCid(cid){
  if (cid < 0xFFFF) { return cid + 1 }
  else { return 0 }
}


function type(typeName) {
  return root.lookup(typeName)
}

function uIntToBinaryDefault(attr){
  if (typeof attr === 'number' && Number.isInteger(attr) && attr > 0) {
    const len = calcRequiredByteLength(attr)
    const Buf = Buffer.allocUnsafe(len)
    Buf.writeUIntBE(attr, 0, len)
    return Buf
  } else {
    const Buf = Buffer.allocUnsafe(0)
    return Buf
  }
}

function calcRequiredByteLength(u) {
  if (u <= 0xFF) {return 1}
  else if (u <= 0xFFFF) {return 2}
  else if (u <= 0xFFFFFF) {return 3}
  else if (u <= 0xFFFFFFFF) {return 4}
  else if (u <= 0xFFFFFFFFFF) {return 5}
  else {return 6}
}

function formatResponse(r){
  //debug('format response: ', util.inspect(r, {showHidden: false,depth: null}))
  if( r.response ){
    if( r.response.ok ){
      debug('response: ok')
      return 'ok'
    } else if( r.response.columns ){
      const res = r.response.columns.fields.map(f => formatField(f))
      debug('response: ', util.inspect(res, {showHidden: false,depth: null}))
      return res
    } else if( r.response.keyColumnsList ){
      const res = r.response.keyColumnsList.list.map(kcp => formatKCP(kcp))
      debug('response: ', util.inspect(res, {showHidden: false,depth: null}))
      return res
    } else if( r.response.keyColumnsPair ){
      const res = formatKCP(r.response.keyColumnsPair)
      debug('response: ', util.inspect(res, {showHidden: false,depth: null}))
      return res
    } else if( r.response.kcpIt ){
      const kcpIt = r.response.kcpIt
      const kcp = formatKCP(kcpIt.keyColumnsPair)
      const it = kcpIt.it
      const res = {kcp, it}
      debug('response: ', util.inspect(res, {showHidden: false,depth: null}))
      return res
    } else if( r.response.postings ){
      const res = r.response.postings.list.map(p => formatPosting(p))
      debug('response: ', util.inspect(res, {showHidden: false,depth: null}))
      return res
    } else if( r.response.stringList ){
      const res = r.response.stringList.fieldNames
      debug('response: ', util.inspect(res, {showHidden: false,depth: null}))
      return res
    }
  } else if( r.error ){
    const keys = Object.keys(r.error)
    const values = Object.values(r.error)
    const res = {error: values[0], type: keys[0]}
    debug('response: ', util.inspect(res, {showHidden: false,depth: null}))
    return res
  } else {
    Error('Invalid response received.')
  }
}

function formatField(f) {
  const values = Object.values(f)
  const keys = Object.keys(f)
  let value
  switch (keys[1]) {
    case 'boolean':
      value = values[1]
      break
    case 'int':
      value = values[1].toNumber()
      break
    case 'binary':
      value = values[1]
      break
    case 'null':
      value = null
      break
    case 'double':
      value = values[1]
      break
    case 'string':
      value = values[1]
      break
    default:
      value = values[1]
  }
  return {[values[0]]: value}
}

function formatKCP(kcp) {
  const key = kcp.key.map(f => formatField(f))
  const columns = kcp.columns.map(f => formatField(f))
  return {key, columns}
}

function formatPosting(p) {
  const key = p.key.map(f => formatField(f))
  const frequency = p.frequency === 0 ? undefined : p.frequency
  const position = p.position === 0 ? undefined : p.frequency
  const timestamp = p.timestamp
  return {key, frequency, position, timestamp}
}

function makeField(f){
  const keyField = setFieldParameters(f)
  return type('Field').create(keyField)
}

function setFieldParameters(f){
  const name = Object.keys(f)[0]
  let type
  let value = Object.values(f)[0]
  switch (typeof value) {
    case 'boolean':
      type = 'boolean'
      break
    case 'string':
      type = 'string'
      break
    default:
      if (typeof value === 'number' && Number.isInteger(value)) {
        type = 'int'
      } else if (typeof value === 'number' && value % 1 !== 0){
        type ='double'
      } else if (typeof value === 'object' && util.isBuffer(value)){
        type = 'binary'
      } else if (value === null){
        type = 'null'
        value = uIntToBinaryDefault(0x123456)
      }
  }
  return {name, [type]: value}
}
