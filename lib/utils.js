'use strict'

function timeout(actionPromise, timeoutInMs, description) {
  description = description || ''
  const timeoutPromise = new Promise((resolve, reject) => {
    setTimeout(reject, timeoutInMs, `${description} timeout after ${timeoutInMs}`).unref()
  })

  return Promise.race([actionPromise, timeoutPromise])
}

module.exports = {
  timeout
}
