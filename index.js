/**
 * Convert new media requests.
 *
 * @author Jared Allard <jaredallard@outlook.com>
 * @license MIT
 * @version 1
 */

const debug  = require('debug')('media:converter')
const Config = require('triton-core/config')
const dyn    = require('triton-core/dynamics')
const kue    = require('kue')
const queue  = kue.createQueue({
  redis: dyn('redis')
})


debug.log = console.log.bind(console)

debug('init', Date.now())

const init   = async () => {
  const config = await Config('converter')

  debug('eval-constraints')

  await require('./lib/main')(config, queue)
}

init()

const cleanup = (code = 0) => {
  debug('cleanup')
  queue.shutdown(1000, err => {
    debug('kue:shutdown', err)
    process.exit(code);
  });
}

process.on('SIGINT', () => {
  cleanup()
})

// Handle shutdown / reject
process.on('unhandledRejection', error => {
  debug('Uncaught Execption:', error)

  cleanup(1)
});
