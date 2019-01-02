/**
 * Media post-processor. Determines if we need to convert or not.
 *
 * @author Jared Allard <jaredallard@outlook.com>
 * @license MIT
 * @version 1
 */

const _ = require('lodash')
const fs = require('fs-extra')
const probe = require('node-ffprobe')
const path = require('path')
const klaw = require('klaw')
const async = require('async')

const { error, Tags } = require('triton-core/tracer')

const mediaExts = [
  '.mp4',
  '.mkv',
  '.mov',
  '.webm'
]

const findMediaFiles = async absPath => {
  return new Promise((resolve, reject) => {
    const files = []
    klaw(absPath, {
      filter: item => {
        if (fs.statSync(item).isDirectory()) {
          // Only process folders that seem like they might contain our content
          if (item.indexOf('Season')) {
            return true
          }

          return false
        }

        const ext = path.extname(item)
        if (mediaExts.indexOf(ext) !== -1) return true
        return false // filter non-media files.
      }
    })
      .on('data', item => {
        const stat = fs.statSync(item.path)
        if (stat.isDirectory()) return // skip
        files.push(item.path)
      })
      .on('end', () => {
        return resolve(files)
      })
      .on('error', err => {
        return reject(err)
      })
  })
}

module.exports = async (config, queue, emitter, logger, spanFactory) => {
  const audioCodec = config.instance.settings.audio.codec
  const videoCodec = config.instance.settings.video.codec

  // determine if we need to process or not.
  return async job => {
    const span = await spanFactory()
    const file = job.data

    span.setTag(Tags.CARD_ID, job.id)
    logger.info('processing directory', file.path)

    const listOfFiles = await findMediaFiles(file.path)
    
    return new Promise((resolve, reject) => {
      async.mapLimit(listOfFiles, 5, (media, next) => {
        probe(media, (err, stats) => {
          if (err) return next(err)
  
          const audioStreams = _.filter(stats.streams, audio => {
            if (audio.codec_type !== 'audio') return false
            if (audio.codec_name === audioCodec) return false
            return true
          })
  
          const videoStreams = _.filter(stats.streams, video => {
            if (video.codec_type !== 'video') return false
            if (video.codec_name === videoCodec) return false
            return true
          })
  
          return next(null, {
            path: media,
            audio: audioStreams,
            video: videoStreams
          })
        })
      }, (err, results) => {
        if (err) {
          return reject(err)
        }
  
        logger.info('found', results.length, 'media files')
        span.setTag('media.files', results.length)
        return resolve({
          media: results
        })
      })
    })
  }
}
