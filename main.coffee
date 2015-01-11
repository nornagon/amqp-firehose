util = require 'util'
amqp = require 'amqp'
request = require 'request'

for k in 'RABBIT_HOST RABBIT_LOGIN RABBIT_PASSWORD'.split(/\x20/)
  global[k] = process.env[k] or throw new Error "need #{k}"
RABBIT_HEARTBEAT = parseInt(process.env.RABBIT_HEARTBEAT ? 10)


rabbit_config =
  host: RABBIT_HOST
  port: 5673
  ssl: {enabled: true}
  login: RABBIT_LOGIN
  password: RABBIT_PASSWORD
  heartbeat: RABBIT_HEARTBEAT

conn = amqp.createConnection rabbit_config

conn.on 'ready', ->
  queue_name = 'alerts'
  util.debug 'connected'
  conn.queue '', autoDelete: true, (q) ->
    q.bind 'amq.rabbitmq.trace', '#'
    util.debug 'bound'
    q.subscribeRaw {noAck: true}, (m) ->
      time = new Date
      buffer = new Buffer(m.size)
      bytes_used = 0
      m.addListener 'data', (d) ->
        d.copy buffer, buffer.used
        bytes_used += d.length
      m.addListener 'end', ->
        console.log JSON.stringify {
          routingKey: m.routingKey
          headers: m.headers
          data: buffer.toString()
          timestamp: time
        }
  .on 'error', (e) ->
    util.debug "[#{new Date}] queue error: "+util.inspect e

conn.on 'error', (e) ->
  util.debug "[#{new Date}] connection 'error' received: "+util.inspect e

conn.on 'close', (e) ->
  util.debug "[#{new Date}] connection 'close' received: "+util.inspect e
