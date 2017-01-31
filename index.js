#!/usr/bin/env node

var amqp = require('amqplib/callback_api');
var elasticsearch = require('elasticsearch');

var client = new elasticsearch.Client({
  host: 'localhost:9200',
  log: 'trace'
});

amqp.connect('amqp://localhost', function(err, conn) {
  conn.createChannel(function(err, ch) {
    var q = 'ErrorLogInput';

    ch.assertQueue(q, {durable: false});
    console.log(" [*] Waiting for messages in %s. To exit press CTRL+C", q);
    ch.consume(q, function(msg) {

      var error = JSON.parse(msg.content.toString())
      client.search({
        index: 'error_log',
        type: 'error',
        body: {
          query: {
            match_phrase: {
              Exception: error.Exception
            }
          }
        }
      }).then(
        function (resp) {
          var hits = resp.hits.hits;

          if (resp.hits.total == 0) {
            client.index({
              index: 'error_log',
              type: 'error',
              body: JSON.stringify(error)
            },function(err,resp,status) {
              console.log(resp);
            });
          }
        },
        function (err) {console.trace(err.message);}
      );

      //console.log(" [x] Received %s", error.Exception);
      //console.log(" [x] Received %s", msg.content.toString());
    }, {noAck: true});
  });
});
