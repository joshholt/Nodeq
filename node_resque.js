/*
  Node Rescue -- A port of Rescue to nodeJS
  Author = Josh Holt
  License = MIT
*/

/*globals NRDEBUG NodeResque*/

NRDEBUG = false;
var sys = require('sys');
var tcp = require('tcp'); 
var redis = require('./lib/redisclient');
var helpers = require('./lib/helpers');
var client = new redis.Client();
require('underscore');

exports.create = function(db, ns) {
  return new NodeResque(db, ns);
};

var NodeResque = exports.NodeResque = function(dbNumber, nameSpace) {
  this.dbNumber = dbNumber || 0;
  this.ns = nameSpace || "NRQueue:";
  this.callbacks = [];
};
sys.inherits(NodeResque, process.EventEmitter);

NodeResque.prototype.push = function(queue,item) {
  var encodedItem = JSON.stringify(item), promise = new process.Promise();
  var ns = this.ns + ":queue:";
  this.emit('push', queue, item);
  this.watch_queue(queue).addCallback(function(ret){
    client.rpush(ns+queue,encodedItem).addCallback(function(ret){
        promise.emitSuccess(ret);
    }).addErrback(function(ret){
      promise.emitError(ret);
    })
  }).addErrback(function(ret){
    promise.emitError(ret);
  });
  return promise;
  
};

NodeResque.prototype.pop = function(queue) {
  var promise = new process.Promise();
  var ns = this.ns + ":queue:";
  this.emit('pop',queue);
  client.lpop(ns+queue).addCallback(function(ret) {
    promise.emitSuccess(ret);
  }).addErrback(function(e) {
    promise.emitError(e);
  });
  return promise;
};

NodeResque.prototype.size = function(queue) {
  var promise = new process.Promise();
  var ns = this.ns + ":queue:";
  this.emit('size',queue);
  client.llen(ns+queue).addCallback(function(ret) {
    promise.emitSuccess(ret);
  }).addErrback(function(e) {
    promise.emitError(e);
  });
  return promise;
};

NodeResque.prototype.peek = function(queue, start, count) {
  var promise = new process.Promise();
  this.emit('peek',queue,start,count);
  start = start || 0; count = count || 1;
  this.list_range(queue,start,count).addCallback(function(ret) {
    promise.emitSuccess(ret);
  }).addErrback(function(e) {
    promise.emitError(e);
  });
  return promise;
};

NodeResque.prototype.list_range = function(key, start, count) {
  var promise = new process.Promise();
  var ns = this.ns + ":queue:";
  this.emit('list_range',key,start,count);
  start = start || 0; count = count || 1;
  if (count === 1) {
   client.lindex(ns+key,0).addCallback(function(ret) {
     // TODO: [JH2] use JSON.parse() here to return a real object...
     promise.emitSuccess(ret);
   }).addErrback(function(e) {
     promise.emitError(e);
   });
  }else{
    client.lrange(ns+key,start,start+count-1).addCallback(function(ret) {
      // TODO: [JH2] forEach here and then JSON.parse() return an array
      promise.emitSuccess(ret);
    }).addErrback(function(e) {
      promise.emitError(e);
    });
  }
  return promise;
};

NodeResque.prototype.queues = function() {
  var promise = new process.Promise();
  this.emit('queues');
  client.smembers(this.ns+':queues').addCallback(function(ret) {
    promise.emitSuccess(ret);
  }).addErrback(function(e) {
    promise.emitError(e);
  });
  return promise;
};

NodeResque.prototype.remove_queue = function(queue) {
  var that = this, promise = new process.Promise();
  this.emit('remove_queue', queue);
  client.srem(this.ns+":queues",queue).addCallback(function(ret) {
    client.del(this.ns+":queue:"+queue).addCallback(function(ret) {
      promise.emitSuccess(ret);
    }).addErrback(function(e) {
      promise.addErrback(e);
    })
  }).addErrback(function(e) {
    promise.emitError(e);
  });
  return promise;
};

NodeResque.prototype.watch_queue = function(queue) {
  var that = this,promise = new process.Promise();
  this.emit('watch_queue', queue);
  // The actual redis call...
  client.sadd(this.ns+":queues",queue).addCallback(function(ret){
    if (ret === 0 || ret === 1) {
      promise.emitSuccess(true);
    }else{
      promise.emitError("Problem watching -> "+queue);
    }
  }).addErrback(function(ret){
    sys.puts(ret);
  });
  return promise;
};

NodeResque.prototype.enqueue = function(klass, args) {
  var queue = klass.queue;
  if (queue) {
    return Job.create(queue,klass,args);
  }
};

NodeResque.prototype.reserve = function(queue) {
  return Job.reserve(queue);
};

NodeResque.prototype.workers = function() {
  return Worker.all;
};

NodeResque.prototype.working = function() {
  return Worker.working
};

NodeResque.prototype.info = function() {
  // return {
  //   pending: this.queues.addCallback(function(ret) {
  //     _.reduce(ret, 0, function(m,k){ return m + this.size(k).wait() });
  //   }).addErrback(function(e) {
  //     promise.emitError(e);
  //   }),
  //   processed: Stat[:processed],
  //   queues:    queues.size,
  //   workers:   workers.size,
  //   working:   working.size,
  //   failed:    Stat[:failed],
  //   servers:   [redis.server]
  // }
};

NodeResque.prototype.keys = function() {
  var that = this,promise = new process.Promise();
  this.emit('keys');
  client.keys('*').addCallback(function(ret) {
    promise.emitSuccess(_.map(ret,function(v){return v.replace('resque:','')}));
  }).addErrback(function(e) {
    promise.emitError(e);
  });
  return promise;
};