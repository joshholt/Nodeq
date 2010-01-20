/*
  Node Rescue -- A port of Rescue to nodeJS
  Author = Josh Holt
  License = MIT
*/
NRDEBUG = false;
var sys = require('sys');
var tcp = require('tcp'); 
var redis = require('./lib/redisclient');
var helpers = require('./lib/helpers');
var client = new redis.Client();

exports.create = function(db, ns) {
  return new NodeResque(db, ns);
}

var NodeResque = exports.NodeResque = function(dbNumber, nameSpace) {
  this.dbNumber = dbNumber || 0;
  this.nameSpace = nameSpace || "NRQueue:";
  this.callbacks = [];
};
sys.inherits(NodeResque, process.EventEmitter);

NodeResque.prototype.push = function(queue,item) {
  var encodedItem = JSON.stringify(item), promise = new process.Promise();
  this.emit('push', queue, item);
  this.watch_queue(queue).addCallback(function(ret){
    client.rpush(queue,encodedItem).addCallback(function(ret){
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
  this.emit('pop',queue);
  client.lpop(queue).addCallback(function(ret) {
    promise.emitSuccess(ret);
  }).addErrback(function(e) {
    promise.emitError(e);
  });
  return promise;
};

NodeResque.prototype.size = function(queue) {
  var promise = new process.Promise();
  this.emit('size',queue);
  client.llen(queue).addCallback(function(ret) {
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
  this.emit('list_range',key,start,count);
  start = start || 0; count = count || 1;
  if (count === 1) {
   client.lindex(key,0).addCallback(function(ret) {
     // TODO: [JH2] use JSON.parse() here to return a real object...
     promise.emitSuccess(ret);
   }).addErrback(function(e) {
     promise.emitError(e);
   });
  }else{
    client.lrange(key,start,start+count-1).addCallback(function(ret) {
      // TODO: [JH2] forEach here and then JSON.parse() return an array
      promise.emitSuccess(ret);
    }).addErrback(function(e) {
      promise.emitError(e);
    });
  }
  return promise;
};

NodeResque.prototype.queues = function() {
  
};

NodeResque.prototype.remove_queue = function(queue) {
  
};

NodeResque.prototype.watch_queue = function(queue) {
  var that = this,promise = new process.Promise();
  this.emit('watch_queue', queue);
  // The actual redis call...
  client.sadd("resque:queues",queue).addCallback(function(ret){
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
  
};

NodeResque.prototype.reserve = function(queue) {
  
};

NodeResque.prototype.workers = function() {
  
};

NodeResque.prototype.working = function() {
  
};

NodeResque.prototype.info = function() {
  
};

NodeResque.prototype.keys = function() {
  
};

NodeResque.prototype.runner = function() {
  while(this.callbacks.length > 0) {
    this.callbacks.shift()();
  }
};
