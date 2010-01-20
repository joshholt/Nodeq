NRDEBUG = true;
process.mixin(GLOBAL, require('ntest'));
var redisclient = require('./lib/redisclient');
var queue = require('./node_resque').create();
var assert = require('assert');
var redis = new redisclient.Client();

// Used in tests
var nodeq = "resque:queue:nodeq";
var nodeqJob = {message: "This is a test queue item", actor: "Mailer"};

redis.flushdb().wait();

describe("When Manipulating a Queue")
  it("should start watching the queue for jobs",function(){
    assert.equal(true,queue.watch_queue(nodeq).wait())
  })
  it("should have an initial size of zero",function() {
    assert.equal(0,queue.size(nodeq).wait());
  })
  it("should allow jobs to be pushed onto the queue", function(){
    assert.equal(true,queue.push(nodeq,nodeqJob).wait());
  })
  it("should have one item after pushing job onto queue",function() {
    assert.equal(1,queue.size(nodeq).wait());
  })
  it("should allow jobs to be popped off of the queue",function() {
    assert.equal(JSON.stringify(nodeqJob),queue.pop(nodeq).wait());
  })
  it("should have a size of zero after popping the last item off of the queue",function() {
    assert.equal(0,queue.size(nodeq).wait());
  })
  
process.exit();