// ..........................................................
// TEST SETUP
// 
NRDEBUG = true;
process.mixin(GLOBAL, require('ntest'));
var sys = require('sys'),
    redisclient = require('./lib/redisclient'),
    queue = require('./node_resque').create(0,"resque"),
    assert = require('assert'),
    redis = new redisclient.Client();

// ..........................................................
// STUFF THAT SHOULD BE IN SETUP/TEARDOWN
// 
var nodeq = "nodeq",
    nodeqJob = {"class": "Demo::Job", 
                args:[{message:"This is test queue item(1)"}]},
    nodeqJob1 = {"class": "Demo::Job", 
                 args:[{message:"This is test queue item(2)"}]};
//Clears out the DB.
//redis.flushdb().wait();


// ..........................................................
// QUEUE MANIPULATION
// 
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
  it("should allow jobs to be popped off of the queue",function() {
    assert.equal(JSON.stringify(nodeqJob),queue.pop(nodeq).wait());
    assert.equal(true,queue.push(nodeq,nodeqJob).wait());
  })
  it("should allow range peeking",function() {
    assert.equal(JSON.stringify(nodeqJob),queue.peek(nodeq,0,-1).wait()); 
  })
  it("should have one item after pushing job onto queue",function() {
    assert.equal(1,queue.size(nodeq).wait());
  })
  it("should allow multiple jobs to be pushed onto the queue", function(){
    assert.equal(true,queue.push(nodeq,nodeqJob).wait());
    assert.equal(true,queue.push(nodeq,nodeqJob1).wait());
  })
  it("should have three jobs after two more pushes",function() {
    assert.equal(3,queue.size(nodeq).wait());
  })
  it("should have two jobs after a pop",function() {
    assert.equal(JSON.stringify(nodeqJob),queue.pop(nodeq).wait());
    assert.equal(2,queue.size(nodeq).wait());
  })
  it("should allow range fetching",function() {
    assert.equal(JSON.stringify(nodeqJob),queue.list_range(nodeq,0,-1).wait()); 
  })
  it("should have one job left after pop", function() {
    assert.equal(JSON.stringify(nodeqJob),queue.pop(nodeq).wait());
    assert.equal(1,queue.size(nodeq).wait());
  })
  it("should allow different jobs to be in the queue",function() {
    assert.equal(JSON.stringify(nodeqJob1),queue.pop(nodeq).wait());
  })
  it("should have a size of zero after popping the last item off of the queue",function() {
    assert.equal(0,queue.size(nodeq).wait());
  })

// ..........................................................
// QUEUE METADATA
// 
describe("When asking for the metadata of a Queue")
  it("should provide and array of keys", function() {
   assert.equal(true,_.isArray(queue.keys().wait()));
  })

// ..........................................................
// JUST TO WATCH THE WORKER WORK
// 
queue.push(nodeq,nodeqJob).wait();
queue.push(nodeq,nodeqJob1).wait();

// ..........................................................
// ENSURES THAT WE EXIT
// 
process.exit();