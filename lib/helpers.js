var sys = require('sys');
var helpers = exports;

helpers.write_debug = function(data) {
  if (!NRDEBUG || !data) return;
  sys.puts(data.replace(/\r\n/g, '<CRLF>'));
}