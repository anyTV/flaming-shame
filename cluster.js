var cluster = require("cluster");

if (cluster.isMaster) {
    var numCPUs = require("os").cpus().length;
    while (numCPUs--) {
        cluster.fork();
    }
}
else {
	require(__dirname + '/blackwidow10');
}

cluster.on('exit', function (worker) {
	console.log('Someone died T_T');
	global.gc();
	require(__dirname + '/blackwidow10');
});
