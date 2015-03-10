var curl = require(__dirname + '/../lib/curl'),
	cluster = require('cluster'),
	cpuCount = require('os').cpus().length,
	mongo = require('mongoskin'),
	db = mongo.db('mongodb://localhost:27017/crawled', {native_parser:true}),
	collection = db.collection('channels', {strict: true}),
	cache = db.collection('cache', {strict: true}),
	categories = db.collection('categories', {strict: true}),
	KEY = 'AIzaSyDqWOahd3OSYfctw5pTTcNjQjjfD3QC-s4',
	channels = [],
	count = 1,

	split = function (a, n) {
		var len = a.length,
			out = [],
			i = 0,
			size;
		while (i < len) {
			size = Math.ceil((len - i) / n--);
			out.push(a.slice(i, i += size));
		}
		return out;
	},

	get_scm = function (a) {
		curl.get
			.to('www.youtube.com', 443, '/watch')
			.raw()
			.secured()
			.send({v : a.first_video})
			.then(function (status, data) {
				var match;
				if (status === 200) {
					match = data.match(/\<meta name=(\")?attribution(\")?(\s*)content=(.{1,50})\>/gi);
					if (match && match[0]) {
						match = match[0].substring(31);
						a.scm = match.substring(0, match.length - 2);
					}
					match = data.match(/\ytid=\\\"(.{1,50})\"\\/gi);
					if (match) {
						match = match
							.filter(function (e, p, s) {
								return s.indexOf(e) === p;
							})
							.map(function (e) {
								return e.substring(7, e.length - 3);
							});
						if (cluster.isMaster) {
							split(match, cpuCount)
								.forEach(function (a) {
									console.log('forking with ' + a.join(','));
									cluster.fork({ channels : a.join(',') });
								});
						}
						else {
							match.forEach(get_channel);
						}
					}
				}
				if (a.statistics) {
					delete a.first_video;
					collection.insert(a, function (err) {
						if (err && err.code === 11000) return console.log('duplicate');
						if (err) return console.log(err);
						console.log(count++, 'Inserted ' + a.username);
					});
				}
			})
			.onerror(function () {
				if (a.statistics) {
					delete a.first_video;
					collection.insert(a, function (err) {
						if (err && err.code === 11000) return console.log('duplicate');
						if (err) return console.log(err);
						console.log(count++, 'Inserted ' + a.username);
					});
				}
			});
	},

	get_username = function (a) {
		curl.get
			.to('www.googleapis.com', 443, '/youtube/v3/search')
			.secured()
			.send({
				part : 'snippet',
				channelId : a._id,
				maxResults : 1,
				fields : 'items(id/videoId,snippet/channelTitle)',
				key : KEY
			})
			.then(function (status, data) {
				if (status === 200) {
					if (data.items[0] && data.items[0].id && !(a.first_video = data.items[0].id.videoId)) return;
					if (data.items[0] && data.items[0].snippet && !(a.username = data.items[0].snippet.channelTitle)) return;
					get_scm(a);
				} else {
					console.log(status, 'Error getting first video and username of channel', a._id);
				}
			})
			.onerror(function (e) {
				console.log('Error getting first video and username of channel', a._id);
			});
	},

	get_channel = function (a) {
		curl.get
			.to('www.googleapis.com', 443, '/youtube/v3/channels')
			.secured()
			.send({
				part : 'id, statistics',
				id : a,
				fields : 'items(statistics)',
				key : KEY
			})
			.then(function (status, data) {
				if (status === 200) {
					if (data.items && data.items[0]) {
						data = data.items[0];
						if (+data.statistics.videoCount > 0) {
							data.date_crawled = +new Date;
							data._id = a;
							data.scm = null;
							get_username(data);
						}
					};
				}
				else
					console.log(status, 'Error on getting channel', a);
			})
			.onerror(function (e) {
				console.log('Error on getting channel', a);
			});
	}
	;

if (cluster.isMaster) {
	get_scm({first_video : process.argv[2]});
} else {
	console.log('Deploying black widow ', cluster.worker.id);
	channels[cluster.worker.id] = process.env['channels'];
	process.env['channels'].split(',').forEach(get_channel);
}

cluster.on('exit', function (worker) {
	console.log('Black widow ' + worker.id + ' died :( Reviving...');
	cluster.fork({channels : channels[cluster.worker.id]});
});
