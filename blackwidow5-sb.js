var curl = require(__dirname + '/../lib/curl'),
	cluster = require('cluster'),
	cpuCount = require('os').cpus().length,
	mongo = require('mongoskin'),
	db = mongo.db('mongodb://localhost:27017/crawled', {native_parser:true}),
	collection = db.collection('channels', {strict: true}),
	KEY = 'AIzaSyDqWOahd3OSYfctw5pTTcNjQjjfD3QC-s4',
	channels = {},

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
						match
							.filter(function (e, p, s) {
								return s.indexOf(e) === p;
							})
							.forEach(function (e) {
								get_channel(e.substring(7, e.length - 3));
								return;
							});
					}
				}
				if (a.statistics) {
					delete a.first_video;
					collection.insert(a, function (err) {
						if (err && err.code === 11000) return console.log('duplicate');
						if (err) return console.log(err);
						console.log('Inserted ' + a.username);
					});
				}
			})
			.onerror(function () {
				if (a.statistics) {
					delete a.first_video;
					collection.insert(a, function (err) {
						if (err && err.code === 11000) return console.log('duplicate');
						if (err) return console.log(err);
						console.log('Inserted ' + a.username);
					});
				}
			});
		return;
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
		return;
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
		return;
	}
	;

if (cluster.isMaster) {
	curl.get
		.to('socialblade.com', 80, '/youtube/top/5000')
		.raw()
		.then(function (status, data) {
			var match;
			console.dir(data);
			if (status === 200) {
				match = data.match(/\/user\/(.{1,50})\"/gi);
				console.dir(match);
				if (match) {
					split(match
						.filter(function (e, p, s) {
							return s.indexOf(e) === p;
						})
						.map(function (e) {
							console.log(e);
							return e.substring(7, e.length - 3);
						}), cpuCount)
						// .forEach(function (a) {
							// cluster.fork({ channels : a.join(',') });
							// return;
						// });
				}
			}
		})
		.onerror(function (e) {
			console.dir(e);
		});
} else {
	console.log('Releasing black widow', cluster.worker.id);
	channels[cluster.worker.id] = process.env['channels'];
	process.env['channels'].split(',').forEach(get_channel);
}

cluster.on('exit', function (worker) {
	console.log('Black widow ' + worker.id + ' died :( Reviving...');
	cluster.fork({channels : channels[cluster.worker.id]});
});
