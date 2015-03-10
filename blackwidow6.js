var curl = require(__dirname + '/../lib/curl'),
	cluster = require('cluster');

if (cluster.isMaster) {
	var cpuCount = require('os').cpus().length,
		regions = ['US', 'DZ', 'AR', 'AU', 'AT', 'BH', 'BE', 'BA', 'BR', 'BG', 'CA', 'CL', 'CO', 'HR', 'CZ', 'DK', 'EG', 'EE', 'FI', 'FR', 'DE', 'GH', 'GR', 'HK', 'HU', 'IN', 'ID', 'IE', 'IL', 'IT', 'JP', 'JO', 'KE', 'KW', 'LV', 'LB', 'LT', 'MK', 'MY', 'MX', 'ME', 'MA', 'NL', 'NZ', 'NG', 'NO', 'OM', 'PE', 'PH', 'PL', 'PT', 'QA', 'RO', 'RU', 'SA', 'SN', 'RS', 'SG', 'SK', 'SI', 'ZA', 'KR', 'ES', 'SE', 'CH', 'TW', 'TH', 'TN', 'TR', 'UG', 'UA', 'AE', 'GB', 'YE'],
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
		};
	curl.get
		.to('www.youtube.com', 443, '/channels')
		.raw()
		.secured()
		.send({gl : regions[~~(74 * Math.random())]})
		.then(function (status, data) {
			var match;
			if (status === 200) {
				match = data.match(/\ytid=\\\"(.{1,50})\"\\/gi);
				if (match) {
					split(match
						.filter(function (e, p, s) {
							return s.indexOf(e) === p;
						})
						.map(function (e) {
							return e.substring(7, e.length - 3);
						}), cpuCount)
						.forEach(function (a) {
							return cluster.fork({ channels : a.join(',') });
						});
				}
			}
		})
		.onerror(function (e) {
			console.dir(e);
		});
} else {
	var io = require('socket.io-client'),
		KEY = 'AIzaSyDqWOahd3OSYfctw5pTTcNjQjjfD3QC-s4',
		get_scm = function (a) {
			return curl.get
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
									return get_channel(e.substring(7, e.length - 3));
								});
						}
					}
					if (a.statistics) {
						delete a.first_video;
						client.emit('insert', a);
					}
					return;
				})
				.onerror(function () {
					if (a.statistics) {
						delete a.first_video;
						client.emit('insert', a);
					}
				});
		},

		get_username = function (a) {
			return curl.get
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
						return get_scm(a);
					} else {
						console.log(status, 'Error getting first video and username of channel', a._id);
					}
					return;
				})
				.onerror(function (e) {
					console.log('Error getting first video and username of channel', a._id);
				});
		},

		get_channel = function (a) {
			if (+new Date > start + (1000 * 60 * 60)) return process.exit();
			return curl.get
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
								return get_username(data);
							}
						};
					}
					else
						console.log(status, 'Error on getting channel', a);
					return;
				})
				.onerror(function (e) {
					console.log('Error on getting channel', a);
				});
		},

		start = +new Date;
	console.log('Releasing black widow', cluster.worker.id);
	// client = io.connect('http://ec2-54-214-176-172.us-west-2.compute.amazonaws.com:8002/');
	client = io.connect('http://192.168.2.182:8002/');
	process.env['channels'].split(',').forEach(get_channel);
}

cluster.on('exit', function (worker) {
	console.log('Black widow ' + worker.id + ' died :(');
});
