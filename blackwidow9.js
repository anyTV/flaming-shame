var cheerio = require('cheerio'),
	cluster = require('cluster'),
	config = require(__dirname + '/../config/config'),
	mysql = require(__dirname + '/../lib/mysql'),
    https = require('https'),
    http = require('http');

var cleanArray = function (input) {
		var ret = [],
			i = input.length;

		while (i--)
			if (input.indexOf(input[i]) === i)
				ret.push(input[i].substring(7, input[i].length - 3));

		return ret;

	},
	stringify = function (obj) {
		var ret = [],
			key;
		for (key in obj)
			ret.push(encodeURIComponent(key) + '=' + encodeURIComponent(obj[key]));
		return ret.join('&');
	},
	Request = function () {
		this.secure = false;
		this._raw = false;
		this.to = function (host, port, path) {
			this.host = host;
			this.port = port;
			this.path = path;
			return this;
		};
		this.secured = function () {
			this.secure = true;
			return this;
		};
		this.raw = function () {
			this._raw = true;
			return this;
		};
		this.then = function (cb) {
			this.scb = cb;
			return this;
		};
		this.onerror = function (cb) {
			this.ecb = cb;
			return this;
		};
		this.finally = function (cb) {
			this.fcb = cb;
			return this;
		};
		this.send = function (data) {
			var self = this,
				protocol,
				req;

			this.path += '?' + stringify(data);

			protocol = this.secure ? https : http;

			req = protocol.request({
				host: this.host,
				port: this.port,
				path: this.path,
				method: 'GET'
			}, function (response) {
				var s = '';
				response.setEncoding('utf8');
				response.on('data', function (chunk) {
					s += chunk;
				});
				response.on('end', function () {
					try {
						JSON.parse(s);
					} catch (e) {
						s = JSON.stringify({data : s});
					}
					self.scb(response.statusCode, self._raw ? s : JSON.parse(s));
					self.fcb && self.fcb();
				});
			});

			req.on('error', function (err) {
				self.ecb(err);
				self.fcb && self.fcb();
			});

			req.end();
			return this;
		};
	};

if (cluster.isMaster) {
	(function () {
		var cpuCount = require('os').cpus().length,
			region = ['US', 'DZ', 'AR', 'AU', 'AT', 'BH', 'BE', 'BA', 'BR', 'BG', 'CA', 'CL', 'CO', 'HR', 'CZ', 'DK', 'EG', 'EE', 'FI', 'FR', 'DE', 'GH', 'GR', 'HK', 'HU', 'IN', 'ID', 'IE', 'IL', 'IT', 'JP', 'JO', 'KE', 'KW', 'LV', 'LB', 'LT', 'MK', 'MY', 'MX', 'ME', 'MA', 'NL', 'NZ', 'NG', 'NO', 'OM', 'PE', 'PH', 'PL', 'PT', 'QA', 'RO', 'RU', 'SA', 'SN', 'RS', 'SG', 'SK', 'SI', 'ZA', 'KR', 'ES', 'SE', 'CH', 'TW', 'TH', 'TN', 'TR', 'UG', 'UA', 'AE', 'GB', 'YE'][~~(74 * Math.random())],
			split = function (a, n) {
				var len = a.length,
					out = [],
					i = 0,
					size;
				console.log('Found ' + a.length + ' unique channels');
				while (i < len) {
					size = Math.ceil((len - i) / n--);
					out.push(a.slice(i, i += size));
				}
				return out;
			};

		if (!process.argv[2]) {
			return console.log('Server IP is missing');
		}

		if (!process.argv[3]) {
			console.log('2nd paramater : MAX handles per thread is missing. Defaulting to 200');
			process.argv[3] = 200;
		}
		else {
			process.argv[3] = +process.argv[3];
		}

		console.log('Server : http://' + process.argv[2]);
		console.log('Region : ' + region);
		console.log('CPU count : ' + cpuCount);
		console.log('MAX handles per thread : ' + process.argv[3]);

		new Request()
			.to('www.youtube.com', 443, '/channels')
			.raw()
			.secured()
			.send({gl : region})
			.then(function (status, data) {
				var match;
				if (status === 200) {
					match = data.match(/\ytid=\\\"(.{1,50})\"\\/gi);
					if (match) {
						match = cleanArray(match);
						mysql.open(config.crawler_cache_db)
							.query('SELECT id FROM channels WHERE id IN (?)', [match], function (err, result) {
								if (err) return console.dir(err);
								result = result.map(function (a) {
									return a.id;
								});
								// split(, cpuCount)
								//.forEach(function (a) {
								return cluster.fork({
										channels : match.filter(function (a) {
														return !~result.indexOf(a) && a.indexOf('U') === 0;
													}).join(','),
										url : process.argv[2],
										max : process.argv[3]
									});
								//});
							})
							.end();
						return;
					}
				}
			})
			.onerror(console.dir);
	}());
} else {
	(function () {
		var KEY = 'AIzaSyDqWOahd3OSYfctw5pTTcNjQjjfD3QC-s4',
			max,
			request_count = 0,
			cache = [],
			use_cache = function () {
				var i = cache.length;
				while (i--)
					get_channel(cache.shift());
			},
			get_network = function (a) {
				a.network = null;
				return new Request()
					.to('www.youtube.com', 443, '/watch')
					.raw()
					.secured()
					.send({v : a.first_video})
					.then(function (status, data) {
						var match;
						request_count--;
						if (status === 200) {
							match = data.match(/\<meta name=(\")?attribution(\")?(\s*)content=(.{1,50})\>/gi);
							if (match && match[0]) {
								match = match[0].substring(31);
								a.network = match.substring(0, match.length - 2);
							}
							match = data.match(/\ytid=\\\"(.{1,50})\"\\/gi);
							if (match) {
								match = cleanArray(match);


								mysql.open(config.crawler_cache_db)
									.query('SELECT id FROM channels WHERE id IN (?)', [match], function (err, result) {
										if (err) return console.dir(err);
										result = result.map(function (a) {
											return a.id;
										});
										match.filter(function (a) {
											return !~result.indexOf(a) && a.indexOf('U') === 0;
										})
										.forEach(function (e) {
											if (request_count > max)
												return;
											return get_channel(e);
										});
									})
									.end();
							}
						}
						if (a.statistics) {
							delete a.first_video;
							new Request()
								.to(process.env['url'], 8002, '/insert')
								.send({
									u : a.username,
									i : a._id,
									v : a.statistics.viewCount,
									s : a.statistics.subscriberCount,
									n : a.network
								})
								.then(function () {
									mysql.open(config.crawler_cache_db)
										.query('INSERT INTO channels(id) VALUES(?)', [a._id], function (err, result) {
											if (err) {
												console.dir(err);
											}
										})
										.end();
								})
								.onerror(function (err) {
									console.dir(err);
								});
						}
						return;
					})
					.onerror(function () {
						if (a.statistics) {
							delete a.first_video;
							new Request()
								.to(process.env['url'], 8002, '/insert')
								.send({
									u : a.username,
									i : a._id,
									v : a.statistics.viewCount,
									s : a.statistics.subscriberCount,
									n : a.network
								})
								.then(function () {
									mysql.open(config.crawler_cache_db)
										.query('INSERT INTO channels(id) VALUES(?)', [a._id], function (err, result) {
											if (err) {
												console.log('er');
												console.log(a.username);
												console.dir(err);
											}
										})
										.end();
								})
								.onerror(function (err) {
									console.dir(err);
								});
						}
						request_count--;
					});
			},

			get_username = function (a) {
				return new Request()
					.to('www.googleapis.com', 443, '/youtube/v3/search')
					.secured()
					.send({
						part : 'snippet',
						channelId : a._id,
						type : 'video',
						maxResults : 1,
						fields : 'items(snippet/channelTitle, id/videoId)',
						order : 'date',
						key : KEY
					})
					.then(function (status, data) {
						if (status === 200) {
							if (data.items[0] && data.items[0].id && !(a.first_video = data.items[0].id.videoId)) {
								request_count--;
								return;
							}
							if (data.items[0] && data.items[0].snippet && !(a.username = data.items[0].snippet.channelTitle)) {
								request_count--;
								return;
							}
							return get_network(a);
						} else {
							console.log(status, 'Error getting first video and username of channel', a._id);
							request_count--;
						}
						return;
					})
					.onerror(function (e) {
						request_count--;
						console.dir(e);
					});
			},

			get_channel = function (a) {
				
				// if (new Date().getHours() >= 10
				// 	&& new Date().getDay() !== 0
				// 	&& new Date().getDay() !== 6){
				// 	console.log('Time is up, putting channel on cache');
				// 	return cache.push(a);
				// }
				
				if (request_count > max)
					return;

				request_count++;
				return new Request()
					.to('www.googleapis.com', 443, '/youtube/v3/channels')
					.secured()
					.send({
						part : 'id, statistics',
						id : a,
						fields : 'items(statistics/viewCount,statistics/subscriberCount,statistics/videoCount)',
						key : KEY
					})
					.then(function (status, data) {
						if (status === 200) {
							if (data.items && data.items[0]) {
								data = data.items[0];
								if (+data.statistics.videoCount > 0) {
									data.date_crawled = +new Date;
									data._id = a;
									delete data.statistics.videoCount;
									return get_username(data);
								} else {
									request_count--;
								}
							} else {
								request_count--;
							}
						}
						else {
							console.log(status, 'Error on getting channel', a);
							request_count--;
						}
						return;
					})
					.onerror(function (e) {
						request_count--;
						console.dir(e);
					});
			};

		console.log('Releasing black widow', cluster.worker.id);
		max = +process.env['max'];
		process.env['channels'].split(',').forEach(get_channel);
		
		// setInterval(function () {
		// 	if ((new Date().getHours() <= 10 || (new Date().getDay() === 0 || new Date().getDay() === 6)) && cache.length > 0) {
		// 		use_cache();
		// 		console.log('Continuing...');
		// 	}
		// }, 1000/*  * 60 * 10 */); //10minutes
	} ());
}

cluster.on('exit', function (worker) {
	console.log('Black widow ' + worker.id + ' died :(');
});



