var readLine = require ("readline"),
	crawler = function () {
		var cheerio = require('cheerio'),
		    https = require('https'),
		    http = require('http'),
			config = require(__dirname + '/../config/config'),
			mysql = require(__dirname + '/../lib/mysql').open(config.crawler_cache_db),
			region,
			KEY = 'AIzaSyDqWOahd3OSYfctw5pTTcNjQjjfD3QC-s4',
			request_count = 0,
			cache = [],
			down_time = 0,
			max,
			regions = ['US', 'DZ', 'AR', 'AU', 'AT', 'BH', 'BE', 'BA', 'BR', 'BG', 'CA', 'CL', 'CO',
			'HR', 'CZ', 'DK', 'EG', 'EE', 'FI', 'FR', 'DE', 'GH', 'GR', 'HK', 'HU', 'IN', 'ID', 'IE',
			'IL', 'IT', 'JP', 'JO', 'KE', 'KW', 'LV', 'LB', 'LT', 'MK', 'MY', 'MX', 'ME', 'MA', 'NL',
			'NZ', 'NG', 'NO', 'OM', 'PE', 'PH', 'PL', 'PT', 'QA', 'RO', 'RU', 'SA', 'SN', 'RS', 'SG',
			'SK', 'SI', 'ZA', 'KR', 'ES', 'SE', 'CH', 'TW', 'TH', 'TN', 'TR', 'UG', 'UA', 'AE', 'GB',
			'YE'],
		    cleanArray = function (input) {
				var ret = [],
					i = input.length,
					a;

				while (i--) {
					a = input[i].substring(7, input[i].length - 3);
					if (input.indexOf(input[i]) === i && a[0] === 'U' && a.length === 24) {
						ret.push(a);
					}
				}

				return ret;
			},
			stringify = function (obj) {
				var ret = [],
					key;
				for (key in obj)
					ret.push(encodeURIComponent(key) + '=' + encodeURIComponent(obj[key]));
				return ret.join('&');
			},
			Request = function (method) {
				this.method = method || 'GET';
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

					data = stringify(data);

					if (this.method === 'GET') {
						this.path += '?' + data;
					}

					protocol = this.secure ? https : http;

					req = protocol.request({
						host: this.host,
						port: this.port,
						path: this.path,
						method: this.method
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
							if (response.statusCode === 200) {
								self.scb(self._raw ? s : JSON.parse(s));
							}
							else {
								self.ecb();
							}
							self.fcb && self.fcb();
						});
					});

					req.on('error', function (err) {
						self.ecb(err);
						self.fcb && self.fcb();
					});

					req.end(this.method === 'POST' ? data : null);
					return this;
				};
			},
			use_cache = function () {
				var i = cache.length;
				while (i--) {
					get_channel(cache.shift());
				}
			},
			get_random_region = function () {
				if (regions.length === 0) {
					return false;
				}
				return regions.splice(~~(regions.length * Math.random()), 1);
			},
			get_network = function (a) {
				a.network = '';
				return new Request()
					.to('www.youtube.com', 443, '/watch')
					.raw()
					.secured()
					.send({v : a.first_video})
					.then(function (data) {
						var match = data
							.match(/\<meta name=(\")?attribution(\")?(\s*)content=(.{1,50})\>/gi);
						if (match && match[0]) {
							match = match[0].substring(31);
							a.network = match.substring(0, match.length - 2);
						}
						delete a.first_video;
						new Request('POST')
							.to(process.argv[2], 8002, '/insert')
							.send({
								u : a.username,
								i : a._id,
								v : a.statistics.viewCount,
								s : a.statistics.subscriberCount,
								n : a.network,
								d : a.snippet.description,
								l : a.last_video_published_at
							})
							.then(function () {
								//console.log('Inserted', a.username);
								process.stdout.write('Inserted ' + a.username + '\t\t\t\033[0G');
								down_time = 0;
							})
							.onerror(function (err) {
							});

						mysql.query(
								'INSERT INTO channels(id) VALUES(?)',
								[a._id],
								function (err, result) {
									if (err && err.errno === 1062) {
										//console.log('Duplicate');
									}
									else if (err)  {
										console.dir(err);
									}
									match = data.match(/\ytid=\\\"(.{1,50})\"\\/gi);
									if (match) {
										match = cleanArray(match);
										mysql.query(
											'SELECT id FROM channels WHERE id IN (?)',
											[match],
											function (err, result) {
												if (err) return stop(err);
												result = result.map(function (a) {
													return a.id;
												});
												result = match.filter(function (a) {
													return !~result.indexOf(a) && a.indexOf('U') === 0;
												});
												if (result.length === 0) {
													return stop('Dead end');
												}
												result.forEach(function (e) {
													if (request_count > max)
														return;
													return get_channel(e);
												});
												return stop();
											});
									}
								});
					})
					.onerror(function () {
						if (typeof a.first_video === 'undefined') {
							return stop('Undefined first video');
						}
						console.log('ERROR GETTING VIDEO ' + a.first_video, 'will retry');
						return get_network(a);
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
						fields : 'items(snippet/channelTitle, id/videoId, snippet/publishedAt)',
						order : 'date',
						key : KEY
					})
					.then(function (data) {
						if (data.items.length === 0) {
							return stop('Channel ot found using search');
						}
						data = data.items[0];
						if (!data.id || !data.items[0].id.videoId) {
							return stop('VideoId not found');
						}
						if (!data.snippet || !data.items[0].snippet.channelTitle) {
							return stop('channelTitle not found');
						}
						a.username = data.snippet.channelTitle;
						a.first_video = data.id.videoId;
						a.last_video_published_at = ~~(new Date(data.snippet.publishedAt) / 1000);
						return get_network(a);
					})
					.onerror(function (e) {
						console.log('Error getting first video+username of channel', a._id, 'will retry');
						return get_username(a);
					});
			},

			get_channel = function (a) {

				if (request_count > max)
					return;

				request_count++;
				return new Request()
					.to('www.googleapis.com', 443, '/youtube/v3/channels')
					.secured()
					.send({
						part : 'id, statistics, snippet',
						id : a,
						fields : ['items(',
								'statistics/viewCount,',
								'statistics/subscriberCount,',
								'statistics/videoCount,',
								'snippet/description',
							')'].join(''),
						key : KEY
					})
					.then(function (data) {
						if (data.items && data.items[0]) {
							data = data.items[0];
							if (+data.statistics.videoCount > 0) {
								data.date_crawled = +new Date;
								data._id = a;
								delete data.statistics.videoCount;
								return get_username(data);
							} else {
								return stop('Videocount is 0');
							}
						} else {
							return stop('Channel not found');
						}
					})
					.onerror(function (e) {
						console.log('Error in getting channel', a, 'will retry');
						return get_channel(a);
					});
			},
			stop = function (msg) {
				//console.log(msg);
				request_count--;
				if (request_count <= 0) {
					global.gc();
					setTimeout(start, 10);
					return;
				}
			},
			start = function () {
				var ids = [],
					i = 50,
					temp;

				console.log('Querying 50 random channels as seed');
				console.time('Random 50 channels');

				while (i--) {
					temp = ~~(Math.random() * 100000);
					mysql.query('select id from channels limit ?, 1', temp, function (err, result) {
						if (err) {
							return console.dir(err);
						}
						ids.push(result[0].id);
						if (ids.length === 50) {
							console.timeEnd('Random 50 channels');
							console.log('Done querying');
							ids.forEach(get_channel);
						}
					});
				}
			};

		console.time('Up time');

		if (!process.argv[2]) {
			return console.log('Server IP is missing');
		}

		if (!process.argv[3]) {
			console.log('2nd paramater : MAX request per process is missing. Defaulting to 200');
		}

		max = process.argv[3] = +(process.argv[3] || 200);

		http.globalAgent.maxSockets = 10;
		https.globalAgent.maxSockets = 10;

		console.log('Server : http://' + process.argv[2]);
		console.log('MAX handles per thread : ' + max);


		if (process.platform === 'win32'){
		    readLine.createInterface({
		        input: process.stdin,
		        output: process.stdout
		    }).on('SIGINT', function () {
		        process.emit('SIGINT');
		    });
		}

		// process.on('uncaughtException', function (err) {
		// 	console.log('Caught exception: ' + err);
  //       	process.emit('SIGINT');
		// });

		process.on('SIGINT', function(){
			console.timeEnd('Up time');
			mysql.end();
			process.exit();
		});

		setInterval(function () {
			down_time++;
			if (down_time >= 60) {
				console.log('Been doing nothing for a minute');
        		process.emit('SIGINT');
			}
		}, 1000);

		start();
	};

module.exports = crawler();
