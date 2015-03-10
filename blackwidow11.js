'use strict';

var crawler = function () {
	var https = require('https'),
		cudl = require('cuddle'),
		config = require(__dirname + '/../config/config'),
		mysql = require(__dirname + '/../lib/mysql').open(config.crawler_cache_db),
		KEY = 'AIzaSyDqWOahd3OSYfctw5pTTcNjQjjfD3QC-s4',
		request_count = 0,
		down_time = 0,
		ids = [],
		max,
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
		get_network = function (a) {
			a.network = '';
			cudl.to('www.youtube.com', 443, '/watch')
				.raw()
				.secured()
				.send({v : a.first_video})
				.then(function (data) {
					var match = data.match(/\<meta name=(\")?attribution(\")?(\s*)content=(.{1,50})\>/gi);
					if (match && match[0]) {
						match = match[0].substring(31);
						a.network = match.substring(0, match.length - 2);
					}

					cudl.to(process.argv[2], 8002, '/insert')
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
							process.stdout.write('Inserted ' + a.username + '\t\t\t\r');
							down_time = 0;
						})
						.onerror(function () {
						});

					mysql.query(
							'INSERT INTO channels(id) VALUES(?)',
							[a._id],
							function (err) {
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
										function (_err, _result) {
											if (_err) {
												return stop(_err);
											}

											_result = _result.map(function (a) {
												return a.id;
											});
											_result = match.filter(function (a) {
												return !~_result.indexOf(a) && a.indexOf('U') === 0;
											});

											if (_result.length === 0) {
												return stop('Dead end');
											}

											_result.forEach(function (e) {
												if (request_count > max) {
													return;
												}
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
			cudl.to('www.googleapis.com', 443, '/youtube/v3/search')
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
				.onerror(function () {
					console.log('Error getting first video+username of channel', a._id, 'will retry');
					return get_username(a);
				});
		},

		get_channel = function (a) {

			if (request_count > max) {
				return;
			}

			request_count++;
			return cudl.to('www.googleapis.com', 443, '/youtube/v3/channels')
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
							data.date_crawled = +new Date();
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
				.onerror(function () {
					console.log('Error in getting channel', a, 'will retry');
					return get_channel(a);
				});
		},

		stop = function () {
			//console.log(msg);
			request_count--;
			if (request_count <= 0) {
				global.gc();
				setTimeout(start, 10);
				return;
			}
		},
		start = function () {
			var i = 50,
				temp;

			console.log('Querying 50 random channels as seed');
			console.time('Random 50 channels');

			while (i--) {
				temp = ~~(Math.random() * 100000);
				mysql.query('select id from channels limit ?, 1', temp, store_seed);
			}
		},

		store_seed = function (err, result) {
			if (err) {
				return console.dir(err);
			}

			ids.push(result[0].id);
			if (ids.length === 50) {
				console.timeEnd('Random 50 channels');
				console.log('Done querying');
				ids.forEach(get_channel);
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

	https.globalAgent.maxSockets = Infinity;

	console.log('Server : http://' + process.argv[2]);
	console.log('MAX handles per thread : ' + max);

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
