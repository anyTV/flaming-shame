var curl = require(__dirname + '/../lib/curl'),
	mongo = require('mongoskin'),
	cluster = require('cluster'),
	cpuCount = require('os').cpus().length,
	db = mongo.db('mongodb://localhost:27017/crawled', {native_parser:true}),
	collection = db.collection('channels', {strict: true}),
	KEY = 'AIzaSyDqWOahd3OSYfctw5pTTcNjQjjfD3QC-s4',
	regions = [],
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

	get_first_video = function (a) {
		curl.get
			.to('www.youtube.com', 443, '/watch')
			.raw()
			.secured()
			.send({v : a.first_video})
			.then(function (status, data) {
				var match;
				if (status === 200) {
					match = data.match(/\<meta name=(\")?attribution(\")?\s*content=(")?(.*)(")?\/\>/gi);
					if (match && match[0]) {
						match = match[0].substring(31);
						a.scm = match.substring(0, match.length - 2);
					}
				}
				delete a.first_video;
				collection.insert(a, function (err) {
					if (err) return;
					console.log(count++, 'Inserted ' + a.username);
				});
			})
			.onerror(function () {
				delete a.first_video;
				collection.insert(a, function (err) {
					if (err) return;
					console.log(count++, 'Inserted ' + a.username);
				});
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
					get_first_video(a);
				} else {
					console.log(status, 'Error getting first video and username of channel', a._id);
				}
			})
			.onerror(function (e) {
				console.log('Error getting first video and username of channel', a._id);
			});
	},

	get_channels = function (a) {
		var data = {
			part : 'id, statistics',
			categoryId : a.id,
			maxResults : 50,
			fields : 'nextPageToken,items(id,statistics)',
			key : KEY
		};

		if (a.nextPageToken)
			data.pageToken = a.nextPageToken;

		curl.get
			.to('www.googleapis.com', 443, '/youtube/v3/channels')
			.secured()
			.send(data)
			.then(function (status, data) {
				if (status === 200) {
					console.log(a.regionCode, 'Found', data.items.length, 'channels under ', a.snippet.title);

					data.items && data.items.forEach(function (b) {
						if (+b.statistics.videoCount > 0) {
							b.regionCode = a.regionCode;
							b.category = a.snippet.title;
							b.categoryId = a.id;
							b.date_crawled = +new Date;
							b._id = b.id;
							b.scm = null;
							delete b.id;
							get_username(b);
						}
					});

					if (data.nextPageToken) {
						a.nextPageToken = data.nextPageToken;
						get_channels(a);
					}
				}
				else {
					console.log(status, 'Error on getting channels of guideCategory', a.snippet.title);
				}
			})
			.onerror(function (e) {
				console.log('Error on getting channels of guideCategory', a.snippet.title);
			});
	},




	get_categories = function (b) {
		curl.get
			.to('www.googleapis.com', 443, '/youtube/v3/guideCategories')
			.secured()
			.send({
				part : 'id, snippet',
				regionCode : b,
				fields : 'items(id,snippet/title)',
				key : KEY
			})
			.then(function (status, data) {
				if (status === 200) {
					data.items.forEach(function (a) {
						a.regionCode = b;
						get_channels(a);
					});
				} else {
					console.log(status, 'Error on getting guideCategories', data);
				}
			})
			.onerror(function (e) {
				console.log('Error on getting guideCategories', e);
			});
	};


if (cluster.isMaster) {
	curl.get
		.to('www.googleapis.com', 443, '/youtube/v3/i18nRegions')
		.secured()
		.send({
			part : 'id',
			fields : 'items(id)',
			key : KEY
		})
		.then(function (status, data) {
			if (status === 200) {
				split(data.items.map(function (a) {return a.id;}), cpuCount).forEach(function (a) {
					cluster.fork({ regionCodes : a.join(',') });
				});
			}
			else
				console.log(status, 'Error on getting regions', data);
		})
		.onerror(function (e) {
			console.log('Error on getting regions', e);
		});
} else {
	console.log('Deploying black widow ', cluster.worker.id, 'for region codes:', process.env['regionCodes']);
	regions[cluster.worker.id] = process.env['regionCodes'];
	process.env['regionCodes'].split(',').forEach(get_categories);
}

cluster.on('exit', function (worker) {
	console.log('Black widow ' + worker.id + ' died :( Reviving...');
	cluster.fork({ regionCodes : regions[worker.id] });
});

