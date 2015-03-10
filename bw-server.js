'use strict';

var http = require('http'),
    url = require('url'),
    fs = require('fs'),
    qs = require('querystring'),
    config = require(__dirname + '/../config/config'),
    mysql = require(__dirname + '/../lib/mysql').open(config.crawled_db),
    count,
    index;

process.setMaxListeners(0);
http.globalAgent.maxSockets = 100;
//process.on('uncaughtException', function () {});

http.createServer(function (req, res) {
    var _url = url.parse(req.url),
        //startAt = process.hrtime(),
        send_response = function (status, data) {
            //var diff = process.hrtime(startAt);
            //console.log(_url.pathname, (diff[0] * 1e3 + diff[1] * 1e-6).toFixed(3) + 'ms');
            res.writeHead(status || 400);
            res.end('' + data);
        };


    switch (_url.pathname) {
    case '/insert':
        if (req.method === 'POST') {
            var body = '';
            req.on('data', function (data) {
                body += data;
                // 1e6 === 1 * Math.pow(10, 6) === 1 * 1000000 ~~~ 1MB
                if (body.length > 1e6) {
                    // FLOOD ATTACK OR FAULTY CLIENT, NUKE REQUEST
                    req.connection.destroy();
                }
            });

            req.on('end', function () {
                _url.query = qs.parse(body);
                mysql.query(
                    'INSERT INTO channels(id, username, views, subscribers, network, description, last_video_published_at, date_crawled) VALUES(?, ?, ?, ?, ?, ?, ?, ?)', [
                        _url.query.i,
                        _url.query.u, +_url.query.v, +_url.query.s,
                        _url.query.n || '',
                        _url.query.d || '',
                        _url.query.l || null, ~~(+new Date() / 1000)
                    ],
                    function (err, result) {
                        if (err && err.errno === 1062) {
                            console.log('Duplicate ' + _url.query.i);
                            return send_response(400);
                        }
                        else if (err) {
                            console.dir(err);
                            return send_response(400);
                        }
                        if (result.affectedRows === 1) {
                            mysql.query(
                                'INSERT INTO temp(channel_id, first_name, last_name, location, tags) VALUES(?, ?, ?, ?, ?)', [
                                    _url.query.i,
                                    _url.query.first_name,
                                    _url.query.last_name,
                                    _url.query.location,
                                    _url.query.tags
                                ],
                                function (err, result) {
                                    if (err) {
                                        console.dir(err);
                                    }
                                    if (result.affectedRows === 1) {
                                        console.log('Inserted ' + _url.query.u);
                                        if (count) {
                                            count++;
                                        }
                                        send_response(200);
                                    }
                                    else {
                                        send_response(400);
                                    }
                                });
                        }
                        else {
                            send_response(400);
                        }
                    });
            });
        }
        break;

    case '/count':
        if (count) {
            return send_response(200, count);
        }

        mysql.query('SELECT count(*) as count from channels;', function (err, _count) {
            if (err) {
                console.dir(err);
                return send_response(400);
            }
            send_response(200, count = _count[0].count);
        });
        break;

    case '/favicon.ico':
        send_response(404);
        break;

    default:
        if (index) {
            return send_response(200, index);
        }

        fs.readFile(__dirname + '/index.html', function (err, data) {
            if (err) {
                return send_response(500, 'Error loading index.html');
            }
            send_response(200, (index = data));
        });
    }

}).listen(8002);
console.log('App now listening port 8002');
