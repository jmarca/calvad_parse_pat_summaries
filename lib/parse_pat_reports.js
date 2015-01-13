/*global console */
// rewrite of perl parsing here in node.js
//
// no problems with exisit perl code, but I have to redo it for PAT,
// and the way I was doing it in perl is largely untestable
// here I write up the pieces, export them, then I can test each part

/*global require process */

var pg = require('pg'); //native libpq bindings = `var pg = require('pg').native`
// var env = process.env
// var puser = process.env.PSQL_USER
// var ppass = process.env.PSQL_PASS
// var phost = process.env.PSQL_HOST || '127.0.0.1'
// var pport = process.env.PSQL_PORT || 5432
// var pdbname = process.env.PSQL_DB || 'test'
// var schema = process.env.WIM_SCHEMA || 'wim'
// var connectionString = "pg://"+puser+":"+ppass+"@"+phost+":"+pport+"/"+pdbname;

var fs = require('fs'),
    byline = require('byline')
//var async = require('async')
var queue = require('queue-async')
var _ = require('lodash')

//var df = require('date-functions')


var speedkey = {
    '1-  5'    : 2.5,
    '6- 10'   : 7.5,
    '11- 15'  : 12.5,
    '16- 20'  : 17.5,
    '21- 25'  : 22.5,
    '26- 30'  : 27.5,
    '31- 35'  : 32.5,
    '36- 40'  : 37.5,
    '41- 45'  : 42.5,
    '46- 50'  : 47.5,
    '51- 55'  : 52.5,
    '56- 60'  : 57.5,
    '61- 65'  : 62.5,
    '66- 70'  : 67.5,
    '71- 75'  : 72.5,
    '76- 80'  : 77.5,
    '81- 85'  : 82.5,
    '86- 90'  : 87.5,
    '91- 95'  : 92.5,
    '96-100' : 97.5,
    '> 100'   : 100,
                '00-35':17.5,
                '36-40':37.5,
                '41-45':42.5,
                '46-50':47.5,
                '51-55':52.5,
                '56-60':57.5,
                '61-65':62.5,
                '66-70':67.5,
                '71-75':72.5,
                '76-80':77.5,
                '81-85':82.5,
                 '> 85':87.5,
}


function make_collector(insert_string,config){
    if(!config) throw new Error('config required in call to make_collector')
    var _to_db = save_to_db(insert_string,config)
    var data=[]
    var collect = function(rows){
        //console.log('adding to '+insert_string)
        data = data.concat(rows)
        return null
    }
    collect.done=function(cb){
        // save data to db
        _to_db(_.clone(data),function(err){
            if(err) return cb(err)
            //console.log('db save done for '+insert_string)
            return cb()
        })
        data = []
        return null
    }
    collect.end=function(cb){
        //console.log('called end '+insert_string)
        return cb()
    }
    return collect
}



function save_to_db(insert_string,config){
    var host = config.postgresql.host ? config.postgresql.host : '127.0.0.1';
    var user = config.postgresql.username ? config.postgresql.username : 'myname';
    var pass = config.postgresql.password ? config.postgresql.password : 'secret';
    var port = config.postgresql.port ? config.postgresql.port :  5432;
    var db  = config.postgresql.parse_pat_summaries_db ? config.postgresql.parse_pat_summaries_db : 'spatialvds'
    var connectionString = "pg://"+user+":"+pass+"@"+host+":"+port+"/"+db

    function save_chunk(data,next){

        if(!data) return next('must have data, an array of records')
        if(data.length === 0) return next()
        //
        // create values statements array from incoming data
        // [ $site, $ts, $lane, $speedkey{$speed}, $count ];
        //

        // send the data one row at a time
        var one_at_a_time=function(client,val,callback){
            console.log('called one at a time')
            client.query(insert_string + val
                        ,function(err){
                             if(err) {
                                 // I don't care, just a duplicate entry most likely
                                 if(! /duplicate key value/.test(err)){
                                     console.log(err)
                                     //console.log(insert_string+val)
                                     throw new Error(err)
                                 }
                             }
                                         callback()
                         })
        }

        var all_at_once=function(client,rows, callback) {
            // var flat_rows = _.flatten(rows)
            if(rows.length < 10) console.log(insert_string + rows.join(','))
            client.query(insert_string + rows.join(',')
                        ,function(err){
                             var passed = true
                             if(err){
                                 if(/duplicate key value/.test(err)){
                                     //console.log(err)
                                 }else{
                                     console.log('all at once failed '+insert_string + rows.join(','))
                                     console.log(err)
                                     throw new Error(err)
                                     // bleh
                                 }
                                 passed = false
                             }
                             return callback(null,passed)
                         })
        }

        pg.connect(connectionString, function(err, client, done) {

            var tick = data.length
            console.log('connected to write '+tick+' rows')

            var values = []
            _.forEach(data
                     ,function(row){
                          values.push('('+row.join(',')+')')
                      })
            values = _.flatten(values)
            // first try all at once.  If fail, one by one
            queue().defer(all_at_once,client,values)
            .await(function(err,result){
                if(err || ! result){
                    console.log('all at once failed, try smaller chunks')
                    //return next(err)
                    // break into 10 blocks
                    var j = values.length
                    var q = new queue(5)
                    var blocksize = Math.ceil(j/10)
                    blocksize=500 // for testing?  keeping for
                                  // now---it reduces the number of
                                  // single line inserts
                    for (var i = 0;
                          i < j;
                         i+=blocksize){
                        var subblock = values.slice(i,i+blocksize)
                        q.defer(all_at_once,client,subblock)
                    }
                    q.awaitAll(function(err,results){
                        console.log('done with chunked')
                        var qq = queue(5)
                        var redo = false
                        for(var i = 0,j=results.length; i<j; i++){
                            if(! results[i]){
                                var start = i*blocksize
                                var subblock = values.slice(start,start+blocksize)
                                console.log('issue from ',start,start+blocksize)
                                //throw new Error()
                                qq.defer(one_at_a_time,client,subblock)
                                redo=true
                            }
                        }
                        if(redo){
                            console.log('waiting for one-at-a-time saver')

                            // need a final handler for qq
                             qq.awaitAll(function(e,moreresults){
                                 console.log('done with one-at-a-time saver')
                                 done()
                                 return next()
                             })
                        }else{
                            console.log('no problems saving in chunks')
                            done()
                            next()
                        }
                        return null
                    })
                }else{
                    console.log('no problems saving')
                    done()
                    next()
                }
                return null
            })
            return null
        })
        return null
    }

    // i used to use this in prior versions of node-pg api
    //
    // save_chunk.end=function(cb){
    //     // call this when you are done with this
    //     console.log('calling end for '+insert_string)
    //     cb()
    // }

    return save_chunk
}



function setup_file_parser (options ){
    if(!options) options = {}
    var _speed_table = options.speed_table || 'summaries_5min_speed'
    var _class_table = options.class_table || 'summaries_5min_class'
    var _speed_class_table = options.speed_class_table || 'summaries_daily_speed_class'

    var speed_collector = make_collector('INSERT INTO '+_speed_table+'(site_no,ts,wim_lane_no,veh_speed,veh_count) values '
                                        ,options)

    var class_collector = make_collector('INSERT INTO '+_class_table+'(site_no,ts,wim_lane_no,veh_class,veh_count) values '
                                        ,options)

    var speed_class_collector = make_collector('INSERT INTO '+_speed_class_table+'(site_no,ts,wim_lane_no,veh_speed,veh_class,veh_count) values '
                                        ,options)


    function parse_file(file,cb){

        var stream = fs.createReadStream(file);
        stream = byline.createStream(stream);

        // document parser starts in this routine.
        //
        // the document parser is stateful.  By that I mean that from one
        // line to the next, how a line gets parsed is dependent upon what
        // has come before.  the lines for speed and so on look much like
        // each other, and a block of data requires knowledge of the
        // header to save the right timestamp, lane, and direction, and so
        // on.
        //
        // there are three tables that I care about.
        //
        //   vehicle class by hour of day
        //   vehicle speed by hour of day
        //   vehicle class by vehicle speed (for an entire day)
        //
        // parsing state bounces between three states based on seeing the
        // correct table header line.  each time state changes, the site,
        // date, lane, and direction variables are reset by the next block of
        // lines that are read.
        //
        // Then when the data arrives, it is sent to the correct parsing
        // routine for data extraction
        //
        // when state changes again, the active parsing routine is sent a
        // message to save its data and reset itself
        //

        // globals

        var state
        var states = [process_header_lines
                     ,process_speed_class_lines
                     ,process_speed_hour_lines
                     ,process_class_hour_lines]

        var data_parser = process_header_lines
        var collector

        stream.on('data', function(line) {
            var result = null
            if(state && _.isArray(state) ){
                result = states[state[0]](line)
            }
            if(result === null){
                // maybe time to switch state
                if(state && _.isArray(state) && state.length == 2){
                    // check if header done parsing
                    if(process_header_lines.ready()){
                        state.shift()
                        return null
                    }
                }
                if(/DISTRIBUTION OF VEHICLE CLASSIFICATIONS BY HOUR OF DAY/.test(line)){
                    //if(collector !== undefined) collector.done(function(){})
                    collector = class_collector
                    process_header_lines.reset()
                    state=[0,3]
                }else if(/DISTRIBUTION OF SPEEDS BY VEHICLE CLASSIFICATION/.test(line)){
                    //if(collector !== undefined) collector.done(function(){})
                    collector = speed_class_collector
                    process_header_lines.reset()
                    state=[0,1]
                }else if(/DISTRIBUTION OF VEHICLE SPEEDS BY HOUR OF DAY/.test(line)){
                    //if(collector !== undefined) collector.done(function(){})
                    collector = speed_collector
                    process_header_lines.reset()
                    state=[0,2]
                }
            }else{
                // push result to the collector
                if(_.isArray(result)){
                    // don't test collector here because it if is
                    // undefined I need to fail
                    if(state[0] === 1){

                        var header_cols = process_header_lines.get_record(0)
                        //console.log(header_cols)
                        result = _.map(result
                                      ,function(row){
                                           return _.flatten([header_cols,row])
                                       })

                    }else{
                        result = _.map(result
                                      ,function(row){
                                           var header_cols = process_header_lines.get_record(row.shift())
                                           //console.log(header_cols)
                                           return _.flatten([header_cols,row])
                                       })
                   }
                    collector(result)
                }
            }

            //console.log([state,result]);
            return null
        });

        stream.on('end',function(err){
            if(err){
                console.log('Error '+err)
            }
            queue().defer(speed_class_collector.done)
            .defer(speed_collector.done)
            .defer(class_collector.done)
            .await(function(e){
                queue().defer(speed_class_collector.end)
                .defer(speed_collector.end)
                .defer(class_collector.end)
                .await(cb)
            })
            return null;
        })
    }
    return parse_file
}

var process_header_lines =
    function(){

        var lane=null;
        var site_no=null;
        var date=null;
        var direction=null;

        // stateful parsing of lines.  When the SITE NO line is detected,
        // all data are reset

        var site_no_regex = /SITE NO\s*:\s*0*(\d+).*Lane.*:\s*(\d+)/i
        var date_regex = /DATE\s*:\s*0?(\d+)\/0?(\d+)\/(\d+).*Direction.*:\s*(\d)/i

        function process_line(line){
            var match = site_no_regex.exec(line)
            if(match !== null){
                site_no = +match[1]
                lane = +match[2]
                return true
            }
            match = date_regex.exec(line)
            if(match !== null){
                date = new Date(2000+(+match[3]),match[1]-1,match[2])
                direction = +match[4]
                return true
            }
            return null
        }

        process_line.reset = function(){
            lane = null
            date = null
            direction = null
            site_no=null
        }
        process_line.get_lane      = function(){return lane}
        process_line.get_date      = function(){return date     }
        process_line.get_direction = function(){return direction}
        process_line.get_site_no   = function(){return site_no  }

        process_line.get_record = function(hour){
            if(this.ready()){
                var d = new Date(date)
                d.setHours(hour)
                var ts = "'"+d.toISOString()+"'"
                return [site_no,ts,lane]
            }
            return null
        }
        process_line.ready = function(){
            return (date !== null && site_no !== null && lane !== null)
        }

        return process_line;
    }()


function process_speed_class_lines(line){
    var match = /((\d+-|>)\s*\d+)\s+(\d.*)/.exec(line)
    if(match === null){
        return null
    }
    var speed = speedkey[match[1]]
    var re = /\s+/;
    var counts = match[3].trim().split(re)
    // don't care about total column
    counts.pop()
    var class_counts = []
    _.forEach(counts
             ,function(c,i){
                  if(c>0){
                      var vehclass = i+1
                      class_counts.push([speed,vehclass,+c])
                  }
              })
    return class_counts
}

var process_speed_hour_lines = function(){
    var speed_ranges =[]
    return function(line){
        // line could be the header, or data
        // check for header first
        var match = /HOUR\s*(.*)\s*TOTALS/i.exec(line)
        if(match !== null){
            speed_ranges = match[1].trim().split(/\s{2,}/)
            speed_ranges = _.map(speed_ranges
                                ,function(range){
                                     return speedkey[range]
                                 })
            return null
        }
        match = /^\s*(\d+)-\s*\d+\s+(.*)/.exec(line)
        if(match === null){
            return null
        }
        var hour = +match[1]
        var counts = match[2].trim().split(/\s+/) // split on whitespace
        counts.pop() // don't care about totals
        var speed_counts = []
        _.forEach(counts
                 ,function(c,i){
                      if(c>0){
                          speed_counts.push([hour,speed_ranges[i],+c])
                  }
              })
        return speed_counts
    }
                               }()

var process_class_hour_lines = function(line){
    // no need to check header line for class...just integers
    var match = /(\d+)-\s*\d+\s+(.*)/.exec(line)
    if(match === null){
        return null
    }
    var hour = +match[1]
    var counts = match[2].trim().split(/\s+/) // split on whitespace
    counts.pop() // don't care about totals
    var class_counts = []
    _.forEach(counts
             ,function(c,i){
                  if(c>0){
                      class_counts.push([hour,i+1,+c])
                  }
              })
    return class_counts
}


exports.setup_file_parser=setup_file_parser

// export everything for now, for initial testing, then switch to
// something like rewire
exports.process_class_hour_lines=process_class_hour_lines
exports.process_speed_hour_lines=process_speed_hour_lines
exports.process_speed_class_lines=process_speed_class_lines
exports.process_header_lines=process_header_lines

exports.save_to_db=save_to_db
exports.make_collector=make_collector


function file_queuer(options){

    var q = queue(5)

    function queuer(file){

        q.defer(function (cb) {
            console.log('queue '+ file)
            var pf = setup_file_parser(options)
            pf(file,function(err){
                pf = null
                console.log('done with '+ file)
                cb(err)
            })
        })
        return null

    }
    queuer.awaitAll = function(f){
        q.awaitAll(function(e,results){
            console.log('all queued files have been processed');
            if(f) f(e,results)
            return null
        })
    }
    return queuer
}

exports.file_queuer=file_queuer
