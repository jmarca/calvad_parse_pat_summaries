/* global require console process it describe after before */

var should = require('should')
var _ = require('lodash');
var queue = require('queue-async')

var config_okay = require('config_okay')
var fs = require('fs')
var path = require('path')
var rootdir = path.normalize(__dirname+'/..')
var config_file = rootdir+'/test.config.json'

var glob = require('glob')

// eventually, work out how to do
// var rewire = require("rewire");
// // rewire acts exactly like require.
// var myModule = rewire("../lib/parse_pat_reports");

var ppr = require('../lib/parse_pat_reports')

// test db
var pg = require('pg'); //native libpq bindings = `var pg = require('pg').native`

var connectionString
var config={}

var localclient
var localclientdone
var speed_table = 'deleteme_test_summary_speed'
var class_table = 'deleteme_test_summary_class'
var speed_class_table = 'deleteme_test_summary_speed_class'

before(function (done){
    config_okay(config_file,function(err,c){
        if(err) throw new Error(err)

        if(!c.postgresql.parse_pat_summaries_db){ throw new Error('need valid postgresql.parse_pat_summaries_db defined in test.config.json')}
        if(c.postgresql.table){ console.log('ignoring postgresql.table entry in config file; using temp tables instead') }
        if(!c.postgresql.username){ throw new Error('need valid postgresql.username defined in test.config.json')}
        if(!c.postgresql.password){ throw new Error('need valid postgresql.password defined in test.config.json')}

        // sane defaults
        if(c.postgresql.host === undefined) c.postgresql.host = 'localhost'
        if(c.postgresql.port === undefined) c.postgresql.port = 5432

        var host = c.postgresql.host ? c.postgresql.host : '127.0.0.1';
        var user = c.postgresql.username ? c.postgresql.username : 'myname';
        var pass = c.postgresql.password ? c.postgresql.password : 'secret';
        var port = c.postgresql.port ? c.postgresql.port :  5432;
        // global
        var db  = c.postgresql.parse_pat_summaries_db ? c.postgresql.parse_pat_summaries_db : 'spatialvds'
        connectionString = "pg://"+user+":"+pass+"@"+host+":"+port+"/"+db
        config = _.assign(config,c)
        //return done()

    var create_tables =['CREATE  TABLE '+class_table+'('
                       + '     site_no integer not null ,'
                       + '     ts      timestamp not null,'
                       + '     wim_lane_no integer not null,'
                       + '     veh_class integer not null,'
                       + '     veh_count integer not null,'
                       + '     primary key (site_no,ts,wim_lane_no,veh_class)'
                       + ' )'
                       ,'CREATE TABLE '+speed_table+' ('
                       + '     site_no integer not null ,'
                       + '     ts      timestamp not null,'
                       + '     wim_lane_no integer not null,'
                       + '     veh_speed numeric not null,'
                       + '     veh_count integer not null,'
                       + '     primary key (site_no,ts,wim_lane_no,veh_speed)'
                       + ' )'
                       ,'CREATE TABLE '+speed_class_table +' ('
                       + '     site_no integer not null ,'
                       + '     ts      timestamp not null,'
                       + '     wim_lane_no integer not null,'
                       + '     veh_class integer not null,'
                       + '     veh_speed numeric not null,'
                       + '     veh_count integer not null,'
                       + '     primary key (site_no,ts,wim_lane_no,veh_class,veh_speed)'
                       + ' )'
                       ]
        pg.connect(connectionString, function(err, _client, _done) {
            if(err){
                console.log(err)
                return done(err)
            }
            localclient = _client
            localclientdone = _done

            var q = queue(3)
            create_tables.forEach(function(stmt){
                q.defer(function(cb){
                    console.log('create '+stmt)
                    var query = localclient.query(stmt)
                    query.on('end', function(r){
                        console.log('done with '+stmt)
                        console.log(r)
                        return cb()
                    })
                    query.on('error',function(e){
                        console.log(e)
                        throw new Error(e)
                        return null
                    })
                })
                return null
            })
            q.await(function(err){
                console.log('finished making temp tables')
                return done()
            })
            return null
        })
        return null
    })
})

after( function(done){
    var stmt = 'drop table '+[speed_table
                             ,class_table
                             ,speed_class_table].join(',')
    var query = localclient.query(stmt)
    query.on('end', function(r){
        console.log('done with '+stmt)
        return done()
    })
    query.on('error',function(e){
        console.log(e)
        console.log('you should manually delete: '+stmt)
        throw new Error(e)
        return null
    })
    return null
})

describe ('parse file code is okay',function(){
    it('should exist', function(done){
        var pf = ppr.setup_file_parser
        should.exist(pf)
        return done()
    })
    return null
})

describe ('parse file can process a file', function(){
    var test_config = _.assign(config,{'speed_table':speed_table
                                      ,'class_table':class_table
                                      ,'speed_class_table':speed_class_table
                                      })



    it('hsould have a temportay table',function(done){
        var query = localclient.query('select * from '+speed_class_table)
        query.on('end',function(r){
            console.log(r)
            return done()
        })
        query.on('error',function(e){
            throw new Error(e)
        })
    })

    it('should parse a file',function(done){

        var pf = ppr.setup_file_parser(config)
        should.exist(pf)
        var filename = rootdir+'/test/pat_small_test_file.txt'
        console.log('parsing '+filename)
        pf(filename,function(err){
            should.not.exist(err)
            // add sql checks here
            return done(err)
        })
        return null
    })

    it('should parse a big file',function(done){

        var pf = ppr.setup_file_parser(test_config)
        should.exist(pf)
        var filename = rootdir+'/test/pat_report_sample_2.txt'
        console.log('parsing '+filename)
        pf(filename,function(err){
            should.not.exist(err)
            // add sql checks here
            return done(err)
        })
        return null
    })

   it('should parse multiple troublesome files',function(done){
       var fqueuer = ppr.file_queuer(test_config
                                    ,function(err){
                                         console.log('done with queued files')
                                         done(err)
                                     })
       should.exist(fqueuer)
       var groot = rootdir+'/test/report_0210_pr'
       var pattern = "*"
       glob("/**/"+pattern,{'cwd':groot,'root':groot},function(err,files){
           files.forEach(function(f){
               fs.stat(f,function(err,stats){
                   if(stats.isFile()){
                       fqueuer(f,function(err){
                           should.not.exist(err)
                           console.log('in test, done with file '+f)
                           return null
                       })
                   }
                   return null
               })
               return null
           })
           return null
       })
       return null
   })
   return null
})

// these are tested implicitly above
// describe('options setting',function(){
//     it ('should allow different schemas in options setting')
//     it ('should allow different schemas in environ setting')
//     it ('should allow different db tables via options setting')
// })
