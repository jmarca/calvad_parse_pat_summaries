/* global require console process it describe after before */

var should = require('should')
var _ = require('lodash');
var async = require('async')

var fs = require('fs')
var path = require('path')
var rootdir = path.normalize(__dirname+'/..')

var glob = require('glob')

// eventually, work out how to do
// var rewire = require("rewire");
// // rewire acts exactly like require.
// var myModule = rewire("../lib/parse_pat_reports");

var ppr = require('../lib/parse_pat_reports')

// test db
var pg = require('pg'); //native libpq bindings = `var pg = require('pg').native`
var env = process.env
var puser = process.env.PSQL_USER
var ppass = process.env.PSQL_PASS
var phost = process.env.PSQL_HOST || '127.0.0.1'
var pport = process.env.PSQL_PORT || 5432
var pdbname = process.env.PSQL_DB || 'test'
var connectionString = "pg://"+puser+":"+ppass+"@"+phost+":"+pport+"/"+pdbname;
var schema = process.env.WIM_SCHEMA || 'wim_test'


describe ('parse file',function(){
    it('should exist', function(done){
        var pf = ppr.setup_file_parser
        should.exist(pf)
        done()
    })
})
describe ('parse file can process a file', function(){

    var speed_table = 'summary_speed'
    var class_table = 'summary_class'
    var speed_class_table = 'summary_speed_class'
    var create_tables =['CREATE TABLE '+schema+'.'+class_table+'('
                       + '     site_no integer not null ,'
                       + '     ts      timestamp not null,'
                       + '     wim_lane_no integer not null,'
                       + '     veh_class integer not null,'
                       + '     veh_count integer not null,'
                       + '     primary key (site_no,ts,wim_lane_no,veh_class)'
                       + ' )'
                       ,'CREATE TABLE '+schema+'.'+speed_table+' ('
                       + '     site_no integer not null ,'
                       + '     ts      timestamp not null,'
                       + '     wim_lane_no integer not null,'
                       + '     veh_speed numeric not null,'
                       + '     veh_count integer not null,'
                       + '     primary key (site_no,ts,wim_lane_no,veh_speed)'
                       + ' )'
                       ,'CREATE TABLE '+schema+'.'+speed_class_table +' ('
                       + '     site_no integer not null ,'
                       + '     ts      timestamp not null,'
                       + '     wim_lane_no integer not null,'
                       + '     veh_class integer not null,'
                       + '     veh_speed numeric not null,'
                       + '     veh_count integer not null,'
                       + '     primary key (site_no,ts,wim_lane_no,veh_class,veh_speed)'
                       + ' )'
                       ]


    // create temporary tables in database
    before( function(done){
        var client = new pg.Client(connectionString);
        client.connect(function(err){
            if(err) throw new Error(err)
        });

        client.query("drop schema if exists  " + schema + " cascade",function(err){
            if(err) return done(err)
            client.query("CREATE schema " + schema,function(err){
                if(err) return done(err)
                async.forEach(create_tables
                             ,function(sql,cb){
                                  client.query(sql,cb)
                              }
                             ,function(err){
                                  client.end()
                                  return done(err)
                              })
                return null
            })
            return null
        })

    })
    after(function(done){
        var client = new pg.Client(connectionString);
        client.connect(function(err){
            if(err) throw new Error(err)
        });
        client.query("drop schema if exists  " + schema + " cascade",function(err){
            client.end();
            return done(err)
        })
        return null
    })

    it('should parse a file',function(done){

        var pf = ppr.setup_file_parser({'schema':schema
                                       ,'speed_table':speed_table
                                       ,'class_table':class_table
                                       ,'speed_class_table':speed_class_table
                                       })
        should.exist(pf)
        var filename = rootdir+'/test/pat_small_test_file.txt'
        console.log('parsing '+filename)
        pf(filename,function(err){
            should.not.exist(err)
            // add sql checks here
            done(err)
        })

    })

    it('should parse a big file',function(done){

        var pf = ppr.setup_file_parser({'schema':schema
                                       ,'speed_table':speed_table
                                       ,'class_table':class_table
                                       ,'speed_class_table':speed_class_table
                                       })
        should.exist(pf)
        var filename = rootdir+'/test/pat_report_sample_2.txt'
        console.log('parsing '+filename)
        pf(filename,function(err){
            should.not.exist(err)
            // add sql checks here
            done(err)
        })

    })

   it('should parse multiple troublesome files',function(done){
        var fqueuer = ppr.file_queuer({'schema':schema
                                       ,'speed_table':speed_table
                                       ,'class_table':class_table
                                       ,'speed_class_table':speed_class_table
                                      }
                                     ,function(err){
                                          console.log('done with queued files')
                                          done(err)
                                      })
        should.exist(fqueuer)
        var groot = rootdir+'/test/report_0210_pr'
        var pattern = "*"
        glob("/**/"+pattern,{'cwd':groot,'root':groot},function(err,files){
            _.forEach(files
                     ,function(f){
                          fs.stat(f,function(err,stats){
                              if(stats.isFile()){
                                  fqueuer(f,function(err){
                                      should.not.exist(err)
                                      console.log('in test, done with file '+f)
                                  })
                              }
                          })
                      })
        })

    })

})

// these are tested implicitly above
// describe('options setting',function(){
//     it ('should allow different schemas in options setting')
//     it ('should allow different schemas in environ setting')
//     it ('should allow different db tables via options setting')
// })
