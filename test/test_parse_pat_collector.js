/* global require console process it describe after before */

var should = require('should')
var _ = require('lodash');
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


describe ('make collector', function(){

    before( function(done){
        var client = new pg.Client(connectionString);
        client.connect(function(err){
            if(err) throw new Error(err)
        });

        client.query("drop table if exists beatles",function(err){
            client.query("CREATE TABLE beatles(name varchar(10) primary key, height integer, birthday timestamptz)",
                         function(err){
                             if(err) return done(err)
                             client.end()
                             return done()
                         });
        });
        return null
    })
    after(function(done){
        var client = new pg.Client(connectionString);
        client.connect(function(err){
            if(err) throw new Error(err)
        });
        client.query("drop temp table beatles",function(err){
            client.end();
            return done()
        })
        return null
    })

    it('should make a usable collector (also tests save_to_db)'
      ,function(done){
           var collector = ppr.make_collector('INSERT INTO beatles(name, height, birthday) values')
           should.exist(collector)
           collector.should.be.an.instanceOf(Function)
           collector.should.have.property('done')
           collector.should.have.property('end')
           // now try to use it
           collector([["'Ringo'", 67,"'"+ (new Date(1945, 11, 2)).toISOString()+"'"]
                     ,["'John'", 68,"'"+ (new Date(1944, 10, 13)).toISOString()+"'"]])
           collector([["'Ringo'", 67,"'"+ (new Date(1945, 11, 2)).toISOString()+"'"]
                     ,["'John'", 68,"'"+ (new Date(1944, 10, 13)).toISOString()+"'"]
                     ,["'Justin'", 65,"'"+ (new Date(1995, 10, 13)).toISOString()+"'"]])
           collector.done(function(err){
               should.not.exist(err)
               if(err) return done(err)
               // check the results
               var client = new pg.Client(connectionString);
               client.connect(function(err){
                   if(err) throw new Error(err)
                   client.query('select * from  beatles order by birthday'
                               ,function(err,result){
                                    should.not.exist(err)
                                    result.should.have.property('rows')
                                    var rows = result.rows
                                    rows.should.be.an.instanceOf(Array)
                                    rows.should.have.length(3)
                                    rows[0].name.should.eql('John')
                                    rows[1].name.should.eql('Ringo')
                                    rows[2].name.should.eql('Justin')
                                    client.end();
                                    return done()
                                })

                   collector.end()
               })
               return null
           })
       })
})
