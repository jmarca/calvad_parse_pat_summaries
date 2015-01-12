/* global require console process it describe after before */

/** process PAT files */

var path = require('path')
var rootdir = path.normalize(__dirname+'/..')
var fs = require('fs')

var ppr = require('./lib/parse_pat_reports')

var async = require('async')
var _ = require('lodash')

// db info
var pg = require('pg');
var env = process.env
var puser = process.env.PSQL_USER
var ppass = process.env.PSQL_PASS
var phost = process.env.PSQL_HOST || '127.0.0.1'
var pport = process.env.PSQL_PORT || 5432
var pdbname = process.env.PSQL_DB || 'spatialvds'
var connectionString = "pg://"+puser+":"+ppass+"@"+phost+":"+pport+"/"+pdbname;
var schema = process.env.WIM_SCHEMA || 'wim'


var argv = require('optimist')
    .usage('parse PAT summary report files, save to database.\nUsage: $0')
    .default('r','/var/lib/wim')
    .alias('r', 'root')
    .describe('r', 'The root directory holding the PAT summary data.')
    .default('p','TEMP.PRN')
    .alias('p', 'pattern')
    .describe('p', 'The file pattern to use when searching for PAT summary reports')
    .argv
;
var root = argv.root;
var pattern = argv.pattern;

var glob = require('glob')
console.log([root,pattern])

var fqueuer = ppr.file_queuer({'schema':schema
                               // ,'speed_table':speed_table
                               // ,'class_table':class_table
                               // ,'speed_class_table':speed_class_table
                              }
                             ,function(err){
                                  console.log('done with queued files')
                                 process.exit()
                              })


glob("/**/"+pattern,{'cwd':root,'root':root},function(err,files){
    _.forEach(files
              ,function(f){
                  fs.stat(f,function(err,stats){
                      if(stats.isFile()){
                          fqueuer(f, function (err) {
                              if(err) console.log(err)
                              console.log('finished processing file '+f);
                          });
                      }
                  })
              })
})
