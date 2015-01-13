/* global require console process it describe after before */

/** process PAT files */


var ppr = require('./lib/parse_pat_reports')

var queue = require('queue-async')
var _ = require('lodash')

// db info
var pg = require('pg');
var env = process.env

var config_okay = require('config_okay')
var fs = require('fs')
var path = require('path')
var rootdir = path.normalize(__dirname+'/..')
var config_file = rootdir+'/config.json'

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
console.log(['going to check',root,pattern])

config_okay(config_file,function(err,c){
    if(err) throw new Error(err)

    if(!c.postgresql.parse_pat_summaries_db){ throw new Error('need valid postgresql.parse_pat_summaries_db defined in config.json')}
    if(!c.postgresql.username){ throw new Error('need valid postgresql.username defined in config.json')}
    if(!c.postgresql.password){ throw new Error('need valid postgresql.password defined in config.json')}

    // sane defaults
    if(c.postgresql.host === undefined) c.postgresql.host = 'localhost'
    if(c.postgresql.port === undefined) c.postgresql.port = 5432


    var fqueuer = ppr.file_queuer(c)


    glob("/**/"+pattern,{'cwd':root,'root':root},function(err,files){
        var filequeue = queue()
        console.log('found up to '+files.length+' files matching pattern.  Checking for real files, and processing')
        files.forEach(function(f){
            filequeue.defer(fs.stat,f)
            return null
        })
        filequeue.awaitAll(function(err,stats){
            for(var i =0,j=stats.length;i<j; i++){
                if(stats[i].isFile()){
                    fqueuer(files[i])
                }
            }
            console.log('processing queue loaded up')
            fqueuer.awaitAll(function(e,results){
                console.log('done with queued files')
                process.exit()
            })
            return null
        })
    })

    return null
})
