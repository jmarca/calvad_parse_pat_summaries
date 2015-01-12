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


describe ('process class lines',function(){
    it('should exist', function(done){
        var pchl = ppr.process_class_hour_lines
        should.exist(pchl)
        done()
    })
    it('should process speed class data lines properly',function(done){
        var lines =[
 '                    DISTRIBUTION OF VEHICLE CLASSIFICATIONS BY HOUR OF DAY                                              '
,'========================================================================================================================'
,'SITE NO :        035               Location :  PACHECO-SCL-152, 26.9                  Lane(s) :     1                   '
,'DATE    :   01/01/10               County   :  SCL          State-ID : 06             Direction :   7                   '
,'========================================================================================================================'
,'                                                                                                                        '
,'HOURLY SUMMARY                                   VEHICLE COUNTS                                                         '
,'--------------                                   --------------                                                         '
,'HOUR                1     2     3     4     5     6     7     8     9    10    11    12    13    14    15     TOTALS    '
,'------------------------------------------------------------------------------------------------------------------------'
,' 0- 1               0    57    12     0     5     0     0     3     7     0     0     0     0     0     0       84      '
,' 1- 2               0    51    24     0     2     0     0     0     5     0     3     0     0     0     0       85      '
,' 2- 3               0    52    12     1     2     0     0     1     4     0     3     0     0     0     0       75      '
,' 3- 4               0    52    14     0     1     0     0     0     1     0     2     0     0     0     0       70      '
,' 4- 5               0    49     9     0     0     0     0     1     7     0     4     0     0     0     0       70      '
,' 5- 6               0    43    15     1     3     0     0     1     4     0     4     0     0     0     0       71      '
,'------------------------------------------------------------------------------------------------------------------------'
,'QTR TOTALS          0   304    86     2    13     0     0     6    28     0    16     0     0     0     0      455      '
,'========================================================================================================================'
,' 6- 7               0    53    14     0     4     0     0     1     5     0     4     0     0     1     0       82      '
,' 7- 8               0    59    17     0     5     0     0     1    12     0     2     0     0     0     0       96      '
,' 8- 9               0    89    30     0     9     0     0     0    11     0     0     0     0     0     0      139      '
,' 9-10               0   153    47     2    10     0     0     1     6     1     0     0     0     0     1      221      '
,'10-11               0   220    69     0    11     1     0     3     9     0     1     0     0     0     0      314      '
,'11-12               0   296    79     0    18     0     0     2    19     0     1     0     0     0     1      416      '
,'------------------------------------------------------------------------------------------------------------------------'
,'QTR TOTALS          0   870   256     2    57     1     0     8    62     1     8     0     0     1     2     1268      '
,'========================================================================================================================'
,'12-13               0   382    90     0    22     0     0     3    12     0     0     0     0     0     1      510      '
,'13-14               0   337   101     2    36     0     0     1    15     0     0     0     0     0     1      493      '
,'14-15               0   405   114     1    25     0     0     4    11     0     0     0     0     0     0      560      '
,'15-16               0   425   113     4    30     1     0     4     8     0     0     0     0     1     0      586      '
,'16-17               0   481   105     2    24     0     0     4    10     0     1     0     0     0     1      628      '
,'17-18               0   451   120     1    23     0     0     3    12     0     0     0     0     0     1      611      '
,'------------------------------------------------------------------------------------------------------------------------'
,'QTR TOTALS          0  2481   643    10   160     1     0    19    68     0     1     0     0     1     4     3388      '
,'========================================================================================================================'
,'18-19               0   449    99     2    19     0     0     5     4     0     0     0     0     0     0      578      '
,'19-20               0   384   108     1    19     0     0     1     3     0     0     0     0     0     0      516      '
,'20-21               0   310    76     0    16     0     0     1    11     0     3     0     0     0     1      418      '
,'21-22               0   295    62     2    10     0     0     2     4     0     0     1     0     0     0      376      '
,'22-23               0   199    60     0    11     0     0     0    10     0     1     0     0     0     1      282      '
,'23-24               0   141    33     0    11     1     0     2    10     0     0     1     0     0     0      199      '
,'------------------------------------------------------------------------------------------------------------------------'
,'QTR TOTALS          0  1778   438     5    86     1     0    11    42     0     4     2     0     0     2     2369      '
,'========================================================================================================================'
,'                                                                                                                        '
,'DAILY SUMMARY                                        VEHICLE COUNTS                                                     '
,'------------------------------------------------------------------------------------------------------------------------'
,'                    1     2     3     4     5     6     7     8     9    10    11    12    13    14    15     TOTALS    '
,'                                                                                                                        '
,'TOTAL               0  5433  1423    19   316     3     0    44   200     1    29     2     0     2     8       7480    '
,'PERCENT           0.0  72.6  19.0   0.3   4.2   0.0   0.0   0.6   2.7   0.0   0.4   0.0   0.0   0.0   0.1      100.0    '
,'                                                                                                                        '
,'========================================================================================================================'
,'                                                                                                                      '
        ]

        var pchl = ppr.process_class_hour_lines
        var result

        var collect = []
        _.range(0,10).forEach(function(i){
            result = pchl(lines[i])
            should.not.exist(result)
        })


        // now should have non null results
        _.range(10,43).forEach(function(i){
            result = pchl(lines[i])
            if(result && result.length > 0){
                collect = collect.concat(result)
            }
        })
        // no more matches
        _.range(43,lines.length).forEach(function(i){
            result = pchl(lines[i])
            should.not.exist(result)
        })

        collect[0].should.have.length(3)
        collect[8].should.have.length(3)
        collect.should.have.length(154)
        var popped = collect.pop()
        popped.should.eql([23,12,1])

        done()
    })

})