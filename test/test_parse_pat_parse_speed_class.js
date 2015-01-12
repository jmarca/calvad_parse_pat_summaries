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


describe ('process speed class lines',function(){
    it('should exist', function(done){
        var pscl = ppr.process_speed_class_lines
        should.exist(pscl)
        done()
    })
    it('should process speed class data lines properly',function(done){
        var lines =[
            'DISTRIBUTION OF SPEEDS BY VEHICLE CLASSIFICATION'
          ,'========================================================================================================================'
          ,'SITE NO :        035               Location :  PACHECO-SCL-152, 26.9                  Lane(s) :     1'
          ,'DATE    :   01/01/10               County   :  SCL          State-ID : 06             Direction :   7'
          ,'========================================================================================================================'
          ,''
          ,'                                                  VEHICLE COUNTS'
          ,'                                                  --------------'
          ,'SPEED'
          ,'( mph)           1     2     3     4     5     6     7     8     9    10    11    12    13    14    15    TOTALS'
          ,'------------------------------------------------------------------------------------------------------------------------'
          ,'  1-  5          0     0     0     0     0     0     0     0     0     0     0     0     0     0     0       0'
          ,'  6- 10          0     0     0     0     0     0     0     0     0     0     0     0     0     0     0       0'
          ,' 11- 15          0     0     0     0     0     0     0     0     0     0     0     0     0     0     0       0'
          ,' 16- 20          0     0     0     0     0     0     0     0     0     0     0     0     0     0     0       0'
          ,' 21- 25          0     0     0     0     0     0     0     0     0     0     0     0     0     0     0       0'
          ,' 26- 30          0     0     1     0     0     0     0     0     0     0     0     0     0     0     0       1'
          ,' 31- 35          0     3     1     0     1     0     0     0     0     0     0     0     0     0     0       5'
          ,' 36- 40          0     1     1     0     0     0     0     0     0     0     0     0     0     0     0       2'
          ,' 41- 45          0     5     0     0     0     0     0     0     0     0     0     0     0     0     0       5'
          ,' 46- 50          0    18     3     0     7     0     0     0    15     0     2     0     0     0     0      45'
          ,' 51- 55          0   126    30     4    27     1     0    17    87     1     9     0     0     2     3     307'
          ,' 56- 60          0   578   123     1    64     0     0    18    81     0    18     2     0     0     1     886'
          ,' 61- 65          0  2026   533    11   137     1     0     8    16     0     0     0     0     0     3    2735'
          ,' 66- 70          0  1655   401     3    55     0     0     0     1     0     0     0     0     0     1    2116'
          ,' 71- 75          0   756   255     0    22     1     0     1     0     0     0     0     0     0     0    1035'
          ,' 76- 80          0   221    62     0     3     0     0     0     0     0     0     0     0     0     0     286'
          ,' 81- 85          0    40     9     0     0     0     0     0     0     0     0     0     0     0     0      49'
          ,' 86- 90          0     1     1     0     0     0     0     0     0     0     0     0     0     0     0       2'
          ,' 91- 95          0     0     0     0     0     0     0     0     0     0     0     0     0     0     0       0'
          ,' 96-100          0     1     1     0     0     0     0     0     0     0     0     0     0     0     0       2'
          ,'  > 100          0     2     2     0     0     0     0     0     0     0     0     0     0     0     0       4'
          ,'------------------------------------------------------------------------------------------------------------------------'
          ,' TOTALS          0  5433  1423    19   316     3     0    44   200     1    29     2     0     2     8    7480'
          ,''
          ,'AVG. SPEED       0    66    66    61    62    63     0    57    56    53    56    58     0    53    59      65'
          ,'======================14====28====32====40=====43=========47=====52====53====56===57===========58====62========================================='
          ,''
          ,'DAILY SPEED SUMMARY'
          ,'-------------------'
          ,''
          ,'TOTAL VEHICLES   :   7480     TOTAL VEHICLES >  55 mph --  7115     PERCENTAGE OF VEHICLES >  55 mph --  95.1'
          ,'AVERAGE SPEED    :   65.4     TOTAL VEHICLES >  60 mph --  6229     PERCENTAGE OF VEHICLES >  60 mph --  83.3'
          ,'MEDIAN SPEED     :   64.8     TOTAL VEHICLES >  65 mph --  3494     PERCENTAGE OF VEHICLES >  65 mph --  46.7'
          ,'85th PERCENTILE  :   74.7     TOTAL VEHICLES >  70 mph --  1378     PERCENTAGE OF VEHICLES >  70 mph --  18.4'
          ,'========================================================================================================================'
        ]

        var pscl = ppr.process_speed_class_lines
        var result

        var collect = []
        _.range(11,16).forEach(function(i){
            result = pscl(lines[i])
            should.exist(result)
            result.should.be.instanceOf(Array).with.lengthOf(0)
        })


        // now should have non null results
        _.range(16,29).forEach(function(i){
            result = pscl(lines[i])
            should.exist(result)
            collect = collect.concat(result)
        })

        result = pscl(lines[29])
            should.exist(result)
            result.should.be.instanceOf(Array).with.lengthOf(0)

                _.range(30,32).forEach(function(i){
            result = pscl(lines[i])
                    should.exist(result)

            collect = collect.concat(result)
        })


        collect[0].should.have.length(3)
        collect[8].should.have.length(3)
        collect.should.have.length(62)
        var popped = collect.pop()
        console.log(popped)
        popped.should.eql([100,3,2])

        done()
    })

})