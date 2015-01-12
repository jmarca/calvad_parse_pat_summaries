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


describe ('process speed lines',function(){
    it('should exist', function(done){
        var pshl = ppr.process_speed_hour_lines
        should.exist(pshl)
        done()
    })
    it('should process speed class data lines properly',function(done){
        var lines =[
 '                    DISTRIBUTION OF VEHICLE SPEEDS BY HOUR OF DAY                                                       '
,'========================================================================================================================'
,'SITE NO :        035               Location :  PACHECO-SCL-152, 26.9                  Lane(s) :     1                   '
,'DATE    :   01/01/10               County   :  SCL          State-ID : 06             Direction :   7                   '
,'========================================================================================================================'
,'                                                                                                                        '
,'                                             SPEED  RANGE, ( mph)                                                       '
,'                                             --------------------                                                       '
,'HOUR           00-35   36-40   41-45   46-50   51-55   56-60   61-65   66-70   71-75   76-80   81-85    > 85  TOTALS    '
,'------------------------------------------------------------------------------------------------------------------------'
,' 0- 1             0       0       0       1       8      11      26      25       8       4       1       0      84     '
,' 1- 2             0       0       0       2       7      22      30      12      11       0       1       0      85     '
,' 2- 3             0       0       0       3       4      11      24      19       8       4       2       0      75     '
,' 3- 4             0       0       0       0       5       7      29      20       6       1       1       1      70     '
,' 4- 5             0       0       0       2       5      23      23       9       4       3       1       0      70     '
,' 5- 6             0       0       0       1       8      14      32      10       5       1       0       0      71     '
,'------------------------------------------------------------------------------------------------------------------------'
,'QTR TOTALS        0       0       0       9      37      88     164      95      42      13       6       1     455     '
,'========================================================================================================================'
,' 6- 7             0       0       0       0       8      16      23      16      14       4       1       0      82     '
,' 7- 8             1       0       0       2       8      17      32      22      11       3       0       0      96     '
,' 8- 9             0       0       0       2      12      23      43      39      13       5       2       0     139     '
,' 9-10             0       1       0       0      12      26      78      65      28       8       3       0     221     '
,'10-11             0       0       0       2       8      33     119      93      43      12       3       1     314     '
,'11-12             2       0       1       1       9      48     156     121      65      12       1       0     416     '
,'------------------------------------------------------------------------------------------------------------------------'
,'QTR TOTALS        3       1       1       7      57     163     451     356     174      44      10       1    1268     '
,'========================================================================================================================'
,'12-13             1       0       0       2      15      63     186     152      75      16       0       0     510     '
,'13-14             0       0       1       1      16      59     183     139      65      27       2       0     493     '
,'14-15             1       0       2       1      15      52     182     190      92      20       5       0     560     '
,'15-16             0       1       1       3      19      53     210     187      86      24       2       0     586     '
,'16-17             0       0       0       8      18      88     243     176      75      17       1       2     628     '
,'17-18             0       0       0       0      30      56     239     176      84      19       4       3     611     '
,'------------------------------------------------------------------------------------------------------------------------'
,'QTR TOTALS        2       1       4      15     113     371    1243    1020     477     123      14       5    3388     '
,'========================================================================================================================'
,'18-19             0       0       0       3      21      70     227     157      79      17       4       0     578     '
,'19-20             1       0       0       1      16      49     193     140      93      21       1       1     516     '
,'20-21             0       0       0       4      19      57     157     112      45      20       4       0     418     '
,'21-22             0       0       0       1      10      43     136     105      60      16       5       0     376     '
,'22-23             0       0       0       1       9      19      94      92      43      19       5       0     282     '
,'23-24             0       0       0       4      25      26      70      39      22      13       0       0     199     '
,'------------------------------------------------------------------------------------------------------------------------'
,'QTR TOTALS        1       0       0      14     100     264     877     645     342     106      19       1    2369     '
,'========================================================================================================================'
,'                                                                                                                        '
,'                                                                                                                        '
,'SUMMARY         00-35   36-40   41-45   46-50   51-55   56-60   61-65   66-70   71-75   76-80   81-85    > 85  TOTALS   '
,'------------------------------------------------------------------------------------------------------------------------'
,'TOTAL             6       2       5      45     307     886    2735    2116    1035     286      49       8    7480     '
,'PERCENT         0.1     0.0     0.1     0.6     4.1    11.8    36.6    28.3    13.8     3.8     0.7     0.1   100.0     '
,'                                                                                                                        '
,'                                                                                                                        '
,'                                                                                                                        '
,'DAILY SPEED SUMMARY                                                                                                     '
,'-------------------                                                                                                     '
,'                                                                                                                        '
,'TOTAL VEHICLES   :   7480     TOTAL VEHICLES >  55 mph --  7115     PERCENTAGE OF VEHICLES >  55 mph --  95.1           '
,'AVERAGE SPEED    :   65.4     TOTAL VEHICLES >  60 mph --  6229     PERCENTAGE OF VEHICLES >  60 mph --  83.3           '
,'MEDIAN SPEED     :   64.8     TOTAL VEHICLES >  65 mph --  3494     PERCENTAGE OF VEHICLES >  65 mph --  46.7           '
,'85th PERCENTILE  :   74.7     TOTAL VEHICLES >  70 mph --  1378     PERCENTAGE OF VEHICLES >  70 mph --  18.4           '
,'========================================================================================================================'
        ]

        var pshl = ppr.process_speed_hour_lines
        var result

        var collect = []
        _.range(0,10).forEach(function(i){
            result = pshl(lines[i])
            should.not.exist(result)
        })


        // now should have non null results
        _.range(10,43).forEach(function(i){
            result = pshl(lines[i])
            if(result && result.length > 0){
                collect = collect.concat(result)
            }
        })
        // no more matches
        _.range(43,lines.length).forEach(function(i){
            result = pshl(lines[i])
            should.not.exist(result)
        })

        collect[0].should.have.length(3)
        collect[8].should.have.length(3)
        collect.should.have.length(199)
        var popped = collect.pop()
        popped.should.eql([23,77.5,13])

        done()
    })

})