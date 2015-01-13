/* global require console process it describe after before */

var should = require('should')
var _ = require('lodash');
// eventually, work out how to do
// var rewire = require("rewire");
// // rewire acts exactly like require.
// var myModule = rewire("../lib/parse_pat_reports");

var ppr = require('../lib/parse_pat_reports')



describe ('process header lines',function(){
    it('should exist', function(done){
        var phl = ppr.process_header_lines()
        should.exist(phl)
        phl.should.have.property('reset')
        phl.should.have.property('get_lane')
        phl.should.have.property('get_date')
        phl.should.have.property('get_direction')
        phl.should.have.property('get_site_no')
        phl.should.have.property('ready')
        done()
    })
    it('should process header lines properly',function(done){
        var lines = ['                    DISTRIBUTION OF VEHICLE SPEEDS BY HOUR OF DAY'
                    ,'========================================================================================================================'
                    ,'SITE NO :        035               Location :  PACHECO-SCL-152, 26.9                  Lane(s) :     1'
                    ,'DATE    :   01/12/10               County   :  SCL          State-ID : 06             Direction :   7'
                    ,'========================================================================================================================']

        var phl = ppr.process_header_lines()
        var notready = phl.ready()
        notready.should.not.be.ok

        phl(lines[0])
        notready = phl.ready()
        notready.should.not.be.ok

        phl(lines[1])
        notready = phl.ready()
        notready.should.not.be.ok

        phl(lines[2])
        notready = phl.ready()
        notready.should.not.be.ok

        var site_no = phl.get_site_no()
        site_no.should.eql(35)

        var lane = phl.get_lane()
        lane.should.eql(1)

        var record = phl.get_record(15)
        should.not.exist(record)

        phl(lines[3])
        notready = phl.ready()
        notready.should.be.ok

        phl(lines[4])
        notready = phl.ready()
        notready.should.be.ok

        var date = phl.get_date()
        var date_string = date.toISOString()
        date_string.should.eql( (new Date(2010,0,12)).toISOString())

        var direction = phl.get_direction()
        direction.should.eql(7)

        record = phl.get_record(15)
        record.should.be.ok
        record.should.eql([35,"'2010-01-12T23:00:00.000Z'",1])

        phl(lines[4])
        notready = phl.ready()
        notready.should.be.ok

        phl.reset()
        notready = phl.ready()
        notready.should.not.be.ok

        done()
    })

})