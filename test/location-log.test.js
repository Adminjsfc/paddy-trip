'use strict';
const test = require('node:test');
const assert = require('node:assert/strict');
const { locationTripLog, LocationLogGate } = require('../src/location-log');
const cfg = { radiusMultiplier:1, offset:'+05:30' };
const trip = { id:'1',trip_status:1,source_lat:0,source_lng:0,source_radius:100,
 destination_lat:0,destination_lng:1,destination_radius:500 };
const point = { latitude:0,longitude:0,gps_timestamp:1790754413000,packet_type:'L' };
test('location log measures centre distances in km and identifies source/destination state',()=>{
 const r=locationTripLog('JH16B5498',trip,point,cfg);
 assert.equal(r.source_status,'INSIDE');assert.equal(r.destination_status,'OUTSIDE');
 assert.equal(r.distance_from_source_km,0);assert.equal(r.distance_from_destination_km,111.195);
 assert.equal(r.source_radius_km,0.1);assert.equal(r.trip_status_name,'STARTED');
});
test('missing location/geofence is unknown, never reported as zero distance',()=>{
 const r=locationTripLog('JH16B5498',{...trip,source_lat:null},point,cfg);
 assert.equal(r.source_status,'UNKNOWN');assert.equal(r.distance_from_source_km,null);
 const empty=locationTripLog('JH16B5498',trip,null,cfg);
 assert.equal(empty.source_status,'NO_LOCATION');assert.equal(empty.distance_from_source_km,null);
});
test('completed trip remains loggable and older processed history is labelled separately',()=>{
 const r=locationTripLog('JH16B5498',{...trip,trip_status:2},point,cfg,point.gps_timestamp-60000);
 assert.equal(r.trip_status_name,'COMPLETED');assert.equal(r.gps_timestamp,point.gps_timestamp);
 assert.equal(r.processed_packet_older_than_last_location,true);
 assert.equal(r.gps_datetime_ist,'2026-09-30 13:16:53.000');
});
test('identical location logs are suppressed but status changes and minute refresh stay visible',()=>{
 const gate=new LocationLogGate(60000),entry=locationTripLog('JH16B5498',trip,point,cfg);
 assert.equal(gate.shouldLog('JH16B5498:1',{...entry,reason:'after_packet_batch',location_age_seconds:39},0),true);
 assert.equal(gate.shouldLog('JH16B5498:1',{...entry,reason:'minute_refresh',location_age_seconds:40},1000),false);
 assert.equal(gate.shouldLog('JH16B5498:1',entry,60000),true);
 assert.equal(gate.shouldLog('JH16B5498:1',{...entry,trip_status:2},60001),true);
 gate.retain([]);assert.equal(gate.entries.size,0);
});
