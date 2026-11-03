'use strict';
const test=require('node:test'),assert=require('node:assert/strict');
const {createCounts,recordReplay,recordFlush}=require('../src/worker-metrics');
const {flushVehicle}=require('../src/write-buffer');
test('history enqueue does not claim a flush or a committed trip transition',()=>{
  const counts=createCounts();recordReplay(counts,{processed:4,scanned:3,preceding:1,missing:0});
  assert.equal(counts.processed_packets,4);assert.equal(counts.history_packets_enqueued,4);
  assert.equal(counts.history_context_packets,1);assert.equal(counts.missing_history_payloads,0);
  assert.equal(counts.flushed_packets,0);assert.equal(counts.last_successful_flush_at,null);
  assert.equal(counts.started,0);assert.equal(counts.ended,0);
});
test('flush telemetry changes only after commit plus Redis acknowledgement',async()=>{
  const counts=createCounts(),packets=[{ts:1000}],intent={packets,operations:[]};
  let disconnected=true;
  const store={assertOwner:async()=>{},loadIntent:async()=>intent,publish:async()=>{},
    finishIntent:async()=>{if(disconnected)throw Error('Redis disconnected after commit');}};
  const repo={transitionBatch:async()=>({intent,rows:[],events:[]})};
  const run=async()=>recordFlush(counts,'JH16B5498',await flushVehicle(store,repo,'JH16B5498'),2000);
  await assert.rejects(run());assert.equal(counts.flushed_packets,0);assert.equal(counts.last_successful_flush_at,null);
  disconnected=false;await run();assert.equal(counts.flushed_packets,1);
  assert.equal(counts.last_successful_flush_at,'1970-01-01T00:00:02.000Z');
  assert.equal(counts.last_flushed_gps_timestamp,1000);assert.equal(counts.started,0);
});
