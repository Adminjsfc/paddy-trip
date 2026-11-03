'use strict';
const test = require('node:test');
const assert = require('node:assert/strict');
const { replayBatch } = require('../src/history-replay');
const { flushVehicle } = require('../src/write-buffer');
test('history replay queues every point in event order before advancing cursor', async () => {
  const points = [{ ts: 1, packet: { ts: 1 } }, { ts: 2, packet: { ts: 2 } }];
  const calls = [];
  const result = await replayBatch({ begin: async () => 0, batch: async () => points,
    enqueueHistory: async (v, p) => { calls.push('queue'); assert.deepEqual(p, points.map(x => x.packet)); },
    advance: async (v, ts) => { calls.push('cursor'); assert.equal(ts, 2); }
  }, 'JH16B5498', 0);
  assert.deepEqual(calls, ['queue', 'cursor']); assert.equal(result.processed, 2);
});
test('failed history enqueue cannot advance cursor or lose the replay batch', async () => {
  let advanced = false;
  await assert.rejects(replayBatch({ begin: async () => 0,
    batch: async () => [{ ts: 1, packet: { ts: 1 } }],
    enqueueHistory: async () => { throw new Error('Redis disconnected'); },
    advance: async () => { advanced = true; }
  }, 'JH16B5498', 0));
  assert.equal(advanced, false);
});
test('committed transition requests history rescan before Redis finalization, including retry', async () => {
  const p = { vehicle_no: 'JH16B5498', ts: 1000 };
  const intent = { packets: [p], operations: [{ packet: p, kind: 'start' }] };
  const calls = [];
  await flushVehicle({ assertOwner: async () => {}, loadIntent: async () => intent,
    publish: async () => calls.push('publish'),
    rewind: async (v, ts) => { calls.push('rewind'); assert.equal(ts, 1000); },
    finishIntent: async () => calls.push('finish')
  }, { transitionBatch: async () => ({ rows: [], events: [], intent }) }, p.vehicle_no);
  assert.deepEqual(calls, ['publish','rewind','finish']);
});
test('predecessor is crossing evidence, not a negative missing-payload count', async () => {
  let queued;
  const result = await replayBatch({ begin:async()=>10, batch:async()=>[{ts:11,packet:{ts:11}}],
    before:async()=>({ts:9}), enqueueHistory:async(v,p)=>{queued=p;}, advance:async()=>{}
  }, 'JH16B5498',10);
  assert.deepEqual(queued.map(p=>p.ts),[9,11]);
  assert.deepEqual(result,{processed:2,missing:0,scanned:1,preceding:1});
});
test('predecessor must not mask a real missing payload', async () => {
  const result = await replayBatch({ begin:async()=>10,
    batch:async()=>[{ts:11,packet:null},{ts:12,packet:{ts:12}}],before:async()=>({ts:9}),
    enqueueHistory:async()=>{},advance:async()=>{}
  },'JH16B5498',10);
  assert.equal(result.missing,1);assert.equal(result.processed,2);assert.equal(result.scanned,2);
});
