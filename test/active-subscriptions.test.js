'use strict';
const test = require('node:test');
const assert = require('node:assert/strict');
const { ActiveSubscriptions, ACTIVE_VEHICLES } = require('../src/active-subscriptions');
function setup(initial=[]) {
 const registry=new Set(initial),calls=[];
 const state={rows:[],dbError:false,deny:false,unsubscribeFail:false};
 const client={connected:true,
  subscribe:(topics,opts,cb)=>{calls.push({action:'subscribe',topics});cb(null,topics.map(topic=>({topic,qos:state.deny?128:1})));},
  unsubscribe:(topics,cb)=>{calls.push({action:'unsubscribe',topics});cb(state.unsubscribeFail?Error('Disconnected'):null);}};
 const redis={smembers:async()=>[...registry],sadd:async(k,...ts)=>ts.forEach(t=>registry.add(t)),srem:async(k,...ts)=>ts.forEach(t=>registry.delete(t))};
 const database={readExecute:async(sql,args,opts)=>{
  assert.equal(sql,ACTIVE_VEHICLES);assert.equal(opts.primary,true);
  if(state.dbError)throw Error('DB unavailable');return state.rows.map(vehicle_no=>({vehicle_no}));}};
 const manager=new ActiveSubscriptions({client,redis,database,clientId:'test',prefix:'test',log:()=>{}});
 return {manager,state,calls,registry,client};
}
test('subscribe only unique pending/started vehicle topics, never wildcard',async()=>{
 const f=setup();f.state.rows=['JH16B5498','JH16B5498','JH01AB1234'];await f.manager.load();await f.manager.sync(true);
 assert.match(ACTIVE_VEHICLES,/shipments s JOIN vehicles v/);
 assert.match(ACTIVE_VEHICLES,/s\.status IN \('DISPATCHED','IN_TRANSIT'\)/);
 assert.deepEqual(f.calls.filter(c=>c.action==='subscribe').flatMap(c=>c.topics),['gps/JH16B5498/data','gps/JH01AB1234/data']);
 assert.equal(f.manager.accepts('gps/OTHER/data'),false);
});
test('minute refresh adds new vehicle and removes vehicle with no remaining active trip',async()=>{
 const f=setup();f.state.rows=['JH16B5498'];await f.manager.load();await f.manager.sync(true);f.calls.length=0;
 f.state.rows=['JH01AB1234'];await f.manager.sync();
 assert.deepEqual(f.calls,[{action:'unsubscribe',topics:['gps/JH16B5498/data']},{action:'subscribe',topics:['gps/JH01AB1234/data']}]);
});
test('same active list does not resubscribe every minute',async()=>{
 const f=setup();f.state.rows=['JH16B5498'];await f.manager.load();await f.manager.sync(true);f.calls.length=0;
 await f.manager.sync();assert.equal(f.calls.length,0);
});
test('empty active list unsubscribes all known vehicle topics',async()=>{
 const f=setup(['gps/JH16B5498/data']);await f.manager.load();await f.manager.sync(true);
 assert.equal(f.registry.size,0);assert.equal(f.manager.accepts('gps/JH16B5498/data'),false);
});
test('DB outage preserves active subscriptions',async()=>{
 const f=setup(['gps/JH16B5498/data']);await f.manager.load();f.state.dbError=true;
 await f.manager.sync(true);assert.equal(f.manager.accepts('gps/JH16B5498/data'),true);
 await assert.rejects(f.manager.sync());assert.equal(f.registry.size,1);
});
test('reconnect restores vehicle subscriptions and removes legacy wildcard',async()=>{
 const f=setup(['gps/JH16B5498/data']);f.state.rows=['JH16B5498'];await f.manager.load();await f.manager.sync(true);
 assert.deepEqual(f.calls[0],{action:'unsubscribe',topics:['gps/+/data']});
 assert.ok(f.calls.some(c=>c.action==='subscribe'));
});
test('denied subscribe remains tracked and retries, with no false confirmation',async()=>{
 const f=setup();f.state.rows=['JH16B5498'];await f.manager.load();f.state.deny=true;
 await assert.rejects(f.manager.sync(true),/denied/);assert.equal(f.registry.size,1);assert.equal(f.manager.known.size,0);
 f.state.deny=false;await f.manager.sync();assert.equal(f.manager.known.size,1);
});
test('uncertain subscription removed on next sync if vehicle became inactive',async()=>{
 const f=setup();f.state.rows=['JH16B5498'];await f.manager.load();f.state.deny=true;
 await assert.rejects(f.manager.sync(true));f.state.rows=[];await f.manager.sync();assert.equal(f.registry.size,0);
 assert.ok(f.calls.some(c=>c.action==='unsubscribe'&&c.topics.includes('gps/JH16B5498/data')));
});
test('failed wildcard cleanup is retried before subscriptions',async()=>{
 const f=setup();f.state.rows=['JH16B5498'];await f.manager.load();f.state.unsubscribeFail=true;
 await assert.rejects(f.manager.sync(true));f.state.unsubscribeFail=false;await f.manager.sync();
 assert.equal(f.calls.filter(c=>c.topics.includes('gps/+/data')).length,2);
});
