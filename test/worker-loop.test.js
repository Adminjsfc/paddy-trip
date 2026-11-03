'use strict';
const test=require('node:test'),assert=require('node:assert/strict'),vm=require('node:vm'),fs=require('node:fs'),path=require('node:path');
const {createRequire}=require('node:module');
test('worker reports the post-flush queue and distinguishes replay from committed evaluation',async()=>{
  const filename=path.resolve(__dirname,'../src/worker.js'),load=createRequire(filename),cfg=load('./config');
  const now=Date.now(),v='JH02AK7064';let pendingCalls=0,snapshot;const handlers={},logs=[];
  const row={id:'1',vehicle_no:v,route_id:1,trip_status:0,lifting_date:'2026-10-02 00:00:00',
    source_lat:24,source_lng:86,source_radius:0.2,destination_lat:25,destination_lng:87,destination_radius:0.2};
  const point={vehicle_no:v,lat:24,lng:86,ts:now-1000};
  const redis={connect:async()=>{},quit:async()=>{},disconnect:()=>{}};
  class Store {
    acquire=async()=>true;assertOwner=async()=>{};release=async()=>{};renew=async()=>true;
    publish=async()=>{};rewind=async()=>{};
    pendingVehicles=async()=>++pendingCalls===1?[v]:[];
    health=async value=>{snapshot=value;handlers.SIGTERM();};
  }
  const modules={
    './archive-config':{enabled:false},
    './config':{...cfg,shardId:7,logTripDetails:false,logVehicleLocation:false},
    './Database':{writeConfig:{timezone:cfg.offset},close:async()=>{}},
    './redis-store':{connection:()=>redis,Store},
    './repository':{Repository:class {refresh=async()=>[row];}},
    './write-buffer':{flushVehicle:async()=>({rows:[row],events:[],flushed:1,intent:{packets:[point],decisions:[]}}),reconnectable:()=>false},
    './history-replay':{replayBatch:async()=>({processed:2,scanned:1,preceding:1,missing:0})}
  };
  const source=fs.readFileSync(filename,'utf8');
  await vm.runInNewContext(source,{require:name=>modules[name]||load(name),
    process:{pid:123,on:(event,fn)=>{handlers[event]=fn;}},
    console:{log:line=>logs.push(JSON.parse(line))},setInterval:()=>1,clearInterval:()=>{},
    setTimeout:fn=>{fn();return 1;}});
  assert.equal(snapshot.pending_vehicles,0);assert.equal(snapshot.history_packets_enqueued,2);
  assert.equal(snapshot.flushed_packets,1);assert.equal(snapshot.started,0);assert.equal(snapshot.ended,0);
  assert.equal(snapshot.missing_history_payloads,0);assert.ok(snapshot.sampled_at);
  assert.equal(snapshot.build,load('./worker-metrics').BUILD);
  assert.ok(snapshot.last_successful_flush_at);assert.equal(snapshot.last_flushed_vehicle,v);
  assert.ok(logs.some(r=>r.kind==='worker_stopped'));
});
for(const savedIntent of [false,true])test(savedIntent?'worker reconciles an unfinished MySQL intent before archive recovery':'worker does not flush live packets while history recovery is incomplete',async()=>{
 const filename=path.resolve(__dirname,'../src/worker.js'),load=createRequire(filename),cfg=load('./config');
 const v='JH02AK7064',handlers={},point={vehicle_no:v,lat:24,lng:86,ts:Date.now()-1000};let writes=0,ticks=0,prepares=0;
 const row={id:'1',vehicle_no:v,route_id:1,trip_status:0,lifting_date:'2026-10-02 00:00:00',
  source_lat:24,source_lng:86,source_radius:0.2,destination_lat:25,destination_lng:87,destination_radius:0.2};
 const intent=savedIntent?{packets:[point],operations:[],decisions:[]}:null;
 class Store{
  acquire=async()=>true;assertOwner=async()=>{};release=async()=>{};renew=async()=>true;
  publish=async()=>{};rewind=async()=>{};loadIntent=async()=>intent;pendingVehicles=async()=>[v];
  health=async()=>handlers.SIGTERM();
 }
 const modules={
  './config':{...cfg,shardId:7,logTripDetails:false,logVehicleLocation:false},'./archive-config':{enabled:true},
  './Database':{writeConfig:{timezone:cfg.offset},close:async()=>{}},
  './redis-store':{connection:()=>({connect:async()=>{},quit:async()=>{}}),Store},
  './repository':{Repository:class{refresh=async()=>[row];}},
  './archive-recovery':{ArchiveRecovery:class{prepare=async()=>{prepares++;return{ready:false};};tick=async()=>{ticks++;};health=async()=>({archive_waiting_vehicles:1});}},
  './write-buffer':{flushVehicle:async()=>{writes++;return{rows:[row],events:[],flushed:1,intent};},reconnectable:()=>false},
  './history-replay':{replayBatch:async()=>({processed:0,scanned:0,preceding:0,missing:0})}
 };
 await vm.runInNewContext(fs.readFileSync(filename,'utf8'),{require:name=>modules[name]||load(name),
  process:{pid:1,on:(e,f)=>{handlers[e]=f;}},console:{log:()=>{}},setInterval:()=>1,clearInterval:()=>{},setTimeout:f=>{f();return 1;}});
 assert.equal(ticks,1);assert.equal(writes,savedIntent?1:0);assert.equal(prepares,savedIntent?0:1);
});
