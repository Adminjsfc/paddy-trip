'use strict';
// Read-only inspection. No leases, queue acknowledgement, replay or business UPDATE.
const cfg=require('../src/config');
const {vehicle,shardFor,plan,sqlTime}=require('../src/domain');
const {SELECT,DETAILS_SELECT,transitionId}=require('../src/repository');
const {Store,connection}=require('../src/redis-store');
const {observeEvent,canStart}=require('../src/geofence-state');
const {decisionsFor}=require('../src/transition-decisions');
const {locationTripLog}=require('../src/location-log');
const {BUILD}=require('../src/worker-metrics');
const archiveCfg=require('../src/archive-config');
const {ArchiveStore}=require('../src/archive-store');
async function main(){
 if(!process.argv[2])throw Error('Usage: node scripts/diagnose-trip.cjs VEHICLE_NO [TRIP_ID]');
 const v=vehicle(process.argv[2]), selected=process.argv[3]||null;
 const db=require('../src/Database'), redis=connection(), store=new Store(redis,shardFor(v));
 try{
  await redis.connect();
  const rows=await db.readExecute(`${SELECT} AND UPPER(TRIM(v.registration_no)) = ? ORDER BY s.id`,[v],{primary:true});
  let displayed=rows.filter(t=>!selected||String(t.id)===selected);
  if(selected&&!displayed.length){
   displayed=await db.readExecute(`${DETAILS_SELECT}
    WHERE UPPER(TRIM(v.registration_no)) = ? AND s.id = ? AND s.trip_status IN (0, 1, 2)`,[v,selected],{primary:true});
  }
  const completed=await db.readExecute(`SELECT MAX(s.arrived_at) AS last_end FROM shipments s
   JOIN vehicles v ON v.id=s.vehicle_id WHERE UPPER(TRIM(v.registration_no))=? AND s.trip_status=2`,[v],{primary:true});
  const tracked=await store.loadGeofences(v,rows.map(t=>String(t.id)));
  const k=store.keys(v), raw=await redis.hget(k.live,'packet'), live=raw?JSON.parse(raw):null;
  const intent=await store.loadIntent(v);
  const archive=await new ArchiveStore(store).load(v);
  const p=live?{vehicle_no:v,lat:live.latitude??live.lat,lng:live.longitude??live.lng,
   ts:live.gps_timestamp??live.ts,packet_type:live.packet_type}:null;
  let decisions={};
  if(p){
   const states=Object.fromEntries(rows.map(t=>[String(t.id),observeEvent(tracked[String(t.id)],t,p,
    archive?.sourceInside?.[String(t.id)]?{...cfg,allowMissingStateAssumption:false}:cfg).current]));
   const done=await store.completedEvents(v,rows.flatMap(t=>['start','end'].map(kind=>transitionId(p,t.id,kind))));
   const duplicate=(t,kind)=>done.has(transitionId(p,t.id,kind));
   const action=plan(rows,p,cfg,(t,kind)=>!duplicate(t,kind)&&(kind==='end'||
    (!(archive?.sourceInside?.[String(t.id)]&&states[String(t.id)]?.source_exit_basis==='MISSING_REDIS_STATE')&&canStart(states[String(t.id)],t,p,cfg))));
   decisions=decisionsFor(rows,p,states,action,duplicate,cfg);
  }
  console.log(JSON.stringify({kind:'trip_diagnostic',build:BUILD,read_only:true,
   start_policy:archiveCfg.enabled?'RECOVER_ARCHIVE_BEFORE_MISSING_STATE_FALLBACK':'ASSUME_SOURCE_IN_ONLY_WHEN_REDIS_STATE_MISSING',
   gps_time_basis:'UTC_DATE_TIME',mysql_datetime_timezone:cfg.offset,archive_transport:'IN_PROCESS_INFLUX_SERVICE',archive_recovery_enabled:archiveCfg.enabled,
   archive_recovery:archive?{status:archive.status,query_version:archive.version,
    query_upgraded_at:archive.queryUpgradedAt||null,from_gps_utc:new Date(archive.from).toISOString(),
    from_gps_ist:sqlTime(archive.from,'+05:30'),until_gps_ist_exclusive:sqlTime(archive.until,'+05:30'),
    until_gps_utc_exclusive:new Date(archive.until).toISOString(),scanned:archive.scanned,accepted:archive.accepted,
    rejected:archive.rejected,evidence:archive.evidence,last_error:archive.last_error||null,
    created_at:archive.createdAt,completed_at:archive.completedAt||null,
    retry_at:archive.retryAt?new Date(archive.retryAt).toISOString():null}:null,
   vehicle_no:v,shard:store.shard,radius_unit:'km',radius_multiplier:cfg.radiusMultiplier,
   active_trip_ids:rows.map(t=>String(t.id)),last_completed_at:completed[0]?.last_end||null,
   requested_trip_id:selected,requested_trip_found:selected?displayed.length>0:null,
   pending_packets:await redis.zcard(k.pendingOrder),
   unfinished_write:intent?{id:intent.id,operations:intent.operations.map(o=>({trip_id:o.trip_id,kind:o.kind,gps_timestamp:o.packet.ts}))}:null,
   trips:displayed.map(t=>({
    ...locationTripLog(v,t,live,cfg),lifting_date:t.lifting_date,trip_date:t.trip_date,
    completion_date:t.completion_date||null,
    redis_key:`${store.base}:vehicle:${v}:trip:${t.id}:geofence`,redis_state:tracked[String(t.id)]||null,
    latest_packet_evaluation:Number(t.trip_status)===2?{reason:'TRIP_COMPLETED'}:
      archiveCfg.enabled&&archive?.status!=='READY'?{reason:archive?.status==='BLOCKED'?'ARCHIVE_RECOVERY_BLOCKED':'ARCHIVE_RECOVERY_PENDING',geofence_reason:decisions[String(t.id)]?.reason||'NO_LOCATION_RECEIVED'}:
      decisions[String(t.id)]||{reason:'NO_LOCATION_RECEIVED'}
   }))},null,2));
 }finally{await db.close().catch(()=>{});await redis.quit().catch(()=>redis.disconnect());}
}
main().catch(e=>{console.error(JSON.stringify({kind:'diagnostic_failed',code:e.code,message:e.message}));process.exitCode=1;});
