'use strict';
// Read-only Internal service/packet validation. Does not update MySQL, Redis or a trip.
require('../src/config');
const cfg=require('../src/archive-config'),{HistoryService,archivePacket}=require('../src/archive-client');
const {vehicle,sqlTime}=require('../src/domain');
async function main(){
 const v=vehicle(process.argv[2]),from=Date.parse(process.argv[3]),until=Date.parse(process.argv[4]);
 if(!Number.isFinite(from)||!Number.isFinite(until)||from>=until||![process.argv[3],process.argv[4]].every(x=>/[Zz]$|[+-]\d\d:\d\d$/.test(x)))
  throw Error('Usage: node scripts/check-history-service.cjs VEHICLE FROM_ISO_WITH_TIMEZONE TO_ISO_WITH_TIMEZONE');
 const service=new HistoryService({...cfg,pageSize:5});
 try {
 const page=await service.page(v,from,until);
 const validation=page.data.map(r=>{
  const result=archivePacket(r,v,from,until);
  return result.packet?{valid:true,gps_timestamp:result.packet.ts,gps_datetime_utc:new Date(result.packet.ts).toISOString(),
   gps_datetime_ist:sqlTime(result.packet.ts,'+05:30'),
   latitude:result.packet.lat,longitude:result.packet.lng,packet_type:result.packet.packet_type}:{valid:false,reason:result.rejected};
 });
 console.log(JSON.stringify({kind:'history_service_check',transport:'IN_PROCESS_INFLUX_SERVICE',read_only:true,vehicle_no:v,
  display_timezone:'Asia/Kolkata',query_range:{from_date_utc:new Date(from).toISOString(),to_date_utc_exclusive:new Date(until).toISOString(),
   from_date_ist:sqlTime(from,'+05:30'),to_date_ist_exclusive:sqlTime(until,'+05:30')},
  first_page_count:page.count,has_more:page.pagination.has_more,validation},null,2));
 } finally { service.close(); }
}
main().catch(e=>{console.error(JSON.stringify({kind:'history_service_check_failed',code:e.code,message:e.message,status:e.status}));process.exitCode=1;});
