'use strict';
const assert = require('node:assert/strict');
const { Repository, START, END, INSERT_EVENT } = require('../src/repository');
const { sqlTime } = require('../src/domain');
const format = ms => sqlTime(ms, '+00:00');
function utcText(ms) {
  const s = new Date(ms).toISOString();
  return s.slice(8,10)+s.slice(5,7)+s.slice(0,4)+' '+s.slice(11,19).replaceAll(':','');
}
function dbFixture(rows) {
  const state = { rows: structuredClone(rows).map(t=>({...t,vehicle_id:t.vehicle_id??12,shipment_no:t.shipment_no??'SHP-'+t.id,
    status:['DISPATCHED','IN_TRANSIT','ARRIVED'].includes(t.status)?t.status:Number(t.trip_status)===0?'DISPATCHED':Number(t.trip_status)===1?'IN_TRANSIT':'ARRIVED',
    expected_completion_at:t.expected_completion_at??null,time_duration:null})), writes: [], events: [], unavailable: false, loseCommit: false };
  const db = {
    async withTransaction(fn) {
      if (state.unavailable) throw Object.assign(new Error('Simulated MySQL outage'), { code: 'ECONNRESET' });
      const before = structuredClone(state.rows), beforeWrites = state.writes.length, beforeEvents=state.events.length;
      let result;
      try { result = await fn(db); }
      catch (e) { state.rows = before; state.writes.length = beforeWrites; state.events.length=beforeEvents; throw e; }
      if (state.loseCommit) { state.loseCommit = false; throw Object.assign(new Error('Commit response lost'), { code: 'ECONNRESET' }); }
      return result;
    },
    async writeExecute(sql, args=[]) {
      if(sql.includes('FROM vehicles'))return [{id:12}];
      if(sql.includes('FROM gps_devices'))return [];
      if(sql.includes('FOR UPDATE'))return state.rows.map(t=>({id:t.id}));
      if(sql===INSERT_EVENT){state.events.push({shipment_id:args[0],event_type:args[1],occurred_at:args[2],detail:JSON.parse(args[3])});return {affectedRows:1};}
      if (sql === START || sql === END) {
        const start = sql === START;
        const t = state.rows.find(t => String(t.id) === String(args[2]));
        if (!t || t.trip_status !== (start ? 0 : 1)) return { affectedRows: 0 };
        assert.equal(args.length,6);assert.equal(args[3],t.vehicle_id);
        assert.ok(args[0] > (start?t.lifting_date:t.trip_date));
        if (start) Object.assign(t,{status:'IN_TRANSIT',trip_status:1,trip_date:args[0],trip_started_at:args[0],trip_completed_at:null,last_gps_at:args[1]});
        else Object.assign(t,{status:'ARRIVED',trip_status:2,completion_date:args[0],trip_completed_at:args[0],last_gps_at:args[1]});
        state.writes.push({kind:start?'start':'end',trip_id:t.id,saved_datetime_utc:args[0]});
        return { affectedRows: 1 };
      }
      assert.ok(sql.startsWith('SELECT'));
      return structuredClone(state.rows.filter(t => t.trip_status < 2 || (sql.includes('OR s.id IN') && args.slice(1).map(String).includes(String(t.id)))));
    }
  };
  return { state, repo: new Repository(db) };
}
module.exports = { format, utcText, dbFixture };
