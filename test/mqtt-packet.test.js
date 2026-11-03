'use strict';
const test = require('node:test');
const assert = require('node:assert/strict');
const { mapMqttPacket, publicPacket, utcDateTimeToTimestamp } = require('../src/mqtt-packet');
const { normalizePacket } = require('../src/domain');
const sample = { packet_header: '$PVT', vehicle_number: 'IGNORED', latitude: 24.6409413,
 longitude: 87.825546, packet_type: 'L', gps_fix: '1', date_time: '30092026 074653',
 gps_timestamp: 1790774213000, server_date_time: 1790754416378, is_history: false, is_live: true };
const cfg = { futureTolerance: 120000, historyMs: 604800000 };
test('topic supplies vehicle; five retained fields include corrected UTC epoch', () => {
 const p = normalizePacket(mapMqttPacket('gps/JH16B5498/data', JSON.stringify(sample)), cfg, sample.server_date_time);
 assert.equal(p.vehicle_no, 'JH16B5498');
 assert.deepEqual(publicPacket(p), { latitude: sample.latitude, longitude: sample.longitude,
   packet_type: 'L', gps_fix: '1', gps_timestamp: 1790754413000 });
});
test('history uses identical UTC conversion regardless of live/history flags', () => {
 const p = mapMqttPacket('gps/JH16B5498/data', JSON.stringify({ ...sample, packet_type: 'H', is_history: true, is_live: false }));
 assert.equal(p.packet_type, 'H'); assert.equal(p.packet_datetime, 1790754413000);
});
test('invalid fix or topic are rejected', () => {
 assert.throws(() => mapMqttPacket('gps/JH16B5498/data', JSON.stringify({ ...sample, gps_fix: '0' })), /fix/);
 assert.throws(() => mapMqttPacket('gps/JH16B5498/other', JSON.stringify(sample)), /topic/);
});
test('faulty or absent publisher epoch does not override confirmed UTC date_time', () => {
 for (const value of [1790774213000, 1790754413000, undefined, null]) {
  assert.equal(mapMqttPacket('gps/JH16B5498/data', JSON.stringify({ ...sample, gps_timestamp: value })).packet_datetime, 1790754413000);
 }
});
test('strict parser rejects invalid dates and missing timezone source', () => {
 for (const value of ['31022026 074653','30092026 246000','29022025 000000','2026-09-30 07:46:53',undefined]) {
  assert.throws(() => utcDateTimeToTimestamp(value));
 }
 assert.throws(() => mapMqttPacket('gps/JH16B5498/data', JSON.stringify({ ...sample, date_time: undefined })), /date_time/);
});
test('leap day and UTC midnight convert without local timezone dependence', () => {
 assert.equal(new Date(utcDateTimeToTimestamp('29022024 000000')).toISOString(), '2024-02-29T00:00:00.000Z');
 assert.equal(new Date(utcDateTimeToTimestamp('01012027 000000')).toISOString(), '2027-01-01T00:00:00.000Z');
});
test('deprecated offset argument cannot double-correct the epoch', () => {
 assert.equal(mapMqttPacket('gps/JH16B5498/data', JSON.stringify(sample), -19800000).packet_datetime, 1790754413000);
});

test('only exact $PVT packet header is accepted', () => {
 for (const header of ['$HBT', '$LGN', '$EPB', 'PVT', '$pvt', ' $PVT ', '', null, undefined]) {
  assert.throws(() => mapMqttPacket('gps/JH16B5498/data', JSON.stringify({ ...sample, packet_header: header })),
    { code: 'MQTT_HEADER_IGNORED' });
 }
});
test('unrelated header is ignored before GPS payload validation', () => {
 assert.throws(() => mapMqttPacket('gps/JH16B5498/data', JSON.stringify({ packet_header: '$HBT' })),
   { code: 'MQTT_HEADER_IGNORED' });
});
test('$PVT header permits both live and historical packet types', () => {
 for (const packet_type of ['L', 'H']) {
  assert.equal(mapMqttPacket('gps/JH16B5498/data', JSON.stringify({ ...sample, packet_type })).packet_type, packet_type);
 }
});
