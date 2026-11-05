'use strict';
const test = require('node:test');
const assert = require('node:assert/strict');
const { tripListEntry } = require('../src/trip-list');

test('active trip remains visible with backend status and missing timestamps', () => {
  const entry = tripListEntry({ id: 2, vehicle_no: 'JH10AF5535',
    status: 'IN_TRANSIT', trip_status: 1, lifting_date: null, trip_date: null }, '+00:00');
  assert.equal(entry.status, 'IN_TRANSIT');
  assert.equal(entry.trip_status, 1);
  assert.deepEqual(entry.missing_timestamps, ['dispatched_at', 'trip_started_at']);
});

test('dispatched trip needs dispatch time and not a start time', () => {
  const entry = tripListEntry({ id: 3, status: 'DISPATCHED', trip_status: 0,
    lifting_date: '2026-11-03 12:00:00' }, '+00:00');
  assert.deepEqual(entry.missing_timestamps, []);
});
