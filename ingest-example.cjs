'use strict';
const PacketIngest = require('./src/packet-ingest');
async function main() {
  const receiver = new PacketIngest(); await receiver.connect();
  try {
    // Replace this sample with the actual parsed GPS packet. Never substitute receipt time.
    const result = await receiver.push({
      vehicle_no: 'JH01AB1234', lat: 23.3441, lng: 85.3096,
      packet_datetime: new Date().toISOString()
    });
    console.log(result);
    console.log(await receiver.getLive('JH01AB1234'));
  } finally { await receiver.close(); }
}
if (require.main === module) main().catch(e => { console.error(e.message); process.exitCode = 1; });
