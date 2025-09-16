// GCE metadata helpers — lets the VM know its external IP, name, zone, project.
import http from 'http';

const HOST = 'metadata.google.internal';
const HDRS = { 'Metadata-Flavor': 'Google' };

function get(path) {
  return new Promise((resolve, reject) => {
    const req = http.request({ host: HOST, path, headers: HDRS, timeout: 2000 }, (res) => {
      let data = '';
      res.setEncoding('utf8');
      res.on('data', (c) => (data += c));
      res.on('end', () => resolve(data.trim()));
    });
    req.on('error', reject);
    req.on('timeout', () => { req.destroy(); reject(new Error('metadata timeout')); });
    req.end();
  });
}

export function createMetadata() {
  return {
    async externalIp()   { return get('/computeMetadata/v1/instance/network-interfaces/0/access-configs/0/external-ip').catch(()=>''); },
    async instanceName() { return get('/computeMetadata/v1/instance/name').catch(()=> ''); },
    async zone() {
      const full = await get('/computeMetadata/v1/instance/zone').catch(()=> '');
      // returns 'projects/123456/zones/us-central1-a' → take last segment
      return full ? full.split('/').pop() : '';
    },
    async projectId()    { return get('/computeMetadata/v1/project/project-id').catch(()=> ''); }
  };
}
