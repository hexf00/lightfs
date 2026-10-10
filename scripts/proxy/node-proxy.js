// Minimal Node.js streaming proxy — Experiment C reference implementation.
// Forwards GET /lightfs-bench/<key> to MinIO with keep-alive + pooling,
// streaming (pipe = backpressure). No metadata, no signing: pure data path.
const http = require('http');
const ORIGIN_HOST = process.env.ORIGIN_HOST || '127.0.0.1';
const ORIGIN_PORT = parseInt(process.env.ORIGIN_PORT || '9000', 10);
const PORT = parseInt(process.env.PROXY_PORT || '18086', 10);
const BUCKET = process.env.BUCKET || 'lightfs-bench';

const agent = new http.Agent({ keepAlive: true, maxSockets: 256 });

http.createServer((req, res) => {
  // POST /upload/<key>: stream the request body straight into the object
  // store (Content-Length preserved). Minimal upload proxy for Experiment C.
  if (req.method === 'POST' && req.url.startsWith('/upload/')) {
    const key = req.url.replace(/^\/upload\/+/, '');
    const headers = { 'content-length': req.headers['content-length'] || '0' };
    const up = http.request(
      { host: ORIGIN_HOST, port: ORIGIN_PORT, path: `/${BUCKET}/${key}`, method: 'PUT', headers, agent },
      (upr) => {
        res.writeHead(upr.statusCode);
        upr.pipe(res);
      }
    );
    up.on('error', (e) => { try { res.writeHead(502); res.end(String(e)); } catch {} });
    req.pipe(up);
    return;
  }
  const key = req.url.replace(/^\/+/, '');
  const up = http.request(
    { host: ORIGIN_HOST, port: ORIGIN_PORT, path: `/${BUCKET}/${key}`, method: 'GET', agent },
    (upr) => {
      res.writeHead(upr.statusCode, { 'content-type': upr.headers['content-type'] || 'application/octet-stream' });
      upr.pipe(res);
    }
  );
  up.on('error', (e) => {
    try { res.writeHead(502); res.end(String(e)); } catch {}
  });
  up.end();
}).listen(PORT, '127.0.0.1', () => console.log(`node proxy on ${PORT} -> ${ORIGIN_HOST}:${ORIGIN_PORT}/${BUCKET}`));
