import { assertEquals } from '@std/assert';
import { app } from './index.ts';

Deno.test('upload admission is held through storage writes and released on storage errors', async () => {
  const writing = Promise.withResolvers<void>();
  const finish = Promise.withResolvers<void>();
  let fail = true;
  const storage = Deno.serve({ port: 0, onListen() {} }, async (request) => {
    if (request.method === 'HEAD') return new Response(null, { status: 404 });
    await request.body?.cancel();
    writing.resolve();
    await finish.promise;
    return fail
      ? new Response('<Error><Code>AccessDenied</Code></Error>', {
        status: 403,
        headers: { 'Content-Type': 'application/xml' },
      })
      : new Response(null);
  });
  const env = {
    NX_CACHE_ACCESS_TOKEN: 'test-token',
    AWS_REGION: 'us-east-1',
    AWS_ACCESS_KEY_ID: 'test',
    AWS_SECRET_ACCESS_KEY: 'test',
    S3_BUCKET_NAME: 'cache',
    S3_ENDPOINT_URL: `http://127.0.0.1:${storage.addr.port}`,
    MAX_UPLOAD_BYTES: 4,
    MAX_CONCURRENT_UPLOADS: 1,
  };
  const upload = () =>
    app.fetch(
      new Request(`http://localhost/v1/cache/${crypto.randomUUID()}`, {
        method: 'PUT',
        headers: { Authorization: 'Bearer test-token', 'Content-Length': '4' },
        body: 'data',
      }),
      env,
    );
  const pending = upload();
  try {
    await writing.promise;
    const busy = await upload();
    assertEquals(busy.status, 503);
    await busy.body?.cancel();
    finish.resolve();
    const failed = await pending;
    assertEquals(failed.status, 500);
    await failed.body?.cancel();
    fail = false;
    const recovered = await upload();
    assertEquals(recovered.status, 200);
    await recovered.body?.cancel();
  } finally {
    finish.resolve();
    await (await pending).body?.cancel();
    await storage.shutdown();
  }
});
