import { assertEquals } from '@std/assert';
import { afterAll, beforeAll, describe, it } from '@std/testing/bdd';
import { startEmulator } from '../scripts/start-emulator.ts';
import { app } from './index.ts';

describe('Bounded uploads', () => {
  let storage: { url: string; close(): Promise<void> };
  let env: {
    NX_CACHE_ACCESS_TOKEN: string;
    AWS_REGION: string;
    AWS_ACCESS_KEY_ID: string;
    AWS_SECRET_ACCESS_KEY: string;
    S3_BUCKET_NAME: string;
    S3_ENDPOINT_URL: string;
    MAX_UPLOAD_BYTES: number;
    MAX_CONCURRENT_UPLOADS: number;
  };
  beforeAll(async () => {
    const listener = Deno.listen({ port: 0 });
    const port = (listener.addr as Deno.NetAddr).port;
    listener.close();
    storage = await startEmulator({ port, bucket: 'nx-cloud' });
    env = {
      NX_CACHE_ACCESS_TOKEN: 'test-token',
      AWS_REGION: 'us-east-1',
      AWS_ACCESS_KEY_ID: 'AKIAIOSFODNN7EXAMPLE',
      AWS_SECRET_ACCESS_KEY: 'wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY',
      S3_BUCKET_NAME: 'nx-cloud',
      S3_ENDPOINT_URL: storage.url,
      MAX_UPLOAD_BYTES: 4,
      MAX_CONCURRENT_UPLOADS: 1,
    };
  });
  afterAll(async () => await storage.close());

  function request(
    method: string,
    key: string,
    body?: BodyInit,
    length?: string,
  ) {
    const headers = new Headers({ Authorization: 'Bearer test-token' });
    if (length !== undefined) headers.set('Content-Length', length);
    return app.fetch(
      new Request(`http://localhost/v1/cache/${key}`, {
        method,
        headers,
        body,
      }),
      env,
    );
  }

  it('stores exact-limit artifacts and never stores invalid or oversized bodies', async () => {
    const key = crypto.randomUUID();
    const exact = await request('PUT', key, 'data', '4');
    assertEquals(exact.status, 200);
    await exact.body?.cancel();
    const downloaded = await request('GET', key);
    assertEquals(downloaded.status, 200);
    assertEquals(await downloaded.text(), 'data');

    for (
      const [length, payload, expected] of [
        ['5', 'data', 413],
        ['3', 'data', 400],
        ['4', 'bad', 400],
        ['3', 'large', 413],
        ['-1', '', 400],
        ['1.5', 'x', 400],
        ['Infinity', '', 400],
        ['1e2', '', 400],
        ['0x4', 'data', 400],
        ['', '', 400],
        ['9007199254740992', '', 400],
        [undefined, 'data', 411],
      ] as const
    ) {
      const rejectedKey = crypto.randomUUID();
      const response = await request('PUT', rejectedKey, payload, length);
      assertEquals(
        response.status,
        expected,
        `Content-Length: ${length}, body: ${payload}`,
      );
      await response.body?.cancel();
      const absent = await request('GET', rejectedKey);
      assertEquals(absent.status, 404);
      await absent.body?.cancel();
    }
    const empty = await request('PUT', crypto.randomUUID(), undefined, '0');
    assertEquals(empty.status, 200);
    await empty.body?.cancel();
  });

  it('bounds active uploads without blocking reads and releases slots after failures', async () => {
    const reading = Promise.withResolvers<void>();
    const finish = Promise.withResolvers<void>();
    const key = crypto.randomUUID();
    const held = request(
      'PUT',
      key,
      new ReadableStream<Uint8Array>({
        async pull(controller) {
          reading.resolve();
          await finish.promise;
          controller.enqueue(new TextEncoder().encode('data'));
          controller.close();
        },
      }, { highWaterMark: 0 }),
      '4',
    );
    await reading.promise;
    try {
      const busy = await request('PUT', crypto.randomUUID(), 'x', '1');
      assertEquals(busy.status, 503);
      await busy.body?.cancel();
      const health = await app.fetch(
        new Request('http://localhost/health'),
        env,
      );
      assertEquals(health.status, 200);
      await health.body?.cancel();
      const read = await request('GET', crypto.randomUUID());
      assertEquals(read.status, 404);
      await read.body?.cancel();
    } finally {
      finish.resolve();
      await (await held).body?.cancel();
    }

    const failedKey = crypto.randomUUID();
    const failed = await request(
      'PUT',
      failedKey,
      new ReadableStream({
        pull(controller) {
          controller.error(new Error('client disconnected'));
        },
      }, { highWaterMark: 0 }),
      '4',
    );
    assertEquals(failed.status, 400);
    await failed.body?.cancel();
    const absent = await request('GET', failedKey);
    assertEquals(absent.status, 404);
    await absent.body?.cancel();

    const recovered = await request('PUT', crypto.randomUUID(), 'ok', '2');
    assertEquals(recovered.status, 200);
    await recovered.body?.cancel();
  });
});
