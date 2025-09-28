# S3 WebDAV Bridge

[![Deploy to Cloudflare Workers](https://deploy.workers.cloudflare.com/button)](https://deploy.workers.cloudflare.com/?url=https://github.com/junjiechen-chris/s3-webdav-cloudflare)

A Cloudflare Worker that translates WebDAV calls to S3 API calls, similar to the [r2-webdav](https://github.com/abersheeran/r2-webdav) project but supporting S3-compatible storage backends. Uses [aws4fetch](https://github.com/mhart/aws4fetch) for signing custom S3 calls.

## Deployment

```bash
wrangler deploy
```

## Configuration

### S3 Storage Backend

Set the following environment variables in Cloudflare Workers dashboard or use `wrangler secret put`:

```bash
wrangler secret put S3_ACCESS_KEY_ID
wrangler secret put S3_SECRET_ACCESS_KEY
wrangler secret put S3_REGION
wrangler secret put S3_BUCKET
wrangler secret put S3_ENDPOINT  # Optional for custom S3 endpoints. By default, AWS S3 endpoint is used.
wrangler secret put USERNAME     # WebDAV authentication 
wrangler secret put PASSWORD     # WebDAV authentication
```

---

## TODO
- [ ] Implement the dir listing properly

*Developed with [Claude Code](https://claude.ai/code)*
