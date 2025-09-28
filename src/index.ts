/**
 * Welcome to Cloudflare Workers! This is your first worker.
 *
 * - Run `npm run dev` in your terminal to start a development server
 * - Open a browser tab at http://localhost:8787/ to see your worker in action
 * - Run `npm run deploy` to publish your worker
 *
 * Learn more at https://developers.cloudflare.com/workers/
 */

import { AwsClient } from 'aws4fetch';

export interface Env {
	// S3 configuration
	S3_ACCESS_KEY_ID: string;
	S3_SECRET_ACCESS_KEY: string;
	S3_REGION: string;
	S3_BUCKET: string;
	S3_ENDPOINT?: string; // Optional for custom S3-compatible endpoints

	// Variables defined in the "Environment Variables" section of the Wrangler CLI or dashboard
	USERNAME: string;
	PASSWORD: string;
}

interface S3Object {
	key: string;
	size: number;
	lastModified: Date;
	etag: string;
	contentType?: string;
	contentDisposition?: string;
	contentLanguage?: string;
	contentEncoding?: string;
	cacheControl?: string;
	metadata?: Record<string, string>;
}

function createS3Client(env: Env): AwsClient {
	return new AwsClient({
		accessKeyId: env.S3_ACCESS_KEY_ID,
		secretAccessKey: env.S3_SECRET_ACCESS_KEY,
		region: env.S3_REGION,
		service: 's3',
		...(env.S3_ENDPOINT && { url: env.S3_ENDPOINT }),
	});
}

function getS3Url(env: Env, bucket: string, key?: string, queryParams?: string): string {
	const baseUrl = env.S3_ENDPOINT || `https://s3.${env.S3_REGION}.amazonaws.com`;
	const pathStyle = env.S3_ENDPOINT ? true : false; // Use path-style for custom endpoints
	
	if (pathStyle) {
		// Path-style: https://endpoint/bucket/key
		const path = key ? `/${bucket}/${key}` : `/${bucket}`;
		return `${baseUrl}${path}${queryParams ? `?${queryParams}` : ''}`;
	} else {
		// Virtual-hosted style: https://bucket.s3.region.amazonaws.com/key
		const path = key || '';
		return `https://${bucket}.s3.${env.S3_REGION}.amazonaws.com/${path}${queryParams ? `?${queryParams}` : ''}`;
	}
}

function parseXmlToObjects(xmlText: string): S3Object[] {
	const objects: S3Object[] = [];
	const contentRegex = /<Contents>(.*?)<\/Contents>/gs;
	let match;

	while ((match = contentRegex.exec(xmlText)) !== null) {
		const contentXml = match[1];
		
		const keyMatch = /<Key>(.*?)<\/Key>/.exec(contentXml);
		const sizeMatch = /<Size>(.*?)<\/Size>/.exec(contentXml);
		const lastModifiedMatch = /<LastModified>(.*?)<\/LastModified>/.exec(contentXml);
		const etagMatch = /<ETag>(.*?)<\/ETag>/.exec(contentXml);

		if (keyMatch) {
			objects.push({
				key: keyMatch[1],
				size: parseInt(sizeMatch?.[1] || '0'),
				lastModified: new Date(lastModifiedMatch?.[1] || ''),
				etag: etagMatch?.[1]?.replace(/"/g, '') || '',
				metadata: {},
			});
		}
	}

	return objects;
}

async function* listAll(aws: AwsClient, env: Env, bucket: string, prefix: string, isRecursive: boolean = false) {
	let continuationToken: string | undefined = undefined;
	
	do {
		const params = new URLSearchParams({
			'list-type': '2',
			prefix: prefix,
			...(isRecursive ? {} : { delimiter: '/' }),
			...(continuationToken && { 'continuation-token': continuationToken }),
		});

		const url = getS3Url(env, bucket, undefined, params.toString());
		const response = await aws.fetch(url);
		
		if (!response.ok) {
			throw new Error(`S3 API error: ${response.status} ${response.statusText}`);
		}

		const xmlText = await response.text();
		const objects = parseXmlToObjects(xmlText);

		for (const object of objects) {
			yield object;
		}

		// Check for continuation token
		const truncatedMatch = /<IsTruncated>(.*?)<\/IsTruncated>/.exec(xmlText);
		const isTruncated = truncatedMatch?.[1] === 'true';
		
		if (isTruncated) {
			const tokenMatch = /<NextContinuationToken>(.*?)<\/NextContinuationToken>/.exec(xmlText);
			continuationToken = tokenMatch?.[1];
		} else {
			continuationToken = undefined;
		}
	} while (continuationToken);
}

type DavProperties = {
	creationdate: string | undefined;
	displayname: string | undefined;
	getcontentlanguage: string | undefined;
	getcontentlength: string | undefined;
	getcontenttype: string | undefined;
	getetag: string | undefined;
	getlastmodified: string | undefined;
	resourcetype: string;
};

function fromS3Object(object: S3Object | null | undefined): DavProperties {
	if (object === null || object === undefined) {
		return {
			creationdate: new Date().toUTCString(),
			displayname: undefined,
			getcontentlanguage: undefined,
			getcontentlength: '0',
			getcontenttype: undefined,
			getetag: undefined,
			getlastmodified: new Date().toUTCString(),
			resourcetype: '<collection />',
		};
	}

	return {
		creationdate: object.lastModified.toUTCString(),
		displayname: object.contentDisposition,
		getcontentlanguage: object.contentLanguage,
		getcontentlength: object.size.toString(),
		getcontenttype: object.contentType,
		getetag: object.etag,
		getlastmodified: object.lastModified.toUTCString(),
		resourcetype: object.metadata?.resourcetype ?? '',
	};
}

function make_resource_path(request: Request): string {
	let path = new URL(request.url).pathname.slice(1);
	path = path.endsWith('/') ? path.slice(0, -1) : path;
	return path;
}

async function handle_head(request: Request, aws: AwsClient, env: Env, bucket: string): Promise<Response> {
	let response = await handle_get(request, aws, env, bucket);
	return new Response(null, {
		status: response.status,
		statusText: response.statusText,
		headers: response.headers,
	});
}

async function handle_get(request: Request, aws: AwsClient, env: Env, bucket: string): Promise<Response> {
	let resource_path = make_resource_path(request);

	if (request.url.endsWith('/')) {
		let page = '',
			prefix = resource_path;
		if (resource_path !== '') {
			page += `<a href="../">..</a><br>`;
			prefix = `${resource_path}/`;
		}

		for await (const object of listAll(aws, env, bucket, prefix)) {
			if (object.key === resource_path) {
				continue;
			}
			let href = `/${object.key + (object.metadata?.resourcetype === '<collection />' ? '/' : '')}`;
			page += `<a href="${href}">${object.contentDisposition ?? object.key.slice(prefix.length)}</a><br>`;
		}
		
		const pageSource = `<!DOCTYPE html><html lang="en"><head><meta charset="UTF-8"><meta name="viewport" content="width=device-width,initial-scale=1.0"><title>S3Storage</title><style>*{box-sizing:border-box;}body{padding:10px;font-family:'Segoe UI','Circular','Roboto','Lato','Helvetica Neue','Arial Rounded MT Bold','sans-serif';}a{display:inline-block;width:100%;color:#000;text-decoration:none;padding:5px 10px;cursor:pointer;border-radius:5px;}a:hover{background-color:#60C590;color:white;}a[href="../"]{background-color:#cbd5e1;}</style></head><body><h1>S3 Storage</h1><div>${page}</div></body></html>`;

		return new Response(pageSource, {
			status: 200,
			headers: { 'Content-Type': 'text/html; charset=utf-8' },
		});
	} else {
		try {
			const url = getS3Url(env, bucket, encodeURIComponent(resource_path));
			const headers: Record<string, string> = {};
			
			if (request.headers.get('Range')) {
				headers['Range'] = request.headers.get('Range')!;
			}

			const response = await aws.fetch(url, { headers });

			if (response.status === 404) {
				return new Response('Not Found', { status: 404 });
			}
			
			if (!response.ok) {
				throw new Error(`S3 API error: ${response.status} ${response.statusText}`);
			}

			const body = await response.arrayBuffer();
			const contentLength = body.byteLength;

			return new Response(body, {
				status: response.status === 206 ? 206 : 200,
				headers: {
					'Content-Type': response.headers.get('Content-Type') ?? 'application/octet-stream',
					'Content-Length': contentLength.toString(),
					...(response.headers.get('Content-Range') && { 'Content-Range': response.headers.get('Content-Range')! }),
					...(response.headers.get('Content-Disposition') && {
						'Content-Disposition': response.headers.get('Content-Disposition')!,
					}),
					...(response.headers.get('Content-Encoding') && {
						'Content-Encoding': response.headers.get('Content-Encoding')!,
					}),
					...(response.headers.get('Content-Language') && {
						'Content-Language': response.headers.get('Content-Language')!,
					}),
					...(response.headers.get('Cache-Control') && {
						'Cache-Control': response.headers.get('Cache-Control')!,
					}),
					...(response.headers.get('Expires') && {
						'Cache-Expiry': response.headers.get('Expires')!,
					}),
				},
			});
		} catch (error: any) {
			if (error.message.includes('404')) {
				return new Response('Not Found', { status: 404 });
			}
			throw error;
		}
	}
}

async function handle_put(request: Request, aws: AwsClient, env: Env, bucket: string): Promise<Response> {
	if (request.url.endsWith('/')) {
		return new Response('Method Not Allowed', { status: 405 });
	}

	let resource_path = make_resource_path(request);

	// Check if the parent directory exists
	let dirpath = resource_path.split('/').slice(0, -1).join('/');
	if (dirpath !== '') {
		try {
			const url = getS3Url(env, bucket, encodeURIComponent(dirpath));
			const response = await aws.fetch(url, { method: 'HEAD' });
			
			if (response.status === 404) {
				return new Response('Conflict', { status: 409 });
			}
			
			const resourceType = response.headers.get('x-amz-meta-resourcetype');
			if (resourceType !== '<collection />') {
				return new Response('Conflict', { status: 409 });
			}
		} catch (error: any) {
			return new Response('Conflict', { status: 409 });
		}
	}

	const body = await request.arrayBuffer();
	const url = getS3Url(env, bucket, encodeURIComponent(resource_path));
	
	const headers: Record<string, string> = {};
	if (request.headers.get('Content-Type')) {
		headers['Content-Type'] = request.headers.get('Content-Type')!;
	}
	if (request.headers.get('Content-Disposition')) {
		headers['Content-Disposition'] = request.headers.get('Content-Disposition')!;
	}
	if (request.headers.get('Content-Language')) {
		headers['Content-Language'] = request.headers.get('Content-Language')!;
	}
	if (request.headers.get('Content-Encoding')) {
		headers['Content-Encoding'] = request.headers.get('Content-Encoding')!;
	}
	if (request.headers.get('Cache-Control')) {
		headers['Cache-Control'] = request.headers.get('Cache-Control')!;
	}

	const response = await aws.fetch(url, {
		method: 'PUT',
		body,
		headers,
	});

	if (!response.ok) {
		throw new Error(`S3 API error: ${response.status} ${response.statusText}`);
	}

	return new Response('', { status: 201 });
}

async function handle_delete(request: Request, aws: AwsClient, env: Env, bucket: string): Promise<Response> {
	let resource_path = make_resource_path(request);

	if (resource_path === '') {
		// Delete all objects in bucket
		const objectsToDelete: string[] = [];
		for await (const object of listAll(aws, env, bucket, '', true)) {
			objectsToDelete.push(object.key);
		}

		if (objectsToDelete.length > 0) {
			// Delete in batches using S3 delete API
			const deleteXml = `<?xml version="1.0" encoding="UTF-8"?>
<Delete>
	${objectsToDelete.map(key => `<Object><Key>${key}</Key></Object>`).join('')}
</Delete>`;

			const url = getS3Url(env, bucket, undefined, 'delete');
			const response = await aws.fetch(url, {
				method: 'POST',
				body: deleteXml,
				headers: { 'Content-Type': 'application/xml' },
			});

			if (!response.ok) {
				throw new Error(`S3 API error: ${response.status} ${response.statusText}`);
			}
		}

		return new Response(null, { status: 204 });
	}

	try {
		// Check if resource exists and get metadata
		const headUrl = getS3Url(env, bucket, encodeURIComponent(resource_path));
		const headResponse = await aws.fetch(headUrl, { method: 'HEAD' });

		if (headResponse.status === 404) {
			return new Response('Not Found', { status: 404 });
		}

		// Delete the object
		const deleteUrl = getS3Url(env, bucket, encodeURIComponent(resource_path));
		const deleteResponse = await aws.fetch(deleteUrl, { method: 'DELETE' });

		if (!deleteResponse.ok) {
			throw new Error(`S3 API error: ${deleteResponse.status} ${deleteResponse.statusText}`);
		}

		const resourceType = headResponse.headers.get('x-amz-meta-resourcetype');
		if (resourceType !== '<collection />') {
			return new Response(null, { status: 204 });
		}

		// Delete all objects with this prefix (directory contents)
		const objectsToDelete: string[] = [];
		for await (const object of listAll(aws, env, bucket, resource_path + '/', true)) {
			objectsToDelete.push(object.key);
		}

		if (objectsToDelete.length > 0) {
			const deleteXml = `<?xml version="1.0" encoding="UTF-8"?>
<Delete>
	${objectsToDelete.map(key => `<Object><Key>${key}</Key></Object>`).join('')}
</Delete>`;

			const batchDeleteUrl = getS3Url(env, bucket, undefined, 'delete');
			const batchResponse = await aws.fetch(batchDeleteUrl, {
				method: 'POST',
				body: deleteXml,
				headers: { 'Content-Type': 'application/xml' },
			});

			if (!batchResponse.ok) {
				throw new Error(`S3 API error: ${batchResponse.status} ${batchResponse.statusText}`);
			}
		}

		return new Response(null, { status: 204 });
	} catch (error: any) {
		if (error.message.includes('404')) {
			return new Response('Not Found', { status: 404 });
		}
		throw error;
	}
}

async function handle_mkcol(request: Request, aws: AwsClient, env: Env, bucket: string): Promise<Response> {
	let resource_path = make_resource_path(request);

	// Check if the resource already exists
	try {
		const url = getS3Url(env, bucket, encodeURIComponent(resource_path));
		const response = await aws.fetch(url, { method: 'HEAD' });
		
		if (response.ok) {
			return new Response('Method Not Allowed', { status: 405 });
		}
	} catch (error: any) {
		// Object doesn't exist, which is what we want
	}

	// Check if the parent directory exists
	let parent_dir = resource_path.split('/').slice(0, -1).join('/');

	if (parent_dir !== '') {
		try {
			const parentUrl = getS3Url(env, bucket, encodeURIComponent(parent_dir));
			const parentResponse = await aws.fetch(parentUrl, { method: 'HEAD' });
			
			if (parentResponse.status === 404) {
				return new Response('Conflict', { status: 409 });
			}
		} catch (error: any) {
			return new Response('Conflict', { status: 409 });
		}
	}

	const url = getS3Url(env, bucket, encodeURIComponent(resource_path));
	const headers: Record<string, string> = {
		'x-amz-meta-resourcetype': '<collection />',
	};
	
	if (request.headers.get('Content-Type')) {
		headers['Content-Type'] = request.headers.get('Content-Type')!;
	}

	const response = await aws.fetch(url, {
		method: 'PUT',
		body: new Uint8Array(),
		headers,
	});

	if (!response.ok) {
		throw new Error(`S3 API error: ${response.status} ${response.statusText}`);
	}

	return new Response('', { status: 201 });
}

function generate_propfind_response(object: S3Object | null): string {
	if (object === null) {
		return `
	<response>
		<href>/</href>
		<propstat>
			<prop>
			${Object.entries(fromS3Object(null))
				.filter(([_, value]) => value !== undefined)
				.map(([key, value]) => `<${key}>${value}</${key}>`)
				.join('\n\t\t\t\t')}
			</prop>
			<status>HTTP/1.1 200 OK</status>
		</propstat>
	</response>`;
	}

	let href = `/${object.key + (object.metadata?.resourcetype === '<collection />' ? '/' : '')}`;
	return `
	<response>
		<href>${href}</href>
		<propstat>
			<prop>
			${Object.entries(fromS3Object(object))
			.filter(([_, value]) => value !== undefined)
			.map(([key, value]) => `<${key}>${value}</${key}>`)
			.join('\n\t\t\t\t')}
			</prop>
			<status>HTTP/1.1 200 OK</status>
		</propstat>
	</response>`;
}

async function handle_propfind(request: Request, aws: AwsClient, env: Env, bucket: string): Promise<Response> {
	let resource_path = make_resource_path(request);

	let is_collection: boolean;
	let page = `<?xml version="1.0" encoding="utf-8"?>
<multistatus xmlns="DAV:">`;

	if (resource_path === '') {
		page += generate_propfind_response(null);
		is_collection = true;
	} else {
		try {
			const url = getS3Url(env, bucket, encodeURIComponent(resource_path));
			const response = await aws.fetch(url, { method: 'HEAD' });

			if (response.status === 404) {
				return new Response('Not Found', { status: 404 });
			}
			
			if (!response.ok) {
				throw new Error(`S3 API error: ${response.status} ${response.statusText}`);
			}

			const object: S3Object = {
				key: resource_path,
				size: parseInt(response.headers.get('Content-Length') || '0'),
				lastModified: new Date(response.headers.get('Last-Modified') || ''),
				etag: response.headers.get('ETag')?.replace(/"/g, '') || '',
				contentType: response.headers.get('Content-Type') || undefined,
				contentDisposition: response.headers.get('Content-Disposition') || undefined,
				contentLanguage: response.headers.get('Content-Language') || undefined,
				contentEncoding: response.headers.get('Content-Encoding') || undefined,
				cacheControl: response.headers.get('Cache-Control') || undefined,
				metadata: {
					resourcetype: response.headers.get('x-amz-meta-resourcetype') || '',
				},
			};

			is_collection = object.metadata?.resourcetype === '<collection />';
			page += generate_propfind_response(object);
		} catch (error: any) {
			if (error.message.includes('404')) {
				return new Response('Not Found', { status: 404 });
			}
			throw error;
		}
	}

	if (is_collection) {
		let depth = request.headers.get('Depth') ?? 'infinity';
		switch (depth) {
			case '0':
				break;
			case '1':
				{
					let prefix = resource_path === '' ? resource_path : resource_path + '/';
					for await (let object of listAll(aws, env, bucket, prefix)) {
						page += generate_propfind_response(object);
					}
				}
				break;
			case 'infinity':
				{
					let prefix = resource_path === '' ? resource_path : resource_path + '/';
					for await (let object of listAll(aws, env, bucket, prefix, true)) {
						page += generate_propfind_response(object);
					}
				}
				break;
			default: {
				return new Response('Forbidden', { status: 403 });
			}
		}
	}

	page += '\n</multistatus>\n';
	return new Response(page, {
		status: 207,
		headers: {
			'Content-Type': 'text/xml',
		},
	});
}

async function handle_proppatch(request: Request, aws: AwsClient, env: Env, bucket: string): Promise<Response> {
	const resource_path = make_resource_path(request);

	try {
		// Check if resource exists
		const headUrl = getS3Url(env, bucket, encodeURIComponent(resource_path));
		const headResponse = await aws.fetch(headUrl, { method: 'HEAD' });

		if (headResponse.status === 404) {
			return new Response('Not Found', { status: 404 });
		}

		const body = await request.text();

		const setProperties: { [key: string]: string } = {};
		const removeProperties: string[] = [];
		let currentAction: 'set' | 'remove' | null = null;
		let currentPropName: string | null = null;
		let currentPropValue: string = '';

		class PropHandler {
			element(element: Element) {
				const tagName = element.tagName.toLowerCase();
				if (tagName === 'set') {
					currentAction = 'set';
				} else if (tagName === 'remove') {
					currentAction = 'remove';
				} else if (tagName === 'prop') {
					// ignore <prop> tag
				} else {
					// property name
					currentPropName = tagName;
					currentPropValue = '';
				}
			}

			text(textChunk: Text) {
				if (currentPropName) {
					currentPropValue += textChunk.text;
				}
			}

			end(_element: Element) {
				if (currentAction === 'set' && currentPropName) {
					setProperties[currentPropName] = currentPropValue.trim();
				} else if (currentAction === 'remove' && currentPropName) {
					removeProperties.push(currentPropName);
				}
				currentPropName = null;
				currentPropValue = '';
			}
		}

		await new HTMLRewriter().on('propertyupdate', new PropHandler()).transform(new Response(body)).arrayBuffer();

		// Get the current object
		const getUrl = getS3Url(env, bucket, encodeURIComponent(resource_path));
		const getResponse = await aws.fetch(getUrl);

		if (!getResponse.ok) {
			return new Response('Not Found', { status: 404 });
		}

		const objectBody = await getResponse.arrayBuffer();

		// Prepare headers with updated metadata
		const headers: Record<string, string> = {};
		
		// Copy existing headers
		if (headResponse.headers.get('Content-Type')) {
			headers['Content-Type'] = headResponse.headers.get('Content-Type')!;
		}
		if (headResponse.headers.get('Content-Disposition')) {
			headers['Content-Disposition'] = headResponse.headers.get('Content-Disposition')!;
		}
		if (headResponse.headers.get('Content-Language')) {
			headers['Content-Language'] = headResponse.headers.get('Content-Language')!;
		}
		if (headResponse.headers.get('Content-Encoding')) {
			headers['Content-Encoding'] = headResponse.headers.get('Content-Encoding')!;
		}
		if (headResponse.headers.get('Cache-Control')) {
			headers['Cache-Control'] = headResponse.headers.get('Cache-Control')!;
		}

		// Copy existing metadata
		for (const [key, value] of headResponse.headers.entries()) {
			if (key.startsWith('x-amz-meta-')) {
				headers[key] = value;
			}
		}

		// Update metadata with new properties
		for (const propName in setProperties) {
			headers[`x-amz-meta-${propName}`] = setProperties[propName];
		}

		for (const propName of removeProperties) {
			delete headers[`x-amz-meta-${propName}`];
		}

		// Re-upload object with new metadata
		const putUrl = getS3Url(env, bucket, encodeURIComponent(resource_path));
		const putResponse = await aws.fetch(putUrl, {
			method: 'PUT',
			body: objectBody,
			headers,
		});

		if (!putResponse.ok) {
			throw new Error(`S3 API error: ${putResponse.status} ${putResponse.statusText}`);
		}

		let responseXML = '<?xml version="1.0" encoding="utf-8"?>\n<multistatus xmlns="DAV:">\n';

		for (const propName in setProperties) {
			responseXML += `
    <response>
        <href>/${resource_path}</href>
        <propstat>
            <prop>
                <${propName} />
            </prop>
            <status>HTTP/1.1 200 OK</status>
        </propstat>
    </response>\n`;
		}

		for (const propName of removeProperties) {
			responseXML += `
    <response>
        <href>/${resource_path}</href>
        <propstat>
            <prop>
                <${propName} />
            </prop>
            <status>HTTP/1.1 200 OK</status>
        </propstat>
    </response>\n`;
		}

		responseXML += '</multistatus>';

		return new Response(responseXML, {
			status: 207,
			headers: {
				'Content-Type': 'application/xml; charset="utf-8"',
			},
		});
	} catch (error: any) {
		if (error.message.includes('404')) {
			return new Response('Not Found', { status: 404 });
		}
		throw error;
	}
}

async function handle_copy(request: Request, aws: AwsClient, env: Env, bucket: string): Promise<Response> {
	let resource_path = make_resource_path(request);
	let dont_overwrite = request.headers.get('Overwrite') === 'F';
	let destination_header = request.headers.get('Destination');
	if (destination_header === null) {
		return new Response('Bad Request', { status: 400 });
	}
	let destination = new URL(destination_header).pathname.slice(1);
	destination = destination.endsWith('/') ? destination.slice(0, -1) : destination;

	// Check if the parent directory exists
	let destination_parent = destination
		.split('/')
		.slice(0, destination.endsWith('/') ? -2 : -1)
		.join('/');
	if (destination_parent !== '') {
		try {
			const parentUrl = getS3Url(env, bucket, encodeURIComponent(destination_parent));
			const parentResponse = await aws.fetch(parentUrl, { method: 'HEAD' });
			
			if (parentResponse.status === 404) {
				return new Response('Conflict', { status: 409 });
			}
		} catch (error: any) {
			return new Response('Conflict', { status: 409 });
		}
	}

	// Check if the destination already exists
	let destination_exists = false;
	try {
		const destUrl = getS3Url(env, bucket, encodeURIComponent(destination));
		const destResponse = await aws.fetch(destUrl, { method: 'HEAD' });
		
		if (destResponse.ok) {
			destination_exists = true;
			if (dont_overwrite) {
				return new Response('Precondition Failed', { status: 412 });
			}
		}
	} catch (error: any) {
		// Destination doesn't exist
	}

	try {
		// Check source resource
		const sourceUrl = getS3Url(env, bucket, encodeURIComponent(resource_path));
		const sourceResponse = await aws.fetch(sourceUrl, { method: 'HEAD' });

		if (sourceResponse.status === 404) {
			return new Response('Not Found', { status: 404 });
		}

		let is_dir = sourceResponse.headers.get('x-amz-meta-resourcetype') === '<collection />';

		if (is_dir) {
			let depth = request.headers.get('Depth') ?? 'infinity';
			switch (depth) {
				case 'infinity': {
					let prefix = resource_path + '/';
					const copy = async (objectKey: string) => {
						let target = destination + '/' + objectKey.slice(prefix.length);
						target = target.endsWith('/') ? target.slice(0, -1) : target;
						
						const copyUrl = getS3Url(env, bucket, encodeURIComponent(target));
						const copyResponse = await aws.fetch(copyUrl, {
							method: 'PUT',
							headers: {
								'x-amz-copy-source': `${bucket}/${objectKey}`,
							},
						});
						
						if (!copyResponse.ok) {
							throw new Error(`S3 copy error: ${copyResponse.status}`);
						}
					};

					// Copy the directory itself
					const copyDirUrl = getS3Url(env, bucket, encodeURIComponent(destination));
					let promise_array: Promise<any>[] = [aws.fetch(copyDirUrl, {
						method: 'PUT',
						headers: {
							'x-amz-copy-source': `${bucket}/${resource_path}`,
						},
					})];

					for await (let object of listAll(aws, env, bucket, prefix, true)) {
						promise_array.push(copy(object.key));
					}
					await Promise.all(promise_array);
					
					return new Response(destination_exists ? null : '', { 
						status: destination_exists ? 204 : 201 
					});
				}
				case '0': {
					const copyUrl = getS3Url(env, bucket, encodeURIComponent(destination));
					const copyResponse = await aws.fetch(copyUrl, {
						method: 'PUT',
						headers: {
							'x-amz-copy-source': `${bucket}/${resource_path}`,
						},
					});
					
					if (!copyResponse.ok) {
						throw new Error(`S3 copy error: ${copyResponse.status}`);
					}
					
					return new Response(destination_exists ? null : '', { 
						status: destination_exists ? 204 : 201 
					});
				}
				default: {
					return new Response('Bad Request', { status: 400 });
				}
			}
		} else {
			const copyUrl = getS3Url(env, bucket, encodeURIComponent(destination));
			const copyResponse = await aws.fetch(copyUrl, {
				method: 'PUT',
				headers: {
					'x-amz-copy-source': `${bucket}/${resource_path}`,
				},
			});
			
			if (!copyResponse.ok) {
				throw new Error(`S3 copy error: ${copyResponse.status}`);
			}
			
			return new Response(destination_exists ? null : '', { 
				status: destination_exists ? 204 : 201 
			});
		}
	} catch (error: any) {
		if (error.message.includes('404')) {
			return new Response('Not Found', { status: 404 });
		}
		throw error;
	}
}

async function handle_move(request: Request, aws: AwsClient, env: Env, bucket: string): Promise<Response> {
	let resource_path = make_resource_path(request);
	let overwrite = request.headers.get('Overwrite') === 'T';
	let destination_header = request.headers.get('Destination');
	if (destination_header === null) {
		return new Response('Bad Request', { status: 400 });
	}
	let destination = new URL(destination_header).pathname.slice(1);
	destination = destination.endsWith('/') ? destination.slice(0, -1) : destination;

	// Check if the parent directory exists
	let destination_parent = destination
		.split('/')
		.slice(0, destination.endsWith('/') ? -2 : -1)
		.join('/');
	if (destination_parent !== '') {
		try {
			const parentUrl = getS3Url(env, bucket, encodeURIComponent(destination_parent));
			const parentResponse = await aws.fetch(parentUrl, { method: 'HEAD' });
			
			if (parentResponse.status === 404) {
				return new Response('Conflict', { status: 409 });
			}
		} catch (error: any) {
			return new Response('Conflict', { status: 409 });
		}
	}

	// Check if the destination already exists
	let destination_exists = false;
	try {
		const destUrl = getS3Url(env, bucket, encodeURIComponent(destination));
		const destResponse = await aws.fetch(destUrl, { method: 'HEAD' });
		
		if (destResponse.ok) {
			destination_exists = true;
			if (!overwrite) {
				return new Response('Precondition Failed', { status: 412 });
			}
		}
	} catch (error: any) {
		// Destination doesn't exist
	}

	try {
		// Check source resource
		const sourceUrl = getS3Url(env, bucket, encodeURIComponent(resource_path));
		const sourceResponse = await aws.fetch(sourceUrl, { method: 'HEAD' });

		if (sourceResponse.status === 404) {
			return new Response('Not Found', { status: 404 });
		}

		if (resource_path === destination) {
			return new Response('Bad Request', { status: 400 });
		}

		if (destination_exists) {
			// Delete the destination first
			await handle_delete(new Request(new URL(destination_header), request), aws, env, bucket);
		}

		let is_dir = sourceResponse.headers.get('x-amz-meta-resourcetype') === '<collection />';

		if (is_dir) {
			let depth = request.headers.get('Depth') ?? 'infinity';
			switch (depth) {
				case 'infinity': {
					let prefix = resource_path + '/';
					const move = async (objectKey: string) => {
						let target = destination + '/' + objectKey.slice(prefix.length);
						target = target.endsWith('/') ? target.slice(0, -1) : target;
						
						// Copy
						const copyUrl = getS3Url(env, bucket, encodeURIComponent(target));
						const copyResponse = await aws.fetch(copyUrl, {
							method: 'PUT',
							headers: {
								'x-amz-copy-source': `${bucket}/${objectKey}`,
							},
						});
						
						if (!copyResponse.ok) {
							throw new Error(`S3 copy error: ${copyResponse.status}`);
						}
						
						// Delete
						const deleteUrl = getS3Url(env, bucket, encodeURIComponent(objectKey));
						await aws.fetch(deleteUrl, { method: 'DELETE' });
					};

					// Move the directory itself
					const copyDirUrl = getS3Url(env, bucket, encodeURIComponent(destination));
					let promise_array: Promise<any>[] = [aws.fetch(copyDirUrl, {
						method: 'PUT',
						headers: {
							'x-amz-copy-source': `${bucket}/${resource_path}`,
						},
					}).then(async () => {
						const deleteUrl = getS3Url(env, bucket, encodeURIComponent(resource_path));
						await aws.fetch(deleteUrl, { method: 'DELETE' });
					})];

					for await (let object of listAll(aws, env, bucket, prefix, true)) {
						promise_array.push(move(object.key));
					}
					await Promise.all(promise_array);
					
					return new Response(destination_exists ? null : '', { 
						status: destination_exists ? 204 : 201 
					});
				}
				case '0': {
					// Copy
					const copyUrl = getS3Url(env, bucket, encodeURIComponent(destination));
					const copyResponse = await aws.fetch(copyUrl, {
						method: 'PUT',
						headers: {
							'x-amz-copy-source': `${bucket}/${resource_path}`,
						},
					});
					
					if (!copyResponse.ok) {
						throw new Error(`S3 copy error: ${copyResponse.status}`);
					}
					
					// Delete
					const deleteUrl = getS3Url(env, bucket, encodeURIComponent(resource_path));
					await aws.fetch(deleteUrl, { method: 'DELETE' });
					
					return new Response(destination_exists ? null : '', { 
						status: destination_exists ? 204 : 201 
					});
				}
				default: {
					return new Response('Bad Request', { status: 400 });
				}
			}
		} else {
			// Copy
			const copyUrl = getS3Url(env, bucket, encodeURIComponent(destination));
			const copyResponse = await aws.fetch(copyUrl, {
				method: 'PUT',
				headers: {
					'x-amz-copy-source': `${bucket}/${resource_path}`,
				},
			});
			
			if (!copyResponse.ok) {
				throw new Error(`S3 copy error: ${copyResponse.status}`);
			}
			
			// Delete
			const deleteUrl = getS3Url(env, bucket, encodeURIComponent(resource_path));
			await aws.fetch(deleteUrl, { method: 'DELETE' });
			
			return new Response(destination_exists ? null : '', { 
				status: destination_exists ? 204 : 201 
			});
		}
	} catch (error: any) {
		if (error.message.includes('404')) {
			return new Response('Not Found', { status: 404 });
		}
		throw error;
	}
}

const DAV_CLASS = '1, 3';
const SUPPORT_METHODS = ['OPTIONS', 'PROPFIND', 'PROPPATCH', 'MKCOL', 'GET', 'HEAD', 'PUT', 'DELETE', 'COPY', 'MOVE'];

async function dispatch_handler(request: Request, aws: AwsClient, env: Env, bucket: string): Promise<Response> {
	switch (request.method) {
		case 'OPTIONS': {
			return new Response(null, {
				status: 204,
				headers: {
					Allow: SUPPORT_METHODS.join(', '),
					DAV: DAV_CLASS,
				},
			});
		}
		case 'HEAD': {
			return await handle_head(request, aws, env, bucket);
		}
		case 'GET': {
			return await handle_get(request, aws, env, bucket);
		}
		case 'PUT': {
			return await handle_put(request, aws, env, bucket);
		}
		case 'DELETE': {
			return await handle_delete(request, aws, env, bucket);
		}
		case 'MKCOL': {
			return await handle_mkcol(request, aws, env, bucket);
		}
		case 'PROPFIND': {
			return await handle_propfind(request, aws, env, bucket);
		}
		case 'PROPPATCH': {
			return await handle_proppatch(request, aws, env, bucket);
		}
		case 'COPY': {
			return await handle_copy(request, aws, env, bucket);
		}
		case 'MOVE': {
			return await handle_move(request, aws, env, bucket);
		}
		default: {
			return new Response('Method Not Allowed', {
				status: 405,
				headers: {
					Allow: SUPPORT_METHODS.join(', '),
					DAV: DAV_CLASS,
				},
			});
		}
	}
}

function is_authorized(authorization_header: string, username: string, password: string): boolean {
    const encoder = new TextEncoder();

    const header = encoder.encode(authorization_header);
    const expected = encoder.encode(`Basic ${btoa(`${username}:${password}`)}`);

    return header.byteLength === expected.byteLength && crypto.subtle.timingSafeEqual(header, expected);
}

export default {
	async fetch(request: Request, env: Env, _ctx: ExecutionContext): Promise<Response> {
		const aws = createS3Client(env);
		const bucket = env.S3_BUCKET;

		if (
			request.method !== 'OPTIONS' &&
			!is_authorized(request.headers.get('Authorization') ?? '', env.USERNAME, env.PASSWORD)
		) {
			return new Response('Unauthorized', {
				status: 401,
				headers: {
					'WWW-Authenticate': 'Basic realm="webdav"',
				},
			});
		}

		let response: Response = await dispatch_handler(request, aws, env, bucket);

		// Set CORS headers
		response.headers.set('Access-Control-Allow-Origin', request.headers.get('Origin') ?? '*');
		response.headers.set('Access-Control-Allow-Methods', SUPPORT_METHODS.join(', '));
		response.headers.set(
			'Access-Control-Allow-Headers',
			['authorization', 'content-type', 'depth', 'overwrite', 'destination', 'range'].join(', '),
		);
		response.headers.set(
			'Access-Control-Expose-Headers',
			['content-type', 'content-length', 'dav', 'etag', 'last-modified', 'location', 'date', 'content-range'].join(
				', ',
			),
		);
		response.headers.set('Access-Control-Allow-Credentials', 'false');
		response.headers.set('Access-Control-Max-Age', '86400');

		return response;
	},
};