// https://vercel.com/templates/other/fastify-serverless-function
import cookie from '@fastify/cookie';
import oauthPlugin, {type OAuth2Namespace} from '@fastify/oauth2';
import {Octokit} from '@octokit/core';
import '@dotenvx/dotenvx/config';
import Fastify, {type FastifyReply, type FastifyRequest} from 'fastify';
import {jwtVerify, SignJWT, type JWK} from 'jose';
import {nanoid} from 'nanoid';
import postgres from 'postgres';
import {handlePush} from '../server/push-handler.ts';
import {must} from '../../../packages/shared/src/must.ts';
import assert from 'assert';
import {authDataSchema, type AuthData} from '../shared/auth.ts';
import type {ReadonlyJSONValue, Row} from '@rocicorp/zero';
import {parseIssueId} from '../shared/id-parse.ts';
import type {Schema} from '../shared/schema.ts';

declare module 'fastify' {
  interface FastifyInstance {
    githubOAuth2: OAuth2Namespace;
  }
}

const sql = postgres(process.env.ZERO_UPSTREAM_DB as string);
type QueryParams = {redirect?: string | undefined};
let privateJwk: JWK | undefined;

export const fastify = Fastify({
  logger: true,
});

fastify.register(cookie);

fastify.register(oauthPlugin, {
  name: 'githubOAuth2',
  credentials: {
    client: {
      id: process.env.GITHUB_CLIENT_ID as string,
      secret: process.env.GITHUB_CLIENT_SECRET as string,
    },
    // eslint-disable-next-line @typescript-eslint/ban-ts-comment
    // @ts-ignore Not clear why this is not working when type checking with tsconfig.node.ts
    auth: oauthPlugin.GITHUB_CONFIGURATION,
  },
  startRedirectPath: '/api/login/github',
  callbackUri: req =>
    `${req.protocol}://${req.hostname}${
      req.port != null ? ':' + req.port : ''
    }/api/login/github/callback${
      (req.query as QueryParams).redirect
        ? `?redirect=${(req.query as QueryParams).redirect}`
        : ''
    }`,
});

fastify.get<{
  Querystring: QueryParams;
}>('/api/login/github/callback', async function (request, reply) {
  if (!privateJwk) {
    privateJwk = JSON.parse(process.env.PRIVATE_JWK as string) as JWK;
  }
  const {token} =
    await this.githubOAuth2.getAccessTokenFromAuthorizationCodeFlow(request);

  const octokit = new Octokit({
    auth: token.access_token,
  });

  const userDetails = await octokit.request('GET /user', {
    headers: {
      'X-GitHub-Api-Version': '2022-11-28',
    },
  });

  let userId = nanoid();
  const existingUser =
    await sql`SELECT id, email FROM "user" WHERE "githubID" = ${userDetails.data.id}`;
  if (existingUser.length > 0) {
    userId = existingUser[0].id;
    // update email on login if it has changed
    if (existingUser[0].email !== userDetails.data.email) {
      await sql`UPDATE "user" SET "email" = ${userDetails.data.email} WHERE "id" = ${userId}`;
    }
  } else {
    await sql`INSERT INTO "user"
      ("id", "login", "name", "avatar", "githubID", "email") VALUES (
        ${userId},
        ${userDetails.data.login},
        ${userDetails.data.name},
        ${userDetails.data.avatar_url},
        ${userDetails.data.id},
        ${userDetails.data.email}
      )`;
  }

  const userRows = await sql`SELECT * FROM "user" WHERE "id" = ${userId}`;

  const jwtPayload: AuthData = {
    sub: userId,
    iat: Math.floor(Date.now() / 1000),
    role: userRows[0].role,
    name: userDetails.data.login,
    exp: 0, // setExpirationTime below sets it
  };

  const jwt = await new SignJWT(jwtPayload)
    .setProtectedHeader({alg: must(privateJwk.alg)})
    .setExpirationTime('30days')
    .sign(privateJwk);

  reply
    .cookie('jwt', jwt, {
      path: '/',
      expires: new Date(Date.now() + 30 * 24 * 60 * 60 * 1000),
    })
    .redirect(
      request.query.redirect ? decodeURIComponent(request.query.redirect) : '/',
    );
});

fastify.post<{
  Querystring: Record<string, string>;
  Body: ReadonlyJSONValue;
}>('/api/push', async function (request, reply) {
  let {authorization} = request.headers;
  if (authorization !== undefined) {
    assert(authorization.toLowerCase().startsWith('bearer '));
    authorization = authorization.substring('Bearer '.length);
  }

  const jwk = process.env.VITE_PUBLIC_JWK;
  let authData: AuthData | undefined;
  try {
    authData =
      jwk && authorization
        ? authDataSchema.parse(
            (await jwtVerify(authorization, JSON.parse(jwk))).payload,
          )
        : undefined;
  } catch (e) {
    if (e instanceof Error) {
      reply.status(401).send(e.message);
      return;
    }
    throw e;
  }

  const response = await handlePush(authData, request.query, request.body);
  reply.send(response);
});

type Issue = Row<Schema['tables']['issue']>;
fastify.get<{
  Params: {issueId: string};
}>('/api/issue/:issueId', async function (request, reply) {
  const [idField, id] = parseIssueId(request.params.issueId);

  const issues = await sql<
    Issue[]
  >`SELECT * FROM "issue" WHERE ${sql(idField)} = ${id}`;
  if (issues.length === 0) {
    reply.status(404).send('Not found');
    return;
  }
  const issue = issues[0];

  if (issue.visibility !== 'public') {
    reply.status(404).send('Not found');
    return;
  }

  const html = generateIssueSocialPreview(issue);
  return reply
    .code(200)
    .header('Content-Type', 'text/html; charset=utf-8')
    .send(html);
});

export default async function handler(
  req: FastifyRequest,
  reply: FastifyReply,
) {
  await fastify.ready();
  fastify.server.emit('request', req, reply);
}

function generateIssueSocialPreview(
  issue: Issue,
  baseUrl = 'https://bugs.rocicorp.dev',
) {
  const escapeHTML = (unsafe: string) => {
    return unsafe
      .replace(/&/g, '&amp;')
      .replace(/</g, '&lt;')
      .replace(/>/g, '&gt;')
      .replace(/"/g, '&quot;')
      .replace(/'/g, '&#039;');
  };

  const truncatedDescription =
    issue.description.length > 160
      ? issue.description.substring(0, 157) + '...'
      : issue.description;

  const titleWithId = `#${issue.shortID || issue.id} - ${issue.title}`;
  const issueUrl = `${baseUrl}/issues/${issue.id}`;
  const status = issue.open ? 'Open' : 'Closed';

  return `<!DOCTYPE html>
<html>
<head>
  <title>${escapeHTML(issue.title)}</title>
  
  <!-- Primary Meta Tags -->
  <meta name="title" content="${escapeHTML(titleWithId)}">
  <meta name="description" content="${escapeHTML(truncatedDescription)}">
  
  <!-- Open Graph / Facebook -->
  <meta property="og:type" content="website">
  <meta property="og:url" content="${issueUrl}">
  <meta property="og:title" content="${escapeHTML(titleWithId)}">
  <meta property="og:description" content="${escapeHTML(truncatedDescription)}">
  <meta property="og:image" content="${baseUrl}/assets/logo-CBxyi1z2.svg">
  <meta property="og:site_name" content="Rocicorp Bug Tracker">
  
  <!-- Twitter -->
  <meta property="twitter:card" content="summary_large_image">
  <meta property="twitter:url" content="${issueUrl}">
  <meta property="twitter:title" content="${escapeHTML(titleWithId)}">
  <meta property="twitter:description" content="${escapeHTML(truncatedDescription)}">
  <meta property="twitter:image" content="${baseUrl}/assets/logo-CBxyi1z2.svg">
  
  <!-- Additional Meta Data -->
  <meta name="issue:id" content="${issue.id}">
  <meta name="issue:status" content="${status}">
  <meta name="issue:visibility" content="${issue.visibility}">
</head>
<body>
</body>
</html>`;
}
