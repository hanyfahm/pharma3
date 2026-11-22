import express from 'express';
import http from 'http';
import fs from 'fs';
import path from 'path';
import os from 'os';
import { WebSocketServer, WebSocket } from 'ws';
import JSZip from 'jszip';
import type { AppDB, TeamMember } from './src/types/pharma.ts';
import { executePharmaAction, makeSeed, normalizeDB } from './src/utils/pharmaEngine.ts';
import type { PharmaAction } from './src/utils/pharmaEngine.ts';

interface PersistedState {
  rev: number;
  updatedAt: string;
  updatedBy?: string;
  db: AppDB;
}

const PORT = Number(process.env.PORT) || 3000;
const DATA_DIR = path.resolve(process.cwd(), 'data');
const DATA_FILE = path.join(DATA_DIR, 'pharma-db.json');

function ensureDataDir() {
  if (!fs.existsSync(DATA_DIR)) {
    fs.mkdirSync(DATA_DIR, { recursive: true });
  }
}

function saveStateToDisk(state: PersistedState) {
  ensureDataDir();
  const tmpFile = `${DATA_FILE}.tmp`;
  fs.writeFileSync(tmpFile, JSON.stringify(state, null, 2), 'utf8');
  fs.renameSync(tmpFile, DATA_FILE);
}

function loadOrInitState(): PersistedState {
  ensureDataDir();
  try {
    if (fs.existsSync(DATA_FILE)) {
      const raw = fs.readFileSync(DATA_FILE, 'utf8');
      const parsed = JSON.parse(raw) as Partial<PersistedState> & Partial<AppDB>;
      if (parsed && parsed.db && parsed.db.version === 3 && Array.isArray(parsed.db.warehouses)) {
        return {
          rev: typeof parsed.rev === 'number' ? parsed.rev : 1,
          updatedAt: parsed.updatedAt || new Date().toISOString(),
          updatedBy: parsed.updatedBy,
          db: normalizeDB(parsed.db)
        };
      }
      if (parsed && parsed.version === 3 && Array.isArray(parsed.warehouses)) {
        return {
          rev: 1,
          updatedAt: new Date().toISOString(),
          db: normalizeDB(parsed as AppDB)
        };
      }
    }
  } catch (err) {
    console.error('[pharma-server] Failed reading existing DB, re-seeding:', err);
  }

  const initial: PersistedState = {
    rev: 1,
    updatedAt: new Date().toISOString(),
    db: normalizeDB(makeSeed())
  };
  saveStateToDisk(initial);
  return initial;
}

const state: PersistedState = loadOrInitState();

interface ClientMeta extends TeamMember {
  isAlive: boolean;
}

const clients = new Map<WebSocket, ClientMeta>();

function sanitizeName(name: unknown): string {
  const s = typeof name === 'string' ? name.trim().slice(0, 40) : '';
  return s || 'عضو الفريق';
}

function getMembersList(): TeamMember[] {
  const seen = new Map<string, TeamMember>();
  for (const meta of clients.values()) {
    seen.set(meta.clientId, {
      clientId: meta.clientId,
      name: meta.name,
      joinedAt: meta.joinedAt
    });
  }
  return Array.from(seen.values());
}

function broadcast(payload: unknown) {
  const msg = JSON.stringify(payload);
  for (const [ws] of clients) {
    if (ws.readyState === WebSocket.OPEN) {
      ws.send(msg);
    }
  }
}

function broadcastPresence() {
  broadcast({
    type: 'presence:list',
    members: getMembersList()
  });
}

async function startServer() {
  const app = express();
  app.use(express.json({ limit: '15mb' }));

  app.get('/api/health', (_req, res) => {
    res.json({
      ok: true,
      rev: state.rev,
      updatedAt: state.updatedAt,
      activeMembers: getMembersList().length
    });
  });

  app.get('/api/db', (_req, res) => {
    res.json({
      ok: true,
      rev: state.rev,
      updatedAt: state.updatedAt,
      updatedBy: state.updatedBy,
      db: state.db,
      members: getMembersList()
    });
  });

  app.post('/api/action', (req, res) => {
    try {
      const body = req.body as {
        action?: PharmaAction;
        actor?: string;
        clientId?: string;
      };
      if (!body?.action || typeof body.action.type !== 'string') {
        res.status(400).json({ ok: false, error: 'طلب غير صالح' });
        return;
      }

      const actor = sanitizeName(body.actor);
      const { db: nextDb, result, summary } = executePharmaAction(state.db, body.action, actor);

      state.db = nextDb;
      state.rev += 1;
      state.updatedAt = new Date().toISOString();
      state.updatedBy = actor;
      saveStateToDisk(state);

      broadcast({
        type: 'db:updated',
        rev: state.rev,
        updatedAt: state.updatedAt,
        updatedBy: actor,
        clientId: body.clientId,
        summary,
        db: state.db
      });

      res.json({
        ok: true,
        rev: state.rev,
        updatedAt: state.updatedAt,
        updatedBy: actor,
        summary,
        result,
        db: state.db
      });
    } catch (err) {
      const message = err instanceof Error ? err.message : 'تعذر تنفيذ العملية';
      res.status(400).json({ ok: false, error: message });
    }
  });

  const STANDALONE_DIR = path.resolve(process.cwd(), 'dist-standalone');
  let standaloneBuildPromise: Promise<string> | null = null;

  function getLatestMtimeMs(dir: string): number {
    let latest = 0;
    if (!fs.existsSync(dir)) return 0;
    for (const entry of fs.readdirSync(dir, { withFileTypes: true })) {
      const full = path.join(dir, entry.name);
      if (entry.isDirectory()) {
        latest = Math.max(latest, getLatestMtimeMs(full));
      } else {
        latest = Math.max(latest, fs.statSync(full).mtimeMs);
      }
    }
    return latest;
  }

  async function buildStandaloneTemplateHtml(): Promise<string> {
    const outIndex = path.join(STANDALONE_DIR, 'index.html');
    const srcMtime = Math.max(
      getLatestMtimeMs(path.resolve(process.cwd(), 'src')),
      fs.existsSync(path.resolve(process.cwd(), 'index.html'))
        ? fs.statSync(path.resolve(process.cwd(), 'index.html')).mtimeMs
        : 0
    );
    const needBuild =
      !fs.existsSync(outIndex) || fs.statSync(outIndex).mtimeMs < srcMtime;

    if (needBuild) {
      if (!standaloneBuildPromise) {
        standaloneBuildPromise = (async () => {
          const { build } = await import('vite');
          await build({
            logLevel: 'silent',
            build: {
              outDir: 'dist-standalone',
              emptyOutDir: true
            }
          });
          return outIndex;
        })().finally(() => {
          standaloneBuildPromise = null;
        });
      }
      await standaloneBuildPromise;
    }

    let html = fs.readFileSync(outIndex, 'utf8');

    html = html.replace(
      /<link\b[^>]*rel=["']stylesheet["'][^>]*href=["']([^"']+)["'][^>]*>/gi,
      (_match, href: string) => {
        const relPath = href.replace(/^\//, '');
        const filePath = path.join(STANDALONE_DIR, relPath);
        if (!fs.existsSync(filePath)) return '';
        const css = fs.readFileSync(filePath, 'utf8');
        return `<style>${css}</style>`;
      }
    );

    html = html.replace(
      /<script\b([^>]*)src=["']([^"']+)["'][^>]*><\/script>/gi,
      (_match, _attrs: string, src: string) => {
        const relPath = src.replace(/^\//, '');
        const filePath = path.join(STANDALONE_DIR, relPath);
        if (!fs.existsSync(filePath)) return '';
        const js = fs.readFileSync(filePath, 'utf8').replace(/<\/script/gi, '<\\/script');
        return `<script type="module">${js}</script>`;
      }
    );

    return html;
  }

  async function handleDownloadStandaloneHtml(req: express.Request, res: express.Response) {
    try {
      const bodyDb = (req.body as { db?: AppDB } | undefined)?.db;
      const dbToEmbed =
        bodyDb && bodyDb.version === 3 && Array.isArray(bodyDb.warehouses)
          ? normalizeDB(bodyDb)
          : state.db;

      const templateHtml = await buildStandaloneTemplateHtml();
      const today = new Date().toISOString().slice(0, 10);
      const snapshotKey = `${today}-${Date.now().toString(36)}`;
      const safeJsonDb = JSON.stringify(dbToEmbed).replace(/</g, '\\u003c');
      const bootstrapScript = `<script>window.__PHARMA_STANDALONE__=true;window.__PHARMA_EMBEDDED_KEY__=${JSON.stringify(snapshotKey)};window.__PHARMA_EMBEDDED_DB__=${safeJsonDb};</script>`;

      const finalHtml = templateHtml.includes('</head>')
        ? templateHtml.replace('</head>', () => `${bootstrapScript}\n</head>`)
        : `${bootstrapScript}\n${templateHtml}`;

      const arName = `صيدليتي_التطبيق_الكامل_${today}.html`;
      res.setHeader('Content-Type', 'text/html; charset=utf-8');
      res.setHeader(
        'Content-Disposition',
        `attachment; filename="saydaliyati-standalone-${today}.html"; filename*=UTF-8''${encodeURIComponent(arName)}`
      );
      res.send(finalHtml);
    } catch (err) {
      console.error('[pharma-server] Standalone HTML build failed:', err);
      res.status(500).json({ ok: false, error: 'تعذر تجهيز ملف التطبيق المستقل' });
    }
  }

  app.get('/api/download-app-html', handleDownloadStandaloneHtml);
  app.post('/api/download-app-html', handleDownloadStandaloneHtml);

  app.get('/api/download-app-zip', async (_req, res) => {
    try {
      const zip = new JSZip();
      const rootDir = process.cwd();

      const rootFiles = [
        'package.json',
        'tsconfig.json',
        'vite.config.ts',
        'index.html',
        'server.ts',
        'metadata.json',
        '.env.example',
        '.gitignore'
      ];

      for (const file of rootFiles) {
        const full = path.join(rootDir, file);
        if (fs.existsSync(full)) {
          zip.file(file, fs.readFileSync(full));
        }
      }

      const addDirRecursive = (absDir: string, zipBase: string) => {
        if (!fs.existsSync(absDir)) return;
        for (const entry of fs.readdirSync(absDir, { withFileTypes: true })) {
          if (entry.name === 'node_modules' || entry.name === 'dist' || entry.name === 'dist-standalone') {
            continue;
          }
          const full = path.join(absDir, entry.name);
          const rel = path.posix.join(zipBase, entry.name);
          if (entry.isDirectory()) {
            addDirRecursive(full, rel);
          } else {
            zip.file(rel, fs.readFileSync(full));
          }
        }
      };

      addDirRecursive(path.join(rootDir, 'src'), 'src');

      zip.file('data/pharma-db.json', JSON.stringify(state, null, 2));

      zip.file(
        'تشغيل_التطبيق_ويندوز.bat',
        `@echo off\r\nchcp 65001 >nul\r\necho ==========================================\r\necho   Saydaliyati - Pharmacy Warehouse System\r\necho ==========================================\r\nwhere node >nul 2>nul\r\nif %errorlevel% neq 0 (\r\n  echo [ERROR] Please install Node.js v20+ from https://nodejs.org\r\n  pause\r\n  exit /b 1\r\n)\r\nif not exist node_modules (\r\n  echo Installing dependencies for first run...\r\n  call npm install\r\n)\r\nstart "" http://localhost:3000\r\ncall npm run dev\r\npause\r\n`
      );

      zip.file(
        'start.sh',
        `#!/usr/bin/env bash\nset -e\nif [ ! -d node_modules ]; then\n  npm install\nfi\nnpm run dev\n`
      );

      zip.file(
        'README-AR.txt',
        `تطبيق صيدليتي — إدارة المخازن والفواتير\n==========================================\n\nطريقة التشغيل على جهازك:\n1. تأكد من تثبيت Node.js (الإصدار 20 أو أحدث) من https://nodejs.org\n2. على ويندوز: اضغط مرتين على ملف "تشغيل_التطبيق_ويندوز.bat"\n   أو عبر موجه الأوامر (Terminal):\n   npm install\n   npm run dev\n3. افتح المتصفح على العنوان: http://localhost:3000\n\nملاحظة:\n- بياناتك الحالية محفوظة في الملف: data/pharma-db.json\n- للعمل الجماعي عبر الشبكة المحلية، يطبع التطبيق رابط الشبكة تلقائياً عند التشغيل.\n`
      );

      const buf = await zip.generateAsync({
        type: 'nodebuffer',
        compression: 'DEFLATE',
        compressionOptions: { level: 6 }
      });

      const today = new Date().toISOString().slice(0, 10);
      res.setHeader('Content-Type', 'application/zip');
      res.setHeader(
        'Content-Disposition',
        `attachment; filename="saydaliyati-full-app-${today}.zip"`
      );
      res.send(buf);
    } catch (err) {
      console.error('[pharma-server] ZIP export failed:', err);
      res.status(500).json({ ok: false, error: 'تعذر تجهيز ملف المشروع المضغوط' });
    }
  });

  const distDir = path.resolve(process.cwd(), 'dist');
  const distIndex = path.join(distDir, 'index.html');
  const isDev =
    process.env.npm_lifecycle_event === 'dev' ||
    process.argv.includes('--dev') ||
    (process.env.NODE_ENV !== 'production' && !fs.existsSync(distIndex));

  if (isDev) {
    const { createServer: createViteServer } = await import('vite');
    const vite = await createViteServer({
      server: { middlewareMode: true },
      appType: 'spa'
    });
    app.use(vite.middlewares);
  } else {
    app.use(express.static(distDir));
    app.get('*', (_req, res) => {
      res.sendFile(distIndex);
    });
  }

  const server = http.createServer(app);
  const wss = new WebSocketServer({ noServer: true });

  server.on('upgrade', (req, socket, head) => {
    try {
      const pathname = new URL(req.url || '/', 'http://localhost').pathname;
      if (pathname !== '/ws') {
        return;
      }
      wss.handleUpgrade(req, socket, head, ws => {
        wss.emit('connection', ws, req);
      });
    } catch {
      socket.destroy();
    }
  });

  wss.on('connection', ws => {
    const meta: ClientMeta = {
      clientId: 'm-' + Math.random().toString(36).slice(2, 9),
      name: 'عضو الفريق',
      joinedAt: new Date().toISOString(),
      isAlive: true
    };
    clients.set(ws, meta);

    ws.send(
      JSON.stringify({
        type: 'welcome',
        rev: state.rev,
        updatedAt: state.updatedAt,
        members: getMembersList()
      })
    );
    broadcastPresence();

    ws.on('pong', () => {
      const current = clients.get(ws);
      if (current) current.isAlive = true;
    });

    ws.on('message', raw => {
      try {
        const msg = JSON.parse(String(raw)) as {
          type?: string;
          clientId?: string;
          name?: string;
        };
        const current = clients.get(ws);
        if (!current) return;

        if (msg.type === 'hello' || msg.type === 'set-name') {
          if (typeof msg.clientId === 'string' && msg.clientId.trim()) {
            current.clientId = msg.clientId.trim().slice(0, 48);
          }
          current.name = sanitizeName(msg.name);
          broadcastPresence();
        } else if (msg.type === 'ping') {
          ws.send(JSON.stringify({ type: 'pong' }));
        }
      } catch {
        // ignore malformed client messages
      }
    });

    ws.on('close', () => {
      clients.delete(ws);
      broadcastPresence();
    });

    ws.on('error', () => {
      clients.delete(ws);
      broadcastPresence();
    });
  });

  const heartbeat = setInterval(() => {
    let changed = false;
    for (const [ws, meta] of clients) {
      if (!meta.isAlive) {
        clients.delete(ws);
        ws.terminate();
        changed = true;
        continue;
      }
      meta.isAlive = false;
      try {
        ws.ping();
      } catch {
        clients.delete(ws);
        changed = true;
      }
    }
    if (changed) broadcastPresence();
  }, 25000);

  server.on('close', () => {
    clearInterval(heartbeat);
  });

  server.listen(PORT, '0.0.0.0', () => {
    console.log(`\n[صيدليتي] الخادم المركزي يعمل على: http://localhost:${PORT}`);
    const nets = os.networkInterfaces();
    for (const ifaceList of Object.values(nets)) {
      if (!ifaceList) continue;
      for (const net of ifaceList) {
        if (net.family === 'IPv4' && !net.internal) {
          console.log(`[صيدليتي] رابط الشبكة المحلية للفريق: http://${net.address}:${PORT}`);
        }
      }
    }
    console.log(`[صيدليتي] ملف قاعدة البيانات المشتركة: ${DATA_FILE}\n`);
  });
}

startServer().catch(err => {
  console.error('[pharma-server] Fatal startup error:', err);
  process.exit(1);
});
