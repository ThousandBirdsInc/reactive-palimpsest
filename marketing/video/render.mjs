// Renders scenes.html frame-by-frame with headless Chromium and pipes
// JPEG frames into ffmpeg. Usage:
//   node render.mjs stills <outdir> t1 t2 ...   # PNG stills at given seconds
//   node render.mjs video <out.mp4> [fps]
import { createRequire } from 'module';
import { spawn } from 'child_process';
import path from 'path';
import { fileURLToPath } from 'url';
const require = createRequire(import.meta.url);
const { chromium } = require('playwright');
const here = path.dirname(fileURLToPath(import.meta.url));
const [mode, out, ...rest] = process.argv.slice(2);

const browser = await chromium.launch();
const page = await browser.newPage({ viewport: { width: 1920, height: 1080 } });
await page.goto('file://' + path.join(here, 'scenes.html'));
await page.evaluate(() => window.ready);
const total = await page.evaluate(() => window.TOTAL);

if (mode === 'stills') {
  for (const t of rest) {
    await page.evaluate((t) => window.render(t), +t);
    await page.screenshot({ path: path.join(out, `t${String(t).padStart(6, '0')}.png`) });
  }
} else {
  const fps = +(rest[0] || 30);
  const frames = Math.round(total * fps);
  const ff = spawn('ffmpeg', ['-y', '-loglevel', 'error', '-f', 'image2pipe', '-framerate', String(fps), '-c:v', 'mjpeg', '-i', '-',
    '-c:v', 'libx264', '-preset', 'medium', '-crf', '18', '-pix_fmt', 'yuv420p', out], { stdio: ['pipe', 'inherit', 'inherit'] });
  for (let f = 0; f < frames; f++) {
    await page.evaluate((t) => window.render(t), f / fps);
    const buf = await page.screenshot({ type: 'jpeg', quality: 95 });
    if (!ff.stdin.write(buf)) await new Promise((r) => ff.stdin.once('drain', r));
    if (f % 300 === 0) console.log(`frame ${f}/${frames}`);
  }
  ff.stdin.end();
  await new Promise((r) => ff.on('close', r));
}
await browser.close();
