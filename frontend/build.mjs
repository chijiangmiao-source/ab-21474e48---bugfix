#!/usr/bin/env node
/* 前端构建脚本（零依赖）。
 *
 * 默认模式：校验 src/ 下 JS 语法与 HTML 资源引用，产出 dist/。
 * --check 模式：仅做校验并确认 dist 产物存在且非空，失败时以非零码退出。
 */
"use strict";

import fs from "node:fs";
import path from "node:path";
import vm from "node:vm";
import { fileURLToPath } from "node:url";

const SRC = path.join(path.dirname(fileURLToPath(import.meta.url)), "src");
const DIST = path.join(path.dirname(fileURLToPath(import.meta.url)), "dist");
const JS_FILES = ["app.js"];
const STATIC_FILES = ["index.html", "styles.css"];
const CHECK_ONLY = process.argv.includes("--check");

function fail(msg) {
  console.error(`[frontend-build] 失败: ${msg}`);
  process.exit(1);
}

function validateJs(filename) {
  const file = path.join(SRC, filename);
  if (!fs.existsSync(file)) fail(`缺少源文件 ${filename}`);
  const code = fs.readFileSync(file, "utf8");
  try {
    new vm.Script(code, { filename }); // 仅编译不执行，捕获语法错误
  } catch (err) {
    fail(`${filename} 语法错误: ${err.message}`);
  }
  return code;
}

function validateHtml() {
  const file = path.join(SRC, "index.html");
  if (!fs.existsSync(file)) fail("缺少 index.html");
  const html = fs.readFileSync(file, "utf8");
  for (const ref of ["/app.js", "/styles.css"]) {
    if (!html.includes(ref)) fail(`index.html 未引用 ${ref}`);
  }
  return html;
}

function checkDist() {
  for (const name of [...STATIC_FILES, ...JS_FILES]) {
    const p = path.join(DIST, name);
    if (!fs.existsSync(p)) fail(`dist 缺少产物 ${name}，请先运行构建`);
    if (fs.statSync(p).size === 0) fail(`dist 产物 ${name} 为空`);
  }
}

function main() {
  const html = validateHtml();
  const jsSources = JS_FILES.map(validateJs);
  const css = fs.readFileSync(path.join(SRC, "styles.css"), "utf8");
  if (!css.trim()) fail("styles.css 为空");

  if (CHECK_ONLY) {
    checkDist();
    console.log("[frontend-build] 校验通过：源码语法正确，dist 产物完整");
    return;
  }

  fs.rmSync(DIST, { recursive: true, force: true });
  fs.mkdirSync(DIST, { recursive: true });
  const stamp = `/* built ${new Date().toISOString()} */\n`;
  fs.writeFileSync(path.join(DIST, "index.html"), html);
  fs.writeFileSync(path.join(DIST, "styles.css"), css);
  JS_FILES.forEach((name, i) => {
    fs.writeFileSync(path.join(DIST, name), stamp + jsSources[i]);
  });
  console.log(`[frontend-build] 构建完成 -> ${DIST}`);
}

main();
