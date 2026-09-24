#!/bin/sh
# 复核入口：代码测试 -> 前端构建检查 -> API/HTTP 冒烟。
# 任一步骤失败即以其退出码终止（set -e），全部通过退出码为 0。
set -eu

cd "$(dirname "$0")/.."

PY=$(command -v python || command -v python3)

echo "==================== 1/3 代码测试 ===================="
"$PY" -m unittest discover -s tests -v

echo "==================== 2/3 前端构建检查 ===================="
node frontend/build.mjs --check

echo "==================== 3/3 API/HTTP 冒烟 ===================="
"$PY" verify/smoke.py

echo "==================== 复核全部通过 ===================="
