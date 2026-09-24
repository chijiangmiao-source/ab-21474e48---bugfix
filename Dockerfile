# 低温阵列扫描监控 —— 应用镜像
# 同一镜像用于 app 服务与 verify 复核服务（verify 以不同 command 启动）。
FROM python:3.12-alpine

# nodejs 供前端构建/检查使用；运行期仅依赖 Python 标准库
RUN apk add --no-cache nodejs

WORKDIR /srv

COPY app ./app
COPY frontend ./frontend
COPY tests ./tests
COPY verify ./verify

# 镜像构建期完成前端构建，容器启动即可服务监看页
RUN node frontend/build.mjs && mkdir -p /srv/data

ENV PORT=8080 \
    DB_PATH=/srv/data/app.db

EXPOSE 8080

CMD ["python", "app/server.py"]
