# 零第三方依赖：仅使用 Python 3.11 标准库
FROM python:3.11-slim

WORKDIR /app

# 应用代码（只读拷贝）
COPY app/ ./app/
COPY run_core.py run_ingress.py run_dispatcher.py run_sim.py ./

# 数据目录：SQLite WAL 文件挂卷到这里，容器重建不丢状态
RUN mkdir -p /data
ENV SHADOW_DB_PATH=/data/shadow.db \
    PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1

EXPOSE 8080 8081

# 默认启动 core；ingress / dispatcher 在 compose 里用 command 覆盖
CMD ["python3", "run_core.py"]
