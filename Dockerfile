FROM node:20-bookworm-slim

# مكتبات حزمة canvas + بناء الحزم الأصلية (sqlite3) + libuuid
RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential python3 pkg-config \
    libcairo2-dev libpango1.0-dev libjpeg-dev libgif-dev librsvg2-dev \
    libuuid1 fonts-dejavu-core \
 && rm -rf /var/lib/apt/lists/*

WORKDIR /app
COPY package*.json ./
COPY scripts ./scripts
RUN npm install --omit=dev

COPY . .

ENV NODE_ENV=production
# Railway يمرّر PORT تلقائياً، واللوحة تقرؤه
CMD ["npm", "start"]
