FROM node:20-bullseye-slim AS runner
WORKDIR /app
ENV NODE_ENV=production
ENV TS_NODE_TRANSPILE_ONLY=1
RUN apt-get update && apt-get install -y --no-install-recommends \
    python3 \
    make \
    g++ \
    pkg-config \
  && rm -rf /var/lib/apt/lists/*
COPY package.json ./
RUN npm install --include=dev
COPY . .
RUN npm run build
EXPOSE 8080 26210 26211 51472 51473 22555 22556
CMD ["node", "dist/index.js"]
