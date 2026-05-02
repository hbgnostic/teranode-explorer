FROM node:22-alpine
WORKDIR /app

COPY package.json package-lock.json* ./
RUN npm install --no-audit --no-fund --omit=dev || npm install --no-audit --no-fund

COPY . .

ENV PORT=8080
EXPOSE 8080

CMD ["npx", "tsx", "index.ts"]
