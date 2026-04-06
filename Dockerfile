FROM node:18-alpine
WORKDIR /app
COPY package.json package-lock.json* ./
RUN npm ci --only=production
COPY . .
ENV PORT=8080
CMD ["node", "index.js"]
