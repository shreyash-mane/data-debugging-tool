FROM node:20-alpine AS builder

WORKDIR /app
COPY package*.json ./
RUN npm install
COPY . .

# Build with the backend URL pointing to the Docker service
ARG VITE_API_URL=http://localhost:8000
RUN npm run build

FROM nginx:alpine
COPY --from=builder /app/dist /usr/share/nginx/html

# Simple nginx config that rewrites /api/* to the backend
COPY nginx.conf /etc/nginx/conf.d/default.conf

EXPOSE 80
