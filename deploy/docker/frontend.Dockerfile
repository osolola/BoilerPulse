FROM node:22-alpine AS build
WORKDIR /src
COPY frontend/package.json frontend/package-lock.json ./
RUN npm ci
COPY frontend .
RUN npm run build

FROM node:22-alpine
WORKDIR /app
ENV NODE_ENV=production
COPY --from=build /src/public ./public
COPY --from=build /src/.next ./.next
COPY --from=build /src/package.json ./package.json
COPY --from=build /src/node_modules ./node_modules
EXPOSE 3000
CMD ["npm", "start"]
