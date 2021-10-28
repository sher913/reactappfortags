FROM node:16.4.2 as builder
COPY frontend/package.json ./
COPY frontend/yarn.lock ./
RUN yarn install --frozen-lockfile
COPY /frontend .
RUN yarn build

FROM nginx:alpine
COPY --from=builder ./build /usr/share/nginx/html



