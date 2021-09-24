FROM node:alpine as builder
COPY frontend/package.json ./
COPY frontend/yarn.lock ./
RUN yarn install --frozen-lockfile
COPY /frontend .
RUN yarn build

FROM nginx:alpine
COPY --from=builder ./build /usr/share/nginx/html



