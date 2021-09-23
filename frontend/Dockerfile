FROM node:16.4.2 as frontend-build-deps
WORKDIR /
COPY package.json yarn.lock ./
RUN yarn install
COPY . /
CMD ["yarn", "start"]