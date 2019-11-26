FROM node:alpine

WORKDIR /kaiko-vwap-adapter
ADD . .

RUN apk add --no-cache git
RUN npm install

ENV EA_PORT=8080

CMD node ./app.js
