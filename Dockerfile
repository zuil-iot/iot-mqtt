FROM node:boron

MAINTAINER mbateman66

ADD ./ .

RUN npm install

EXPOSE 1833

ENTRYPOINT ["node", "app.js"]

