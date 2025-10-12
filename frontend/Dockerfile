FROM nginx:1.25-alpine

COPY nginx/default.conf /etc/nginx/conf.d/default.conf

COPY index.html /usr/share/nginx/html/index.html
COPY about.html /usr/share/nginx/html/about.html
COPY problematic.html /usr/share/nginx/html/problematic.html
COPY styles.css /usr/share/nginx/html/styles.css
COPY app.js /usr/share/nginx/html/app.js
COPY config.js /usr/share/nginx/html/config.js
