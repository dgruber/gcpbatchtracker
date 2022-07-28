FROM alpine:latest
RUN apk add --update --no-cache openssh 
RUN apk add --update --no-cache openssh-server
ENTRYPOINT ["/entrypoint.sh"]
EXPOSE 2022
COPY entrypoint.sh /
