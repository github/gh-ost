FROM ruby

RUN gem install --no-document fpm

RUN apt update && apt install -y rpm

RUN mkdir /gh-ost && chmod 777 /gh-ost
