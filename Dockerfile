# this does NOT work!
FROM pypy:3

WORKDIR /usr/src/app


COPY requirements.txt ./
RUN apt-get update \
  && apt-get upgrade -y \
  && apt-get install -y cmake \
  && pip install --no-cache-dir -r requirements.txt

COPY . .

CMD [ "pypy3", "./src/build_metanew.py 2020-04-08 2024-10-13 init" ]
