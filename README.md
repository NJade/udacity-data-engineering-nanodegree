# Data Engineering Nanodegree
* https://www.udacity.com/course/data-engineer-nanodegree--nd027

## Setting
### Postgres
- docker
```sh
docker pull postgres
docker volume create pgdata
docker run -d -p 5432:5432 --name pgsql -it --rm -v pgdata:/var/lib/postgresql/data postgres
```
### cassandra
- docker
```sh
docker pull cassandra
 docker run --name cassandra -d -e CASSANDRA_BROADCASE_ADDRESS=127.0.0.1 -p 7000:70
00 -p 9042:9042 cassandra
```

## Reference
- https://judo0179.tistory.com/48
- http://blog.naver.com/flowerdances/221213406546
