# Wolverine Repro Template

## Setup

This repro requires a working PostgreSQL server and database.
Simply run this command to create a new PostgreSQL server in Docker

```
docker run --name wolverine-repro -e POSTGRES_USER=postgres -e POSTGRES_PASSWORD=postgres -p 5432:5432 -d postgres
```

## Reproduction Steps

TBD

## Teardown

Remove the created PostgreSQL server with this command

```
docker rm -f -v wolverine-repro
```
