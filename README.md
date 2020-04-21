# Uchi.ru ETL test

## Getting Started

Install docker & docker-compose on your local machine. Next up enable docker service (It may be neccessary to reboot).

### Running


```
sudo docker build --rm -t puckel/docker-airflow .
sudo docker-compose up -d --build
```

### Answers to "Тестовое задание Data Engineer.pdf"
```
1) ./docker-compose.yaml
2) ./dags/config/clickhouse_schemas/*
```

#### createRaw.tbl
Выбран движок Memory, а в качестве формата для Insert JSONEachRow в связи с тем, что таблица временная и хранит еженедельно ~250Mb строк только в качестве переходного хранилища для Spark

#### create.tbl
Выбран движок MergeTree (в связи с соображениями производительности) с параметрами: a) Партиции по ключу Date "YYYYMM", b) Сортировка ид пользователя -> Date -> id сессии -> id песни, c) Семплирование для снижения кол-ва читаемой информации с диска при SELECT

```
3) ./dags/file_sensor.py
```

![Alt text](./static/dag_tree.png?raw=true "Graph Preview") 
