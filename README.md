# Distributed Sorting

## Overview

This is a git repository for CS434-AdvancedProgramming at POSTECH.\
This repository implements the final project which is distributed sorting using multiple servers.

## Usage

Clone the git repository.

```bash
git clone https://github.com/qwercxzsda/cs434-project.git
```

Run `sbt assembly`.

```bash
cd cs434-project
sbt assembly
```

Two files `master.jar` and `worker.jar` will be created at the project root directory.

### Run `Master`

```bash
java -jar master.jar [worker_number]
```

For example,

```bash
java -jar master.jar 1
```

### Run `Worker`

Use `30962` for `master_port`. See below for more information.

```bash
java -jar worker.jar [master_ip:master_port] -I [input_directory1] [input_directory2] ... -O [output_directory]
```

For examble,

```bash
java -jar worker.jar 2.2.2.142:30962 -I /home/blue/test2/input_dir -O /home/blue/test2/output_dir
```

### Caution

`master_port` is always assumed to be `30962`. If you use other number as `master_port` it will just be disregarded.

For master and worker to run properly, port `30962` must be free(not in use). If you really need to change the port as  port `30962` is already in use, change the variable `NetworkConfig.port` in the file `cs434-project/utils/src/main/scala/NetworkConfig.scala` and run `sbt assembly` again.

## Tests

### Test Large

1 Master
4 Workers each with 20000 * 2490 = 49.8 M Records(49.8 M * 100 B = 4.98 GB)

<img width="1490" alt="image" src="https://github.com/qwercxzsda/cs434-project/assets/101696461/8b2a014e-4893-4c99-98a9-5da185d60df0">
<img width="1490" alt="image" src="https://github.com/qwercxzsda/cs434-project/assets/101696461/474382ed-3640-4e88-836e-fa308254c958">
<img width="1492" alt="image" src="https://github.com/qwercxzsda/cs434-project/assets/101696461/c4e6c68a-fe59-49a7-a149-5e0ead9ebac2">
<img width="1492" alt="image" src="https://github.com/qwercxzsda/cs434-project/assets/101696461/dd427842-d44f-4171-82ef-441c7569a63c">
<img width="1490" alt="image" src="https://github.com/qwercxzsda/cs434-project/assets/101696461/3afcdc05-a075-43f2-9e62-9af07e2adbdb">

#### Result

<img width="1489" alt="image" src="https://github.com/qwercxzsda/cs434-project/assets/101696461/43357c69-e73e-47c3-96bf-f55bc1a9802b">
<img width="1495" alt="image" src="https://github.com/qwercxzsda/cs434-project/assets/101696461/fa034dc6-be09-4816-a9ce-e93d6a9feb69">
<img width="1491" alt="image" src="https://github.com/qwercxzsda/cs434-project/assets/101696461/462085d4-56b6-411c-beae-45b7b8ea28cf">
<img width="1487" alt="image" src="https://github.com/qwercxzsda/cs434-project/assets/101696461/30f6c7a0-33d0-4776-8c43-acea14bc3de7">

### Test Simple

1 Master
2 Workers each with 2 Records(2 * 100 B = 200 B)

<img width="1495" alt="image" src="https://github.com/qwercxzsda/cs434-project/assets/101696461/ca9829cf-005b-498f-871d-bb719ebbd6de">
<img width="1491" alt="image" src="https://github.com/qwercxzsda/cs434-project/assets/101696461/8beb2b5b-f1cf-4d1b-a0e2-e1d069f05701">
<img width="1491" alt="image" src="https://github.com/qwercxzsda/cs434-project/assets/101696461/6b73668d-ebda-4ee7-8525-36f42d47392c">

#### Result

##### input

<img width="545" alt="image" src="https://github.com/qwercxzsda/cs434-project/assets/101696461/152567a0-d387-4238-9de4-a620c728d215">
<img width="551" alt="image" src="https://github.com/qwercxzsda/cs434-project/assets/101696461/a2aa4335-0720-424e-aa5d-51555508fbc0">

##### output

<img width="546" alt="image" src="https://github.com/qwercxzsda/cs434-project/assets/101696461/df5720cb-89e1-4fa3-824b-5c81376762f9">
<img width="552" alt="image" src="https://github.com/qwercxzsda/cs434-project/assets/101696461/a5276ca1-afdf-47a0-9c44-cb1d3c4fa0dd">

### Test Large2(no duplicates)

1 Master
4 Workers each with 100,000,000 = 100 M Records(100 M * 100 B = 10 GB)

#### Result

##### Input

Created using the below code on each workers.

```bash
gensort -b[start_index] [number] partition0
```

For examble, on worker 3,

```bash
gensort -b200000000 100000000
```

##### Output
