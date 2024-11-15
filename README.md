# Distributed Sorting

## Overview

This is a git repository for CS434-AdvancedProgramming at POSTECH.

This repository implements the final project which is distributed sorting using multiple servers.

You can find the implementation details [here](./docs/Final%20Presentation.pdf).

Among three teams Red, Blue, and Green, our team (Blue) achieved the best performance by a significant margin. You can find the final results [here](./docs/Final%20Results.pdf).

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

Run the following code.

```bash
java -jar master.jar [worker_number]
```

For example,

```bash
java -jar master.jar 1
```

### Run `Worker`

Run the following code. Use `30962` for `master_port`. See below for more information.

```bash
java -jar worker.jar [master_ip]:[master_port] -I [input_directory1] [input_directory2] ... -O [output_directory]
```

For examble,

```bash
java -jar worker.jar 2.2.2.142:30962 -I /home/blue/test2/input_dir -O /home/blue/test2/output_dir
```

Output will be created in the `output_directory` as `partition*`.

Disregard the `tmp1`, `tmp2` folders created in the `output_directory`. These are not part of the output. They are temporary files used during the `worker` execution.

### Caution

`master_port` is always assumed to be `30962`. If you use other number as `master_port` it will just be disregarded.

For master and worker to run properly, port `30962` must be free(not in use). If you really need to change the port as  port `30962` is already in use, change the variable `NetworkConfig.port` in the file `cs434-project/utils/src/main/scala/NetworkConfig.scala` and run `sbt assembly` again(for both `master` and `worker`).

## Tests

### Test Large

1 Master\
4 Workers each with 20000 * 2490 = 49.8 M Records(49.8 M * 100 B = 4.98 GB)\
Total 49.8 M * 4 = 199.2 M Records(199.2 M * 100 B = 19.92 GB)

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

1 Master\
2 Workers each with 2 Records(2 * 100 B = 200 B)\
Total 2 * 2 = 4 Records(4 * 100 B = 400 B)

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

1 Master\
4 Workers, 1 directory, each directory with 100,000,000 = 100 M Records(100 M * 100 B = 10 GB)\
Totla 100 M * 4 = 400 M Records(400 M * 100 B = 40 GB)

#### Result

##### Input

100,000,000 * 4 = 400 M Records

Created using the below code on each workers.

```bash
gensort -b[start_index] [number] partition0
```

For examble, on worker 3,

```bash
gensort -b200000000 100000000
```

##### Output

99586002 + 94949919 + 107194027 + 98270052 = 400,000,000 = 400 M

All sorted, no duplicates

<img width="1492" alt="image" src="https://github.com/qwercxzsda/cs434-project/assets/101696461/2af3133e-642d-4ff7-a5f9-fe6386aa6fe1">
<img width="1491" alt="image" src="https://github.com/qwercxzsda/cs434-project/assets/101696461/d8c28a37-8275-49d2-83ee-109d02421662">
<img width="499" alt="image" src="https://github.com/qwercxzsda/cs434-project/assets/101696461/90e2c32c-3198-4952-922c-3dcfda8aa192">
<img width="500" alt="image" src="https://github.com/qwercxzsda/cs434-project/assets/101696461/9c4bbd1b-77f0-40c4-8c0d-75536eab58b1">
<img width="495" alt="image" src="https://github.com/qwercxzsda/cs434-project/assets/101696461/640e4060-a37e-4bf4-954c-7edc3228db23">
<img width="497" alt="image" src="https://github.com/qwercxzsda/cs434-project/assets/101696461/923e0962-b5cf-4df8-8839-6b22edc7a703">

### Test Large3(no duplicates)

1 Master\
8 Workers, 3 directories, each directory with 100,000 = 100 K Records(100 K * 100 B = 10 MB)\
Total 100 K * 3 * 8 = 2.4 M Records(2.4 M * 100 B = 240 MB)

#### Result

##### Input

100,000 * 3 * 8 = 2,400,000 = 2.4 M Records

Created using the below code on each workers.

```bash
gensort -b[start_index] [number] partition0
```

For examble,

<img width="610" alt="image" src="https://github.com/qwercxzsda/cs434-project/assets/101696461/90876ce5-9076-46aa-8bc4-f6c4e32dbdb1">

##### Output

283952 + 295771 + 308125 + 307138 + 303761 + 305078 + 307586 + 288589 = 2,400,000 = 2.4 M

All sorted, no duplicates

<img width="1494" alt="image" src="https://github.com/qwercxzsda/cs434-project/assets/101696461/6c623bd0-e09a-47cf-84ea-75d9bebca812">
<img width="1487" alt="image" src="https://github.com/qwercxzsda/cs434-project/assets/101696461/909f8438-cc26-420c-a4c8-2bfafa7f59ca">
<img width="1487" alt="image" src="https://github.com/qwercxzsda/cs434-project/assets/101696461/d68d8c9c-653b-40a3-a70b-9c7900b92252">
<img width="1490" alt="image" src="https://github.com/qwercxzsda/cs434-project/assets/101696461/86f23804-46bb-419a-8fee-adcfa8ad38cc">
<img width="1490" alt="image" src="https://github.com/qwercxzsda/cs434-project/assets/101696461/047e1ff4-64cb-4566-9b49-c089da605586">

<img width="488" alt="image" src="https://github.com/qwercxzsda/cs434-project/assets/101696461/91019e94-1b56-44ae-ab45-a0f28aa948d4">
<img width="489" alt="image" src="https://github.com/qwercxzsda/cs434-project/assets/101696461/3a2dd7bf-e5b7-4e4a-84d2-ef37d28b6e53">
<img width="491" alt="image" src="https://github.com/qwercxzsda/cs434-project/assets/101696461/88328228-7fef-425b-9f05-53350a5b29b7">
<img width="490" alt="image" src="https://github.com/qwercxzsda/cs434-project/assets/101696461/63623016-17e5-4b3f-999f-7c533ca1dec9">
<img width="489" alt="image" src="https://github.com/qwercxzsda/cs434-project/assets/101696461/11ebfb91-f106-4852-8380-ec08632de626">
<img width="489" alt="image" src="https://github.com/qwercxzsda/cs434-project/assets/101696461/a4b9b130-dce0-40c5-b404-e5e189ae8c96">
<img width="487" alt="image" src="https://github.com/qwercxzsda/cs434-project/assets/101696461/16833a76-b423-47da-ad9b-552a6c4d866b">
<img width="488" alt="image" src="https://github.com/qwercxzsda/cs434-project/assets/101696461/531b3236-fb5d-4d08-b3dc-4a23a2495b61">
