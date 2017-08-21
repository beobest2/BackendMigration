# BMD 사용법

1. PL을 통해 모든 노드의 백엔드 이동
```
PL RAM
PL SSD
PL HDD
```

2. 마이스레이션 실행중 백엔드 이동으 방지하기 위해 모드 노드의 PL_RAM, PL_SSD, PR 정지

```
mps term 27903
mps term 27904
mps term 27905
```

3. 추가된 노드에 IRIS 설치 후 ./NodeAdd

4. conf 파일 입력 
    1. 첫번째 줄에는 데이터 노드의 총 개수, 두 번째줄 부터 한 줄 씩 추가되는 노드의 아이피 주소를 입력
    

ex )  기존 2대의 노드가 있고 3대가 추가될 경우

```
5
192.168.000.001
192.168.000.002
192.168.000.003
```

5. 마스터 노드에서 migration_info_maker.py 를 실행하여 파티션과 테이블명 입력 (하루 단위로 동작, 시작 파티션 입력)
```
python migration_info_maker.py
```
6. 마스터 노드에서 생성된 migration_info.dat 파일을, 기존에 있던 데이터 노드의 BMDClient.py가 있는 디렉토리로 각각 복사
```
scp migraton_info.dat [data_node_user]@[data_node_ip]:[data_node_path]
```
7. 모든 노드에서 (마스터 노드, 기존 데이터 노드, 추가된 데이터 노드) BMD.py 실행 
```
python BMD.py
```
8. 기존 데이터 노드 에서 각각 BMDClient.py 실행  ( IRIS 모든 노드가 VALID, 모든 노드에서 BMD.py 서버가 실행되는 상태, PL_RAM, PL_SSD, PLR 이 종료된 상태 확인 ) 
```
python BMDClient.py
```

# SYS_TABLE_MIGRATION 사용법

1. 모든 노드의 IRIS 종료 
    - ~/IRIS/bin/Admin/IRIS-Shutdown
2. 모든 노드의 library를 kddi 버전으로 교체 
    - rm -rf ~/IRIS/lib/*
    - cp ~/BackendMigration/SYS_TABLE_MIGRATION/src/lib/* ~/IRIS/lib/
    - ln -s ~/IRIS/lib/IRIS_CORE_1.5.1_1-55-ged99bb0_ed99bb0.zip ~/IRIS/lib/M6.zip 
3. 각각의 데이터 노드에서 ssd_data 생성 -> link 생성 
    - mkdir -p ~/ssd_data
    - mkdir ~/IRIS/data/slave_ssd
    - ln -s ~/ssd_data ~/IRIS/data/slave_ssd/part00
    - 해당 디렉토리 혹은 소프트링크가 제대로 생성되지 않는 경우 .system ssd 결과가 출력되지 않을 수 도있으니 주의할 것
4. 각각의 데이터 노드의 conf/mps.conf에 PL_SSD, PL_RAM 추가 
5. 각각의 데이터 노드의 IRIS/bin에 PL_SSD, PL_RAM 추가
    - kddi IRIS의 PL_RAM, PL_SSD 복사 
6. 마스터노드에서 create_sys_ssd_info.py 스크립트 실행
7. 마스터노드의 IRIS/data/master에 있는 (SYS_TABLE_LOCATION 제외) 시스템 테이블을 scp를 이용해서 각 데이터 노드로 복사
    - scp SYS\_\*\.DAT iris@192.168.xxx.xxx:~/IRIS/data/master
    - .system ssd 확인

8. 마스터 노드에서 modify_table_info.py를 실행시킨다. 
9. ~/IRIS/data/monitor_data_test에 존재하는 스키마가 제대로 변경되었는지 확인한다. 
10. 스키마가 제대로 변경된 경우, ~/IRIS/data/monitor_data를 삭제하고 스키마가 변경된 백엔드의 디렉토리명을 변경한다. 
    - rm -rf ~/IRIS/data/monitor_data
    - mv ~/IRIS/data/monitor_data_test ~/IRIS/data/monitor_data
11. 각각의 데이터 노드에서 modify_table_size_info.py를 실행시킨다. 
12. ~/IRIS/data/slave_disk/part00/SYS_TABLE_SIZE_INFO에 존재하는 스키마가 제대로 변경되었는지 확인한다. 
13. 스키마가 제대로 변경된 경우, ~/IRIS/data/slave_disk/part00/SYS_TABLE_SIZE_INFO를 삭제하고 스키마가 변경된 백엔드의 디렉토리명을 변경한다. 
    - rm -rf ~/IRIS/data/slave_disk/part00/SYS_TABLE_SIZE_INFO
    - mv ~/IRIS/data/slave_disk/part00/SYS_TABLE_SIZE_INFO_TEST ~/IRIS/data/slave_disk/part00/SYS_TABLE_SIZE_INFO
14. 모든 노드의 IRIS를 IRIS-Startup 
    - ~/IRIS/bin/Admin/IRIS-Startup


## SYS_TABLE_MIGRATION 실행 이후 확인해야할 사항들

- SYS_TABLE_SIZE_INFO 테이블에 'SIZE_SSD', 'FNUM_SSD' 컬럼 추가 
- .table size 커맨드에  'SIZE_SSD', 'FNUM_SSD' 컬럼 추가  
- .system all 커맨드에   'SSD' , 'SSD_TOTAL_SIZE' 컬럼 추가 
- .system ssd 커맨드 추가 
- .show tables 커맨드에  'SSD_EXP_TIME'  컬럼 추가 
- .statistics table 커맨드에 'TABLE_SIZE_SSD', 'NUM_OF_FILE_SSD' 컬럼 추가
