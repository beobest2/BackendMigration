1. PL을 통해 모든 노드의 백엔드 이동
```
PL RAM
PL SSD
PL HDD
```
2. 추가된 노드에 IRIS 설치 후 ./NodeAdd

3. migration_info_maker.py 스크립트가 있는 디렉토리에 conf파일 수정 
    1. 첫번째 줄에는 데이터 노드의 총 개수, 두 번째줄 부터 한 줄 씩 추가되는 노드의 아이피 주소를 입력
    

ex )  기존 2대의 노드가 있고 3대가 추가될 경우

```
5
192.168.000.001
192.168.000.002
192.168.000.003
```

4. 마스터 노드에서 migration_info_maker.py 를 실행하여 파티션과 테이블명 입력
```
python migration_info_maker.py
```
5. 생성된 migration_info.dat 파일을 기존 데이터 노드의 BMDClient.py가 있는 디렉토리로 복사
```
scp migraton_info.dat [user]@[ip]:[path]
```
6. 마스터노드를 포함한 모든 노드에서 BMD.py 실행 
```
python BMD.py
```
7.  ( IRIS 모든 노드가 VALID, 모든 노드에서 BMD.py 서버가 실행되는 상태 ) ""기존 데이터 노드"" 에서 각각 BMDClient.py 실행
```
python BMDClient.py
```
