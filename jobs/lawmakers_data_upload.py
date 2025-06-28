import data_operations as dataops
import datetime
import time

# 오늘 날짜를 출력
print(f'{datetime.datetime.now()} 의원 데이터 업데이트 시작')
start_time = time.time()

# 법안 데이터 업데이트
dataops.update_lawmakers_data(mode='update')

end_time = time.time()
elapsed_time = end_time - start_time

print(f'{datetime.datetime.now()} 의원 데이터 업데이트 완료')
print(f'소요시간: {elapsed_time:.2f}초')