from hdfs import InsecureClient
import os

# hardoop 연결
hdfs_client = InsecureClient('http://localhost:9870', user='ubuntu')

local_logs_path = '/home/ubuntu/dmf/automation/logs/' 
hdfs_logs_path = '/user/ubuntu/input/logs/'

# local_logs_path에 있는 파일 불러와 리스트화
local_files = os.listdir(local_logs_path)

for file_name in local_files:
    local_file_path = local_logs_path + file_name
    hdfs_file_path = hdfs_logs_path + file_name

    # 현재 올리려는 파일이 hdfs에 있으면
    if hdfs_client.content(hdfs_file_path, strict=False):
        print(f'이미 존재함 {file_name}')
    else:
        hdfs_client.upload(hdfs_file_path, local_file_path)
        print(f'업로드 완료 {file_name}')