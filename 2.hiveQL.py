# pip3 install pyhive
# pip3 install thrift thrift-sasl

from pyhive import hive
import pandas as pd

# hive 에 연결할 수 있는 통로 생성
conn = hive.Connection(
    host='localhost',
    port=10000,
    username='ubuntu',
    # database='default'
)

# hive에 연결
cursor = conn.cursor()

query = '''
SELECT ip_address, request_path FROM logs LIMIT 10
'''

# query = '''
# SELECT
#     ip_address,
#     COUNT(*)
# FROM 
#     logs
# GROUP BY
#     ip_address
# ORDER BY 
#     COUNT(*) DESC
# LIMIT 10
# '''

# query = '''
# SELECT 
#     SPLIT(request_path, '/')[2] AS product_id,
#     COUNT(*) AS request_count
# FROM 
#     logs
# WHERE
#     request_path LIKE '/product/%'
# GROUP BY 
#     SPLIT (request_path, '/')[2]
# ORDER BY
#     request_count DESC
# LIMIT 10
# '''

cursor.execute(query)           # cursor : hive server와 연결
result = cursor.fetchall()      # 쿼리 실행

df = pd.DataFrame(result)

output_path = '/home/ubuntu/dmf/automation/logs-result'
output_file = 'requesT_result.csv'

df.to_csv(output_path + output_file)