import data_operations as dataops
import os
import dotenv

dotenv.load_dotenv()

print("[정당별 법안 발의수 갱신 요청 중...]")
post_url_party_bill_count = os.environ.get("POST_URL_party_bill_count")
dataops.request_post(post_url_party_bill_count)
print("[정당별 법안 발의수 갱신 요청 완료]")

print("[의원별 최신 발의날짜 갱신 요청 중...]")
post_ulr_congressman_propose_date = os.environ.get("POST_URL_congressman_propose_date")
dataops.request_post(post_ulr_congressman_propose_date)
print("[의원별 최신 발의날짜 갱신 요청 완료]")