def fetch_data(url, key, params):

    api_key = key
    max_retry = 3

    all_data = []
    processing_count = 0
    retries_left = max_retry

    while True:

        print(f"Requesting page {params['pIndex']}...")

        # API 요청
        response = requests.get(url, params=params)

        # 응답 데이터 확인
        if response.status_code == 200:
            try:
                root = ElementTree.fromstring(response.content)
                head = root.find('head')
                if head is None:
                    print(f"Error: 'head' element not found in response (Page {params['pIndex']})")
                    break

                total_count_elem = head.find('list_total_count')
                if total_count_elem is None:
                    print(f"Error: 'list_total_count' element not found in 'head' (Page {params['pIndex']})")
                    break

                total_count = int(total_count_elem.text)

                rows = root.findall('row')
                if not rows:
                    print("No more data available.")
                    break

                data = []
                for row_elem in rows:
                    row = {child.tag: child.text for child in row_elem}
                    data.append(row)

                all_data.extend(data)
                print(f"Page {params['pIndex']} processed. {len(data)} items added. Total: {len(all_data)}")
                processing_count += 1

                if params['pIndex'] * params['pSize'] >= total_count:
                    print("All pages processed.")
                    break

            except Exception as e:
                print(f"Error: {e}")
                retries_left -= 1
        else:
            print(f"Error Code: {response.status_code} (Page {params['pIndex']})")
            retries_left -= 1

        if retries_left <= 0:
            print("Maximum retry reached. Exiting...")
            break

        if processing_count >= 10:
            processing_count = 0

        params['pIndex'] += 1

    # 데이터프레임 생성
    df = pd.DataFrame(all_data)

    print(f"[모든 파일 다운로드 완료!]")
    print(f"[{len(df)} 개의 데이터 수집됨]")

    data = df

    return data

url = "https://open.assembly.go.kr/portal/openapi/ALLSCHEDULE"
key = "5a53c820f06b438db95a838a4fbb699a"

date = datetime.now().strftime('%Y-%m-%d')

params = {
    'KEY': key,
    'Type': 'xml',
    'pIndex': 1,
    'pSize': 100,
    'SCH_DT': date
}

df_data = fetch_data(url, key, params)