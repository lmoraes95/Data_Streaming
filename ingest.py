import requests
import psycopg2
import config



with requests.get("http://127.0.0.1:5000/data_request/10000", stream=True) as r:

    conn = psycopg2.connect(**config.db_config)
    cur = conn.cursor()
    sql = "INSERT INTO revenue (txid, expenditure, valuable_costumer, amount_difference) VALUES (%s, %s, %s, %s)"
    prev_amount = None

    buffer = ""
    for chunk in r.iter_content(chunk_size=1):
        if chunk.endswith(b'\n'):
            t = eval(buffer)
            expenditure = t[1]
            if prev_amount:
                amount_difference = round(expenditure - prev_amount)
            else:
                amount_difference = None
            prev_amount = expenditure
            if t[1] > 45000:
                t = (t[0], t[1], "valuable costumer", amount_difference)
            elif 30000 < t[1] < 44999:
                t = (t[0], t[1], "good customer", amount_difference)
            else:
                t = (t[0], t[1], "average costumer", amount_difference)
            print(t)
            cur.execute(sql, t)
            conn.commit()
            buffer = ""
        else:
            buffer += chunk.decode()