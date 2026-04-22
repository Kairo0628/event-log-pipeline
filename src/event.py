from faker import Faker
import random
from datetime import datetime

Faker.seed(42)
random.seed(42)

fake = Faker('ko_KR')

def create_user():
    return {'id': [i for i in range(1, 101)],
            'user': [fake.user_name() for _ in range(100)],
            'user_agent': [fake.user_agent() for _ in range(100)],
            'ip': [fake.ipv4() for _ in range(100)]}

def _event_hour(n):
    if n < 0.65:
        hour = random.choice([8, 9, 17, 18, 19, 20, 21, 22, 23, 0, 1])
    elif n < 0.9:
        hour = random.choice([10, 11, 12, 13, 14, 15, 16])
    else:
        hour = random.choice([2, 3, 4, 5, 6, 7])

    return hour

# 상품 이벤트 생성
def _product_event(
    timestamp,
    user_id,
    user,
    user_agent,
    ip,
    url,
    session,
    all_event
):
    product_id = random.randint(1, 10)
    event = {
        'id': user_id, 'user': user, 'user_agent': user_agent, 'ip': ip, 'url': url + f'/product/{product_id}',
            'session': session, 'event_type': 'click', 'timestamp': timestamp
    }
    all_event.append(event)
    timestamp += random.randint(60, 300)

    # 상품 구매 클릭 이벤트
    rand_purchase = random.random()
    if rand_purchase < 0.33:
        event = {
            'id': user_id, 'user': user, 'user_agent': user_agent, 'ip': ip, 'url': url + f'/product/{product_id}',
            'session': session, 'event_type': 'purchase', 'timestamp': timestamp
        }
        all_event.append(event)
        timestamp += random.randint(60, 300)

        # 상품 결제 이벤트
        rand_pay = random.random()
        if rand_pay < 0.7:
            pay_type = 'success'
        elif rand_pay < 0.95:
            pay_type = 'cancel'
        else:
            pay_type = 'fail'
        event = {
            'id': user_id, 'user': user, 'user_agent': user_agent, 'ip': ip, 'url': url + '/payments',
            'session': session, 'event_type': pay_type, 'timestamp': timestamp
        }
        all_event.append(event)
        timestamp += random.randint(60, 300)
    
    return timestamp

def create_event(user_dict):
    url = 'mysite.co.kr'
    all_event = []

    while len(all_event) < 100_000:
        user_num = random.randint(0, 99)

        user_id = user_dict['id'][user_num]
        user = user_dict['user'][user_num]
        user_agent = user_dict['user_agent'][user_num]
        ip = user_dict['ip'][user_num]
        session = fake.uuid4()

        rand_val = random.random()

        day = random.randint(1, 31)
        hour = _event_hour(rand_val)
        minute = random.randint(0, 59)
        second = random.randint(0, 59)

        timestamp = datetime(2026, 3, day, hour, minute, second).timestamp()

        # 광고를 보고 들어왔는지? 메인으로 왔는지?
        if rand_val < 0.75:
            event = {
                'id': user_id, 'user': user, 'user_agent': user_agent, 'ip': ip, 'url': url,
                 'session': session, 'event_type': 'main', 'timestamp': timestamp
            }
            all_event.append(event)
            timestamp += random.randint(30, 300)
        
            # 상품 페이지로 이동하는지?
            rand_product = random.random()
            if rand_product < 0.75:
                event = {
                'id': user_id, 'user': user, 'user_agent': user_agent, 'ip': ip, 'url': url,
                 'session': session, 'event_type': 'main_product', 'timestamp': timestamp
                }
                all_event.append(event)
                timestamp += random.randint(30, 300)
            else:
                continue

        else:
            event = {
                'id': user_id, 'user': user, 'user_agent': user_agent, 'ip': ip, 'url': url + '/product',
                 'session': session, 'event_type': 'product', 'timestamp': timestamp
            }
            all_event.append(event)
            timestamp += random.randint(30, 300)
        
        # 상품 클릭 여부
        rand_val = random.random()
        while rand_val < 0.75 and len(all_event) < 100_000:
            timestamp = _product_event(
                            timestamp,
                            user_id,
                            user,
                            user_agent,
                            ip,
                            url,
                            session,
                            all_event
                        )
            rand_val = random.random()

    return all_event
