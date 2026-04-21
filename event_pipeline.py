import event
import db

if __name__ == '__main__':
    user = event.create_user()
    all_event = event.create_event(user)

    db.create_table_not_exist()
    db.insert_data(all_event)
