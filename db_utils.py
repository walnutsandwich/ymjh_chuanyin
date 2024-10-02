
# 迁移本地数据库到mongodb

import sqlite3
import os
from bson.json_util import dumps, loads
from bson.objectid import ObjectId

from pymongo.mongo_client import MongoClient
from datetime import datetime,timedelta
import pytz

uri = "mongodb+srv://tianwenliu:tianwenliu@tianwen6.i6mum.mongodb.net/?retryWrites=true&w=majority&appName=Tianwen6"
mongo_dbname = 'tw6_db'
mongo_collection_name = 'cy'
sqlite_db_name = 'local_database.db'


class LocalDatabase:
    # 从本地数据库获取数据的操作
    def __init__(self):
        self.conn = sqlite3.connect(sqlite_db_name)
        self.cursor = self.conn.cursor()
        # 初始化数据库
        self.create_tables()
        
    # 本地数据库
    def create_tables(self):
        self.cursor.execute('''
        CREATE TABLE IF NOT EXISTS messages (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            n TEXT,
            m TEXT,
            t INTEGER NOT NULL,
            h INTEGER
        )
        ''')
        self.conn.commit()
    # 本地数据库插入
    def insert_data(self, n=None, m=None, t=None, h=None):
            self.cursor.execute('''
            INSERT INTO messages (n, m, t, h)
            VALUES (?, ?, ?, ?)
            ''', (n, m, t, h))
            self.conn.commit()
    
    def insert_data_batch(self, data):
        # 开始事务，确保一系列操作作为一个整体执行。如果事务中的任何操作失败，那么整个事务都会回滚，确保数据的一致性。
        self.conn.execute('BEGIN TRANSACTION')

        try:
            placeholders = ', '.join(['?' for _ in range(len(data[0]))])
            query = f"INSERT INTO messages (n, m, t, h) VALUES ({placeholders})"
            self.cursor.executemany(query, data)
            self.conn.commit()
        except Exception as e:
            # 回滚事务
            self.conn.rollback()
            raise e

    def get_recent_messages(limit=15):
        self.cursor.execute("""
            SELECT n, m, t, h
            FROM messages
            ORDER BY t DESC
            LIMIT ?
        """, (limit,))
        recent_messages = self.cursor.fetchall()
        return recent_messages

    def get_all_messages(self):
        self.cursor.execute("""
            SELECT n, m, t, h
            FROM messages
        """)
        all_messages = self.cursor.fetchall()
        return all_messages
    
    def clear_table(self):
        # 检查表是否存在
        self.cursor.execute("""
            SELECT COUNT(*) 
            FROM sqlite_master 
            WHERE type='table' AND name='messages';
        """)
        if self.cursor.fetchone()[0] > 0:
            self.cursor.execute("DROP TABLE messages")
            self.conn.commit()
            print("已删除表 'messages'")
        else:
            print("没有该表，已经重建")
        self.create_tables()
            
    def save_to_txt(self, filename='messages.txt'):
        all_messages = self.get_all_messages()
        
        with open(filename, 'w') as file:
            for msg in all_messages:
                # 将时间戳转换为北京时间字符串
                timestamp = datetime.fromtimestamp(msg[2]//10)
                timestamp_beijing = timestamp.astimezone(tz=datetime.now().astimezone().tzinfo)
                formatted_time = timestamp_beijing.strftime('%Y-%m-%d %H:%M:%S')
                line = f"{formatted_time} {msg[3]} {msg[0]}  {msg[1]} \n"
                file.write(line)
    def close(self):
        self.conn.close()

class GlobalDatabase:
    def __init__(self,dbname=mongo_dbname,collection_name=mongo_collection_name,uri=uri):
        self.client = MongoClient(uri)
        self.dbname = dbname
        self.collection_name = collection_name
        self.connect()
    
    def connect(self):
        try:
            self.client.admin.command('ping')
            print("Pinged your deployment. You successfully connected to MongoDB!")
        except Exception as e:
            print(e)

    def insert_data(self, name=None, message=None, timestamp=None, tags=None):
        db = self.client[self.dbname]
        collection = db[self.collection_name]
        collection.insert_one({'n': name, 'm': message, 't': timestamp, 'h': tags})

    def insert_multiple(self, documents):
        db = self.client[self.dbname]
        collection = db[self.collection_name]
        collection.insert_many(documents)

    def get_recent_messages(self, limit=20):
        db = self.client[self.dbname]
        collection = db[self.collection_name]
        query = collection.find().sort('t', -1).limit(limit)
        recent_messages = [loads(dumps(doc)) for doc in query]
        recent_messages = [(message.get('n'), message.get('m')) for message in recent_messages]
        return recent_messages
    def get_all_messages(self):
        db = self.client[self.dbname]
        collection = db[self.collection_name]
        all_messages = list(collection.find())
        return all_messages
    
    def rename_keys_in_collection(self):
        """
        Rename multiple keys in the collection.
        """
        db = self.client[self.dbname]
        collection = db[self.collection_name]

        # 使用 $rename 进行重命名
        collection.update_many(
            {},  # 匹配所有文档
            {"$rename": {
                "name": "n",
                "message": "m",
                "timestamp": "t",
                "tags": "h"
            }}
        )

        print("Keys have been renamed.")
        
    def clear_collection(self):
        db = self.client[self.dbname]
        collection = db[self.collection_name]
        collection.delete_many({})
    
    def update_last_update(self):
        """
        更新 last_update 文档的时间戳，转换为中国时间（UTC+8）。
        """
        db = self.client[self.dbname]
        collection = db[self.collection_name]
        # 获取当前的北京时间（UTC+8）
        current_time_utc8 = datetime.utcnow() + timedelta(hours=8)
        # 转换为时间戳
        current_time_stamp = int(current_time_utc8.timestamp())
        initial_data = {
            "_id": "l",  # 使用字符串形式的_id
            "t": current_time_stamp
        }
        
        # 更新文档
        # collection.update_one({"_id": "last_update"}, {"$set": {"timestamp": current_time_stamp}})
        collection.replace_one({"_id": "l"}, initial_data, upsert=True)
    
        print("Last update timestamp updated.")
    
    def get_last_update(self):
        db = self.client[self.dbname]
        collection = db[self.collection_name]
        last_update_doc = collection.find_one({"_id":"l"}) 
        # print(last_update_doc)      
        return last_update_doc.get("t") if last_update_doc else None


    def delete_last_records(self,num):
        """
            Delete the last records based on the timestamp.
        """
        db = self.client[self.dbname]
        collection = db[self.collection_name]

        # 查找最后 20 条记录的 _id
        recent_ids = [
            record['_id'] for record in
            collection.find().sort('t', 1).limit(num)
        ]

        # 删除这些记录
        if recent_ids:
            result = collection.delete_many({"_id": {"$in": recent_ids}})
            print(f"Deleted {result.deleted_count} records.")
        else:
            print("No records to delete.")
            
    def close(self):
        self.client.close()

# 单独的函数将 MongoDB 数据保存到本地数据库
def save_mongodb_to_local(global_db, local_db):
    # 清空本地数据库表
    local_db.clear_table()
    all_messages = global_db.get_all_messages()
    print("成功获取远程数据")
    documents = [
        {
            'n': d.get('name'),
            'm': d.get('message'),
            't': d.get('timestamp'),
            'h': d.get('tags')
        }
        for d in all_messages
    ]
    data = [
        (doc['n'], doc['m'], doc['t'], doc['h'])
        for doc in documents
    ]
    # 使用批量插入
    local_db.insert_data_batch(data)

# 单独的函数将本地数据库数据上传到 MongoDB
def upload_local_to_mongodb(local_db, global_db):
    # 清空全局数据库集合
    global_db.clear_collection()
    all_messages = local_db.get_all_messages()
    documents = [
        {'n': d[0], 'm': d[1], 't': d[2], 'h': d[3]}
        for d in all_messages
    ]
    global_db.insert_multiple(documents)

# 从云端数据库下载到本地数据库并存储为txt
def downloadfrom():
    # 实例化化本地数据库
    local_db = LocalDatabase()
    # 初始化全局数据库
    global_db = GlobalDatabase()
    # 将全局数据库的数据保存到本地数据库
    save_mongodb_to_local(global_db, local_db)
    local_db.save_to_txt()

    # 打印结果
    print("Data download successfully.")
    local_db.close()
    global_db.close()

def parse_and_convert_timestamp(timestamp_str):
    # 将字符串转换为 datetime 对象
    dt = datetime.datetime.strptime(timestamp_str, '%Y-%m-%d %H:%M:%S')
    
    # 设置时区信息为北京时间（UTC+8）
    beijing_tz = pytz.timezone('Asia/Shanghai')
    dt_with_tz = dt.replace(tzinfo=beijing_tz)
    
    # 转换为 Unix 时间戳
    unix_timestamp = dt_with_tz.timestamp()
    
    return unix_timestamp

def read_from_txt(filename='messages.txt'):
    # 从导出文本中读取数据
    data = []
    temp_timestamp = 0
    with open(filename, 'r') as file:
        for line in file:
            parts = line.strip().split()
            if len(parts) == 4:
                timestamp_str, tags, name, message = parts
                timestamp = int(parse_and_convert_timestamp(timestamp_str)* 10)
                
                if temp_timestamp//10 != timestamp//10:
                    temp_timestamp = timestamp
                else:
                    data.append((name, message, temp_timestamp, tags))
                    temp_timestamp += 1
                                
    return data

def save_txt_to_databases(filename='messages.txt'):
    # 读取 .txt 文件中的数据，存入数据库
    data = read_from_txt(filename)
    
    # 清空本地数据库表
    local_db = LocalDatabase()
    local_db.clear_table()
    
    # 插入数据到本地数据库
    local_db.insert_data_batch(data)
    
    # 清空远程数据库集合
    global_db = GlobalDatabase(uri)
    global_db.clear_collection()
    
    # 准备文档格式
    documents = [
        {'n': d[0], 'm': d[1], 't': d[2], 'h': d[3]}
        for d in data
    ]
    
    # 插入数据到远程数据库
    global_db.insert_multiple(documents)
    
    # 关闭数据库连接
    local_db.close()
    global_db.close()

    print("Data has been saved to both local and remote databases.")



def generate_documents(count):
    """
    动态生成多个文档。
    """
    timest = int(datetime.now(pytz.timezone('Asia/Shanghai')).timestamp() * 10)
    documents = []
    names = ['Alice', 'Charlie', 'Bob', 'Diana']
    messages = ['Hello World!', 'How are you?', 'Nice to meet you!', 'What\'s up?']

    for i in range(count):
        name = names[i % len(names)]
        message = messages[i % len(messages)]
        document = {
            '_id': ObjectId(),  # 生成唯一的 _id
            'n': name,
            'm': message,
            't': timest,
            'h': 1
        }
        documents.append(document)
    return documents

def insert_examples_to_collection(count=9):
    global_db = GlobalDatabase(mongo_dbname, mongo_collection_name, uri)
    # 动态生成多个文档
    documents = generate_documents(count)  # 生成 repeat * 2 个文档
    print(documents)
    global_db.insert_multiple(documents)
    # 获取所有消息
    recent_messages = global_db.get_recent_messages()
    global_db.update_last_update()
    print(recent_messages)
    global_db.close()


if __name__ == '__main__':
    # insert_examples_to_collection()
    # local_db = LocalDatabase()
    # local_db.save_to_txt()
    
    db = GlobalDatabase()
    db.delete_last_records(1)
    
    # # db.rename_keys_in_collection()
    # messages = db.get_recent_messages()
    # for m in messages:
    #     print(m)
    # db.update_last_update()
    # print(db.get_last_update())
    db.close()

    