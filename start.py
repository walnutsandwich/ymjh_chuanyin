# -*- coding: utf-8 -*-
# 该脚本需要打开pc，外置窗口，切换频道，识别传音消息然后上传到MongoDB远程服务器


import time
import numpy as np
import win32gui
from pywinauto import Desktop
import win32ui
import gc
from ctypes import windll
from PIL import Image
import numpy as np

from paddleocr import PaddleOCR, draw_ocr
from collections import Counter
import re
from datetime import datetime, timedelta
# 导入数据库类
from db_utils import LocalDatabase, GlobalDatabase
import sys

records_old = []
new_records_bak = []

def get_window_rect(window_title):
    """使用 pywinauto 获取指定窗口的坐标和尺寸"""
    try:
        # 获取桌面
        desktop = Desktop(backend="uia")
        
        # 查找窗口
        window = desktop.window(title=window_title)
        
        # 获取窗口的矩形范围
        rect = window.rectangle()
        return rect.left, rect.top, rect.right, rect.bottom
    except Exception as e:
        print(f"找不到窗口：{e}")
        return None

def capture_window(hwnd,crop_coords,width, height):

    # 创建设备上下文（DC）
    hwndDC = win32gui.GetWindowDC(hwnd)
    mfcDC = win32ui.CreateDCFromHandle(hwndDC)
    saveDC = mfcDC.CreateCompatibleDC()

    # 创建位图对象
    saveBitMap = win32ui.CreateBitmap()
    saveBitMap.CreateCompatibleBitmap(mfcDC, width, height)
    saveDC.SelectObject(saveBitMap)

    # 复制窗口内容到位图
    result = windll.user32.PrintWindow(hwnd, saveDC.GetSafeHdc(), 0)

    # 将位图转换为PIL图像
    bmpinfo = saveBitMap.GetInfo()
    bmpstr = saveBitMap.GetBitmapBits(True)
    im = Image.frombuffer(
        'RGB',
        (bmpinfo['bmWidth'], bmpinfo['bmHeight']),
        bmpstr, 'raw', 'BGRX', 0, 1)

    # 清理资源
    win32gui.DeleteObject(saveBitMap.GetHandle())
    saveDC.DeleteDC()
    mfcDC.DeleteDC()
    win32gui.ReleaseDC(hwnd, hwndDC)
    
    im.save("game_screenshot.png")
    if result == 1:
        # 裁剪图像
        im = im.crop((crop_coords[0][0], crop_coords[0][1], crop_coords[1][0], crop_coords[1][1]))
        return im
    else:
        print("【截图失败】")
        return None
    

def ocr_image(ocr, image):
    # 将 PIL.Image 转换为 numpy 数组
    image_np = np.array(image)
    
    # 执行 OCR
    result = ocr.ocr(image_np, cls=True)
    # print("OCR Result:", result)

     # 提取信息
    blocks = []
    if result and isinstance(result[0], list):
        for block in result[0]:
            if isinstance(block, list) and len(block) >= 2:
                block_info = {
                    'text': block[1][0].replace("\n", "").replace(" ", ""),
                    'box': block[0]
                }
                
                blocks.append(block_info)
    # 释放 OCR 资源

    return blocks

def jaccard_similarity(ele1, ele2):
    # 使用 Counter 来统计每个字符的出现次数
    counter1 = Counter(ele1)
    counter2 = Counter(ele2)
    if not ele1 and not ele2:
        return 1.0
    
    # 计算并集 和 交集
    intersection = sum((counter1 & counter2).values())
    union = sum((counter1 | counter2).values())
    
    # 计算 Jaccard 相似度
    if union == 0:
        return 0.0
    else:
        # print(intersection / union)
        return intersection / union

def is_chinese_char(char):
    code_point = ord(char)
    # 判断是否在汉字范围内
    if (0x4E00 <= code_point <= 0x9FFF) or \
       (0x3400 <= code_point <= 0x4DBF) or \
       (0xF900 <= code_point <= 0xFAFF) or \
       (0x20000 <= code_point <= 0x2A6DF) or \
       (0x2A700 <= code_point <= 0x2B73F) or \
       (0x2B740 <= code_point <= 0x2B81F) or \
       (0x2B820 <= code_point <= 0x2CEAF):
        return True
    return False

def flter_blocks(blocks,filter_x1,filter_x2):
    name = ""
    message = ""
    processed_texts = []
    start = False
    pattern = r'[\u4e00-\u9fa5]+'
    # 定义需要移除的字符集合,因为最后一个字后面的图标容易被误识别为如下几个字,消息第一个字也容易被误识
    n_chars_to_remove = {'贵','责', '壹',"1","?","了"}
    m_chars_to_remove = {'贵','责', '壹','青',"口","?","了","司"}
    if_chuanyin = ["[","]","】","【","1"]
    for block in blocks:
        if "传音" in block['text'] and any(c in block['text'][:5] for c in if_chuanyin):
            start = True
            processed_texts.append((name, message))
            message = ""
            
            hanzi_matches = re.findall(pattern,block['text'])
            name = ''.join(hanzi_matches).replace("传音","")
            if len(name)>1 and name[-1] in n_chars_to_remove:
                name = name.rstrip(name[-1])
    
        elif block['box'][0][0]>filter_x2 and start:
            # message += re.sub(r'\s+', '', block['text'])
            message += block['text'].strip()
            if message[0]=="1" and len(message)>1 and is_chinese_char(message[1]):
                message = message.lstrip(message[0])
            if message[0] in m_chars_to_remove:
                message = message.lstrip(message[0])
                        
                
        elif block['box'][0][0]<filter_x1:
            continue
        else:
            processed_texts.append((name, message))
            start = False
            name = ""
            message = ""
    processed_texts.append((name, message))
    return [pt for pt in processed_texts if pt[0]+pt[1].strip() != "" ]

def remove_repeated_records(filter_records,records_old):
    new_records = []
    # 从后往前遍历新提取的结果
    for r in filter_records:
        is_new_message = True
        for ro in records_old:
            if is_similar_record(r, ro):
                is_new_message = False
                break
            
        if is_new_message:
            new_records.append(r)
    return new_records

def is_similar_record(record1, record2):
    similarity_threshold = 0.6 
    # flag True表示不是相同的消息
    flag = False
    # 如果两个消息的相似度小于等于阈值，则认为不是相同的消息
    if jaccard_similarity(record1[1], record2[1]) <similarity_threshold:
        flag = True
    # 如果不是同一个人说的，则认为是不同的消息, 名字相似判断要松一点，因为可能识别 多一个少一个字
    elif jaccard_similarity(record1[0], record2[0]) < similarity_threshold-0.3:
        flag = True
    
    return not flag

def remove_repeated_self_records(news_records):
    seen = []
    for record in news_records:
        if not any(is_similar_record(record, seen_tuple) for seen_tuple in seen):
            seen.append(record)
    return seen
    

def get_news(db,blocks,filter_x1,filter_x2):
    global records_old,new_records_bak
    

    filter_records = flter_blocks(blocks,filter_x1,filter_x2)
    print("filter_records:",filter_records)
    
    if records_old == []:
        records_old = db.get_recent_messages()
    else:
        records_old = new_records_bak+records_old
        records_old = records_old[:20]
    
    new_records = remove_repeated_records(filter_records,records_old)
    new_records = remove_repeated_self_records(new_records)
    new_records_bak = new_records
    return new_records

def check_update_state(global_db):
    last_update_timestamp = global_db.get_last_update()
    current_time_utc8 = datetime.utcnow() + timedelta(hours=8)
    current_time_stamp = int(current_time_utc8.timestamp())
    if last_update_timestamp is not None:
        time_diff = abs(current_time_stamp - last_update_timestamp)
        if time_diff <= 60:
            response = input("检测到 60s 内有人正在更新，是否继续? 如果是你自己上传的请输入 Y 确认继续: ").strip().upper()
            if response == 'Y':
                print("User chose to continue.")
                return True
            else:
                print("Stop.")
                sys.exit(0)
                return False
    return True    

def extract_chuanyin():
    window_title = "一梦江湖"
    hwnd = win32gui.FindWindow(None, window_title)
    if not hwnd:
        print(f"【未找到标题为 '{window_title}' 的窗口】")
        return

    left, top, right, bottom = get_window_rect(window_title)
    width, height = right - left, bottom - top
    windows_size = [1350, 789]
    crop_coords = [(1080, 65), (1331, 788)]
    crop_coords = [
        (int(width * crop_coords[0][0] / windows_size[0]),
         int(height * crop_coords[0][1] / windows_size[1])),
        (int(width * crop_coords[1][0] / windows_size[0]),
         int(height * crop_coords[1][1] / windows_size[1]))
    ]

    cut_width = crop_coords[1][0]-crop_coords[0][0]
    mark_t = datetime.now()

    # 创建数据库实例
    local_db = LocalDatabase()
    global_db = GlobalDatabase()
    delay_time = 0
    check_update_state(global_db)

    ocr = PaddleOCR(layout=True, lang="ch")  # 使用中文模型
    while True:
        screenshot = capture_window(hwnd, crop_coords, width, height)
        if screenshot:
            screenshot.save("cropped_game_screenshot.png")
            # 使用 OCR 处理图像
            blocks = ocr_image(ocr, screenshot)
            # print("blocks:",blocks)
            news_message = get_news(global_db,blocks,cut_width*0.1,cut_width*0.2)
            # 输出结果
            print("news_message:",news_message)
            
            if  datetime.now()-mark_t >= timedelta(seconds=30):
                global_db.update_last_update()
                mark_t = datetime.now()
            
            documents = []
            time_offset = 0  # 初始化时间偏移量
            for message in news_message:
                name, text = message
                if len(text)>1:
                    timestamp = int(time.time())*10+ time_offset  # 获取当前时间戳
                    documents.append({'n': name, 'm': text, 't': timestamp,'h':1})
                    # insert_data(name=name, message=text, timestamp=timestamp)
                    time_offset += 1
            if documents:
                local_db.insert_data_batch([(doc['n'], doc['m'], doc['t'], doc['h']) for doc in documents])
                global_db.insert_multiple(documents)
                delay_time-=1
                delay_time = max(delay_time,0)
            else:
                delay_time+=1
                delay_time = min(delay_time,15)
            del screenshot
        gc.collect()
        time.sleep(3+delay_time)
        # break
        print("等待 "+str(3+delay_time)+"秒")


# 测试函数
if __name__ == "__main__":
    extract_chuanyin()