import time
import threading

def blocking_loop():
    print("进入阻塞循环（子线程）")
    while True:
        time.sleep(1)
        print("子线程在转圈")

# 把阻塞循环丢进子线程
thread = threading.Thread(target=blocking_loop, daemon=True)
thread.start()

# 主线程还能继续执行
for i in range(5):
    time.sleep(1)
    print(f"主线程还活着 {i}")
print("主线程结束")