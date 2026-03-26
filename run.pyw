import subprocess
import sys
import os
import time

CREATE_NO_WINDOW = 0x08000000

subprocess.Popen(
    [sys.executable, "-m", "streamlit", "run", "web_app.py", 
     "--server.headless=true", "--server.port=8501"],
    stdout=subprocess.DEVNULL,
    stderr=subprocess.DEVNULL,
    stdin=subprocess.DEVNULL,
    creationflags=CREATE_NO_WINDOW
)

time.sleep(2)
subprocess.run(["cmd", "/c", "start", "http://localhost:8501"], creationflags=CREATE_NO_WINDOW)
