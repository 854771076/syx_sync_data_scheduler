import sys
import os
import subprocess
import signal
import argparse
from pathlib import Path
import time
def get_base_dir():
    return Path(__file__).parent.resolve()

def start_server(host='0.0.0.0', port='7878'):
    base_dir = get_base_dir().parent
    os.chdir(base_dir)
    
    pid_file = get_base_dir() / 'server.pid'
    if pid_file.exists():
        with open(pid_file, 'r') as f:
            pid = int(f.read())
            try:
                os.kill(pid, 0)
                print(f"Server is already running (PID: {pid})")
                return
            except OSError:
                print("Stale pid file found, removing...")
                pid_file.unlink()

    cmd = f"nohup python3 manage.py runserver {host}:{port} --noreload >> server.log 2>&1 &"
    process = subprocess.Popen(
        cmd,
        shell=True,  # 必须添加shell=True来支持重定向
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        stdin=subprocess.PIPE
    )
    
    time.sleep(1)  # 缩短等待时间
    try:
        # 精确匹配进程命令，排除grep进程本身
        cmd = f"pgrep -f 'python3 manage.py runserver {host}:{port}'"
        server_pid = subprocess.check_output(cmd, shell=True).decode().strip().split('\n')[0]
    except subprocess.CalledProcessError:
        print("Failed to get server PID")
        return

    # 只保留一个PID写入逻辑
    with open(pid_file, 'w') as f:
        f.write(server_pid)
    
    print(f"Starting server with command: {cmd}")
    # 移除实时日志输出（nohup已重定向到文件）
    
    print(f"Server started successfully (PID: {server_pid})")

def stop_server():
    pid_file = get_base_dir() / 'server.pid'
    if not pid_file.exists():
        print("Server is not running")
        return

    with open(pid_file, 'r') as f:
        pid = int(f.read())
    
    try:
        os.kill(pid, signal.SIGTERM)
        pid_file.unlink()
        print(f"Server stopped successfully (PID: {pid})")
    except ProcessLookupError:
        print(f"No running process with PID {pid}")
        pid_file.unlink()

def main():
    parser = argparse.ArgumentParser(description='Django Server Manager')
    subparsers = parser.add_subparsers(dest='command')
    
    # Start command
    start_parser = subparsers.add_parser('start')
    start_parser.add_argument('--host', default='0.0.0.0', help='Server host')
    start_parser.add_argument('--port', default='7878', help='Server port')
    
    # Stop command
    subparsers.add_parser('stop')
    
    # Restart command
    restart_parser = subparsers.add_parser('restart')
    restart_parser.add_argument('--host', default='0.0.0.0', help='Server host')
    restart_parser.add_argument('--port', default='7878', help='Server port')

    args = parser.parse_args()

    if args.command == 'start':
        start_server(args.host, args.port)
    elif args.command == 'stop':
        stop_server()
    elif args.command == 'restart':
        stop_server()
        start_server(args.host, args.port)
    else:
        parser.print_help()

if __name__ == '__main__':
    main()
