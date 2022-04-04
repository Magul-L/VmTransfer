import json
import math
import struct
import zipfile
from socket import *
from threading import Thread
from os.path import join
import os
import argparse
import time


peer_ip = ''
port = 25000
block_size = 1024*200
SHARE_DIR = './share'

new_file_set = set()
revfile_list = []                                                                                                       # record uncompleted files
dir_list = []
mtime_dict = {}                                                                                                         # record mtime of each file

local_file = os.listdir(SHARE_DIR)                                                                                      # record initial file in share


def get_argparse():
    parser = argparse.ArgumentParser(description="This is peer host IP!")
    parser.add_argument('--ip', action='store', required=True, dest='ip', help='ip addresses of peers')
    return parser.parse_args()

def creat_share_folder():
    try:
        os.mkdir('share')
        print('share folder created')
    except FileExistsError:
        print('share folder existed')

def scan_file():
# traversing share folder to detect new files and updating files
    global new_file_set, local_file
    file_num = len(local_file)                                                                                          # record how many files in "share" at beginning.
    if file_num != 0:
        for i in local_file:
            if not (i.startswith('compressd_') or i.startswith('received_')):
                if not i.startswith('changing_'):
                    mtime_dict[i] = int(os.stat(join(SHARE_DIR, i)).st_mtime)                                           #record its mtime
                    new_file_set.add(i)                                                                                 #add to new file set
                    if os.path.isdir(join(SHARE_DIR, i)):                                                               #it's a folder
                        dir_list.append(i)                                                                              #add to dir dict
                else:                                                                                                   #file is uncomplete
                    revfile_list.append(i)
    while True:
        file_detected = os.listdir(SHARE_DIR)                                                                           # record the current files in share
        for i in file_detected:
            if not (i.startswith('compressd_') or i.startswith('changing_') or i.startswith('received_')):
                if i not in local_file:                                                                                 #it is new file
                    new_file_set.add(i)
                    local_file.append(i)
                    mtime_dict[i] = int(os.stat(join(SHARE_DIR, i)).st_mtime)
                    if os.path.isdir(join(SHARE_DIR, i)):                                                               # it is folder
                        dir_list.append(i)
                else:                                                                                                   # check mtime
                    if mtime_dict[i] != int(os.stat(join(SHARE_DIR, i)).st_mtime):                                      #it is updated
                        if i not in new_file_set:
                            new_file_set.add(i)                                                                         #treat as a new file
                        mtime_dict[i] = int(os.stat(join(SHARE_DIR, i)).st_mtime)                                       #update mtime
        time.sleep(0.1)                                                                                                 #scan every 0.1 sec, save cpu

def make_msg(code, block_index, total_block, adict, data):
    jfile = json.dumps(adict)
    header = struct.pack('!4I', code, block_index, total_block, len(jfile))
    msg = header + jfile.encode() + data
    return msg

def send_msg(code, file_path, start_block, total_block, adict, data_length):
    send_socket = socket(AF_INET, SOCK_STREAM)
    send_socket.connect((peer_ip, port))
    for i in range(start_block, total_block + 1):
        with open(file_path, 'rb') as f:
            f.seek((i - 1) * data_length)
            file_block = f.read(data_length)

        msg = make_msg(code, i, total_block, adict, file_block)
        try:
            send_socket.sendall(msg)
        except ConnectionResetError:
            send_socket.close()
            break
    send_socket.close()

def send_end():
    global new_file_set, dir_list
    while True:
        if len(new_file_set) != 0 or len(revfile_list) != 0:
            send_socket = socket(AF_INET, SOCK_STREAM)
            try:
                send_socket.connect((peer_ip, port))
            except ConnectionRefusedError:
                continue
            else:
                if len(revfile_list) == 0:
                    temp_dict = {}
                    for i in new_file_set:
                        temp_dict[i] = mtime_dict[i]                                                                    #record filename and mtime
                    msg = make_msg(0, 0, 0, temp_dict, b'')                                                             #ask for synchronization
                    send_socket.sendall(msg)

                    rev = send_socket.recv(1024)
                    code = struct.unpack('!I', rev[:4])[0]
                    send_socket.close()

                    if code == 0:                                                                                       #peer has held the file
                        new_file_set = new_file_set-set(temp_dict)

                    elif code == 2:                                                                                     #peer ask for file, using dict
                        read_length = struct.unpack('!I', rev[12:16])[0]
                        need_file_dict = json.loads(rev[16:16 + read_length].decode())
                        for i in need_file_dict.keys():                                                                 #send all needed files
                            if i not in dir_list:                                                                       #not folder
                                file_dict = {i: need_file_dict[i]}
                                jfile = json.dumps(file_dict)
                                data_length = block_size - (16 + len(jfile))
                                file_size = os.path.getsize(join(SHARE_DIR, i))
                                block_number = math.ceil(file_size / data_length)
                                try:
                                    send_msg(1, join(SHARE_DIR, i), 1, block_number, file_dict, data_length)
                                except ConnectionRefusedError:                                                          # Prevent client from losing connection
                                    break
                            else:
                                compress("./share/" + i, "./share/compressd_" + i + ".zip")                             # compress file and send
                                file_dict = {i: need_file_dict[i]}
                                jfile = json.dumps(file_dict)
                                data_length = block_size - (16 + len(jfile))
                                file_size = os.path.getsize(join(SHARE_DIR, "compressd_" + i + ".zip"))
                                block_number = math.ceil(file_size / data_length)
                                try:
                                    send_msg(3, join(SHARE_DIR, "compressd_" + i + ".zip"), 1, block_number, file_dict, data_length)
                                except ConnectionRefusedError:
                                    break
                                else:
                                    os.remove(join(SHARE_DIR, "compressd_" + i + ".zip"))                               #delete zip file
                            new_file_set.remove(i)
                else:
                    recover_dict = {}
                    for i in revfile_list:
                        name = i.replace('changing_', '')
                        with open(join(SHARE_DIR, "received_" + name.split(".")[0] + ".txt"), 'r') as r:
                            have_read = r.read()
                            recover_dict[name] = int(have_read)
                    msg = make_msg(4, 0, 0, recover_dict, b'')                                                          #reconnect transmission
                    send_socket.sendall(msg)
                    revfile_list.clear()                                                                                #empty the record set
                    send_socket.close()
        time.sleep(0.2)                                                                                                 #save cpu

def receive_end():
    rev_socket = socket(AF_INET, SOCK_STREAM)
    rev_socket.bind(('', port))
    rev_socket.listen(2)
    while True:
        s, addr = rev_socket.accept()
        while True:
            msg = s.recv(block_size)
            if msg != b'':
                code = struct.unpack('!I', msg[:4])[0]
                print("code received")
                if code == 0:                                                                                           #synchronization
                    read_length = struct.unpack('!I', msg[12:16])[0]
                    new_file = json.loads(msg[16:16 + read_length].decode())
                    need_file_dict = check_need(new_file)
                    if len(need_file_dict) == 0:
                        s.sendall(make_msg(0, 0, 0, {}, b''))                                                           #don't need any files
                        print("no need file")
                        break
                    else:
                        s.sendall(make_msg(2, 0, 0, need_file_dict, b''))                                               #reply need_file_dict
                        print("reply needed files")
                elif code == 1:                                                                                         #start to accept file
                    rev_header(msg, s)
                    print("received a file")
                    break
                elif code == 3:                                                                                         #start to accept folder
                    rev_folder(msg, s)
                    print("received a folder")
                    break
                elif code == 4:                                                                                         #reconnect transmission
                    read_length = struct.unpack('!I', msg[12:16])[0]
                    recover_file = json.loads(msg[16:16 + read_length].decode())
                    for i in recover_file.keys():
                        file_dict = {i: mtime_dict[i]}
                        jfile = json.dumps(file_dict)
                        data_length = block_size - (16 + len(jfile))
                        file_size = os.path.getsize(join(SHARE_DIR, i))
                        block_number = math.ceil(file_size / data_length)
                        if i not in dir_list:
                            send_msg(1, join(SHARE_DIR, i), recover_file[i] + 1, block_number, file_dict, data_length)
                        else:
                            send_msg(3, join(SHARE_DIR, "compressd_" + i + ".zip"), recover_file[i] + 1, block_number,
                                        file_dict, data_length)
                            os.remove(join(SHARE_DIR, "compressd_" + i + ".zip"))
                    break
                else:
                    s.close()
                    break
            else:
                s.close()
                break

def set_mtime(file_path, updatetime):
    localtime = time.localtime(updatetime)
    time_str = time.strftime("%Y-%m-%d %H:%M:%S", localtime)
    new = time.mktime(time.strptime(time_str, "%Y-%m-%d %H:%M:%S"))
    os.utime(file_path, (new, new))

def rev_header(msg, socket):
    block_number, length = struct.unpack('!2I', msg[8:16])
    new_file = json.loads(msg[16:16 + length].decode())
    name = list(new_file.keys())[0]
    temp = join(SHARE_DIR, "changing_" + name)
    record_name = join(SHARE_DIR, "received_" + name.split(".")[0] + ".txt")
    data_length = block_size-16-length                                                                                  # compute the length of binary data length
    while True:
        block_index = struct.unpack('!I', msg[4:8])[0]
        if len(msg) == block_size or block_index == block_number:                                                       # accept a complete packet segment
            try:
                with open(temp, 'rb+') as t:                                                                            # write file
                    t.seek((block_index-1) * data_length)
                    t.write(msg[16+length:])
            except FileNotFoundError:
                with open(temp, 'wb+') as t:                                                                            # create file
                    t.seek((block_index - 1) * data_length)
                    t.write(msg[16 + length:])

            try:
                with open(record_name, 'r+') as r:
                    r.write(str(block_index))
            except FileNotFoundError:
                with open(record_name, 'w+') as r:
                    r.write(str(block_index))

            if block_index == block_number:
                set_mtime(join(SHARE_DIR,"changing_" + name), new_file[name])
                try:
                    os.rename(join(SHARE_DIR, "changing_" + name), join(SHARE_DIR, name))
                except FileExistsError:
                    os.remove(join(SHARE_DIR, name))
                    os.rename(join(SHARE_DIR, "changing_" + name), join(SHARE_DIR, name))
                os.remove(join(SHARE_DIR, "received_" + name.split(".")[0] + ".txt"))
                break
            try:
                msg = socket.recv(block_size)
            except ConnectionResetError:
                print("disconnect")
                break
        else:
            if msg == b'':
                socket.close()
                break
            else:
                msg = msg+socket.recv(block_size-len(msg))

def rev_folder(msg, socket):
    block_number, length = struct.unpack('!2I', msg[8:16])
    new_file = json.loads(msg[16:16 + length].decode())
    name = list(new_file.keys())[0]
    data_length = block_size - 16 - length
    content = msg
    while True:
        if len(msg) == block_size or content == b'':
            block_index = struct.unpack('!I', msg[4:8])[0]
            try:
                with open(join(SHARE_DIR, "compressd_" + name + ".zip"), 'rb+') as k:
                    k.seek((block_index - 1) * data_length)
                    k.write(msg[16 + length:])
            except FileNotFoundError:
                with open(join(SHARE_DIR, "compressd_" + name + ".zip"), 'wb+') as k:
                    k.seek((block_index - 1) * data_length)
                    k.write(msg[16 + length:])
            try:
                with open(join(SHARE_DIR, "received_" + name.split(".")[0] + ".txt"), 'r+') as w:
                    w.write(str(block_index))
            except FileNotFoundError:
                with open(join(SHARE_DIR, "received_" + name.split(".")[0] + ".txt"), 'w+') as w:
                    w.write(str(block_index))
            if block_index == block_number:
                os.remove(join(SHARE_DIR, "received_" + name.split(".")[0] + ".txt"))
                break
            msg = socket.recv(block_size)
        else:
            content = socket.recv(block_size-len(msg))
            if content == b'':
                socket.close()
                continue
            else:
                msg = msg+content

    decompress(join(SHARE_DIR, "compressd_" + name + ".zip"), join(SHARE_DIR,"changing_" + name))
    set_mtime(join(SHARE_DIR,"changing_" + name), new_file[name])
    os.rename(join(SHARE_DIR, "changing_" + name), join(SHARE_DIR, name))
    os.remove(join(SHARE_DIR, "compressd_" + name + ".zip"))

def check_need(dict):                                                                                                   # detect file update by compare mtime
    global local_file, mtime_dict
    for i in list(dict.keys()):
        if i in local_file:                                                                                             # find whether the file is held
            if dict[i] == mtime_dict[i]:
                del dict[i]
    return dict

def compress(path, out_name):
    with zipfile.ZipFile(out_name, "w", zipfile.ZIP_DEFLATED) as zz:
        for qpath, dirnames, filenames in os.walk(path):
            fpath = qpath.replace(path, '')
            for filename in filenames:
                zz.write(os.path.join(qpath, filename), os.path.join(fpath, filename))

def decompress(zip_file, path):
    with zipfile.ZipFile(zip_file, 'r') as zz:
        for file in zz.namelist():
            zz.extract(file, path)

def main():
    global peer_ip
    creat_share_folder()
    peer_ip = get_argparse().ip
    t1 = Thread(target=scan_file)
    t1.daemon = True                                                                                                    # terminated along with main thread
    t1.start()
    t2 = Thread(target=receive_end)
    t2.daemon = True
    t2.start()
    t3 = Thread(target=send_end)
    t3.daemon = True
    t3.start()
    time.sleep(400823823)                                                                                               #stop

if __name__ == '__main__':
    main()


