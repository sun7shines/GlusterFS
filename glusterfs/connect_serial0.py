#!/usr/bin/python
# -*- coding: utf-8 -*-

import time
import socket
import sys
import syslog
import traceback
from vmlib import fwprint

def monitor_exec_base(uuid, cmd, timeout):
    
    # 修改monit获取结果超时最少为60秒。changeimg超时用30秒不够
    if timeout < 60:
        timeout = 60
        
    cmd = str(cmd)
    if "\r\n" not in cmd:
        cmd = cmd + "\r\n"
            
    try:
        serial_file = "/var/run/%s/monit.sock" % uuid
        s = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
        s.settimeout(2)  # 设置recv超时为2秒
        s.connect(serial_file)
        # connect结束，不能立即recv，否则大多数情况都是timeout失败
        time.sleep(0.1)
    except:
        syslog.syslog(syslog.LOG_ERR, "VM exe cmd:"+ uuid + ":" + cmd.strip() + ":False:ConnectFailed:" + traceback.format_exc())
        fwprint( "MONIT0:connectError:",uuid,":",str(cmd.strip()),":",traceback.format_exc())
        return (False, "Connect vnc failed")
    
    strs = ""
    num = 0
    try:
        try:
            recv_length = 2048
            # 读取清理掉缓存，确保monit正确返回到提示符，再执行命令，保证命令执行成功。
            # 用脚本反复的连接，打印recv结果表明，每次新开启的连接，都会有前缀缓存存在，次数先recv清理缓存的做法是正确的。
            # 此处理依赖虚拟化版本兼容支持，如果虚拟化版本变更成每次连接前无缓存，则此处理将会造成所有monit的命令2秒超时失败。
            try:
                tmpstrs1 = s.recv(recv_length)
                syslog.syslog(syslog.LOG_ERR, "VM exe cmd:"+ uuid + ":" + cmd.strip() + ":recv before send cmd:" + tmpstrs1)
            except:
                fwprint( "MONIT0:",uuid,":",str(cmd.strip()),":",traceback.format_exc())
                pass
            
            # 写入命令。
            s.send(cmd)
            
            while num < timeout * 5:
                # 由于recv收到大量无意义的字符串，很快达到2048大小，造成recv很快返回，最终造成timeout失去意义，尝试30次，并不等于30秒，远远小于30秒
                # 故这里加上sleep处理，使得超时真正意义的达到
                time.sleep(0.1)
                
                num += 1
                try:
                    result = s.recv(recv_length)
                except:
                    fwprint( "MONIT1:",uuid,":",str(cmd.strip()),":",traceback.format_exc())
                    continue
                strs = strs + result
                if ("VM status:" in strs):
                    if ("paused" in strs) or ("running" in strs):
                        break
                if ("finish" in strs):
                    break
                if ("success" in strs) or ("Success" in strs):
                    break
                if ("fail" in strs) or ("Fail" in strs):
                    break
                if ("Error" in strs) or ("error" in strs):
                    break
                if ("100%" in strs) or ("complete" in strs):
                    break
        except:
            # 可能vServer非常卡，在某次读取结果时超时，此时不能立即返回失败，可依据曾经读取的残缺的结果判断是否成功，有可能命令本身是成功的。
            syslog.syslog(syslog.LOG_ERR, "VM exe cmd:Error:"+ str(uuid)+str(cmd.strip())+":result:" + str(strs)[-1000:])
            syslog.syslog(syslog.LOG_ERR, "VM exe cmd:Error:"+ str(uuid)+str(cmd.strip())+":"+traceback.format_exc())
            fwprint( "MONIT2:",uuid,":",str(cmd.strip()),":",traceback.format_exc())
    finally:
        s.close()
        fwprint( "MONIT3:",uuid,":",str(cmd.strip()),":Socket Close:")
    if ("fail" in strs) or ("Fail" in strs) or  ("Error" in strs) or ("error" in strs):
        flag = False
    else:
        flag = True
        if num >= timeout:
            if not strs:
                flag = False
                strs = "Vm monit time out:" + str(timeout) 
                # monit超时失败，且无任何日志输出。
            elif ("success" in strs) or ("Success" in strs):
                pass
            elif ("100%" in strs) or ("complete" in strs):
                pass
            # 暂时屏蔽“任务执行超时，没有完成”的这种情况，
            # 避免kvm的monit部分命令代码没有修改，没添加返回success等字样，出现错误的判断
            # 当任务真正出现超时失败时，无法识别出来
            else:
                fwprint( "MONIT4:",uuid,":",str(cmd.strip()),":Get Result TIMEOUT:", timeout)
#                 flag = False
#                 # monit超时失败，无成功结果输出。
        
    syslog.syslog(syslog.LOG_ERR, "VM exe cmd:"+ str(uuid) + ":" + str(cmd.strip()) + ":" + str(flag) +":"+ str(strs)[-1000:])
    fwprint( "MONIT5:",uuid,":",str(cmd.strip()),":",strs)
    return (flag, str(strs))

def send_cmd_get_result(serial_file, cmd):

    s = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
    s.connect(serial_file)
    s.settimeout(None)
    # connect结束，不能立即recv，否则大多数情况都是timeout失败
    time.sleep(1)
    s.send(cmd)
    
    strs = ""
    try:
        while True:
            tmp1 = s.recv(1024)
            strs = strs + tmp1
            fwprint( "Recv:",strs)
    finally:
        s.close()
        fwprint( "LOG:Socket is close!")

def run():
    
    if len(sys.argv) < 3:
        return
    cmd = " ".join(sys.argv[2:])
    
    if "monit.sock" in sys.argv[1]:
        uuid = sys.argv[1].split("/")[-2]
        ot = monitor_exec_base(uuid, cmd, 30)
        fwprint( ot[0])
        fwprint( ot[1])
    else:
        send_cmd_get_result(sys.argv[1], cmd)
        
if __name__ == "__main__":
    
    fwprint( "Help:python /usr/vmd/connect_serial0.pyc /var/run/vmUuid/sockfilename cmd1 para1 para2")
    fwprint( "Help:Test in VMD Running: sockfilename = serial1.sock(For None) or monit.sock(For monit) or serial2.sock(For vmFireWall, kill vmd for Test)")
    fwprint( "Help:Test in VMD be killed: sockfilename = serial2.sock(For vmFireWall) or serial3/4.sock(For Anti-virus SoftWare)")
    run()
    sys.exit(0)
