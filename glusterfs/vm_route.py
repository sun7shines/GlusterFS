
import os
import syslog
import time
import traceback
import support.cmd_exe
from vmlib import fwprint

gips_connects = {}
gips_state = {}


def pause_vm(uuid):

    cmd = '/usr/bin/python /usr/vmd/glusterfs/connect_serial0.py /var/run/%s/monit.sock stop' % (uuid)
    fwprint( cmd)
    os.system(cmd)

def cont_vm(uuid):

    cmd = '/usr/bin/python /usr/vmd/glusterfs/connect_serial0.py /var/run/%s/monit.sock cont' % (uuid)
    fwprint( cmd)
    os.system(cmd)

def pause_vms():

    cmd = '''find /var/run -name 'monit.sock' '''
    output = support.cmd_exe.cmd_exe(cmd)
    if not output[0]:
        return 
    lines = output[1]['stdout']
    for line in lines:
        line = line.strip()
        if len(line)!= 40:
            continue
        uuid = line[9:-11]
        pause_vm(uuid)

def cont_vms():

    cmd = '''find /var/run -name 'monit.sock' '''
    output = support.cmd_exe.cmd_exe(cmd)
    if not output[0]:
        return 
    lines = output[1]['stdout']
    for line in lines:
        line = line.strip()
        if len(line)!= 40:
            continue
        uuid = line[9:-11]
        cont_vm(uuid)

def update_route_connect(host_ip, flag):
    fwprint(  'route rrrr state   '+str(host_ip)+' '+str(flag))
    if host_ip not in gips_connects:
        gips_connects[host_ip] = True

    if flag != gips_connects.get(host_ip):
        gips_connects[host_ip] = flag
        fwprint( 'route rrrr state change')
        if flag:
            gips_state[host_ip] = 'route starting'
            cont_vms() 
        else:
            gips_state[host_ip] = 'route downing'
            pause_vms()
    return


def route_connect(host_ip):
    num = 0
    while True:
        num = num + 1
        cmd = "ping %s -c 1 -W 1 > /dev/null" % host_ip
        if 0 == os.system(cmd):
            update_route_connect(host_ip, True)
            break
        time.sleep(2)
        if num>=2:
            break
    if num >=2:
        update_route_connect(host_ip,False)
    return True

def loop_route_state(host_ip):

    while True:
        try:
            route_connect(host_ip)
        except:
            syslog.syslog(syslog.LOG_ERR,'loop_route_state: '+str(traceback.format_exc()))
        time.sleep(3)

