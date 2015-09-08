# -*- coding: utf-8 -*-

import sys
import traceback
reload(sys)
sys.setdefaultencoding('utf-8')
sys.path.append("/usr/vmd")
import syslog
import time
import os
import os.path
import global_params

global_params.django = True
sys.path.append("/usr/")
import global_params
global_params.django = True
sys.path.append("/usr/")
from django.conf import settings
sys.path.append("/usr/django_object")
try:
    import django_object.settings
    settings.configure(default_settings=django_object.settings)
except:
    pass

import support.cmd_exe
import new_subthread
import create_daemon
import subprocess
import operation.vstorage.storage_cmd_op
import vm_route
from vmlib import fwprint
import vmlib
gips_connects = {}
gips_state = {}

HOST_IP = ''
ROUTE_IP = ''
GSTORAGES = []
BRICKS = []
IPS = []
GVOLUME = ''

THDS = {} 
def update_global_ip_connect(host_ip, flag):
    fwprint(  'state   '+str(host_ip)+' '+str(flag)  )
    if host_ip not in gips_connects:
        gips_connects[host_ip] = True

    if flag != gips_connects.get(host_ip):
        gips_connects[host_ip] = flag
        fwprint ('host state change'  )
        if flag:
            gips_state[host_ip] = 'starting'
        else:
            gips_state[host_ip] = 'downing'
    return


def update_connect(host_ip):
    num = 0
    while True:
        num = num + 1
        cmd = "ping %s -c 1 -W 1 > /dev/null" % host_ip
        if 0 == os.system(cmd):
            update_global_ip_connect(host_ip, True)
            break
        time.sleep(2)
        if num>=2:
            break
    if num >=2:
        update_global_ip_connect(host_ip,False)
    return True 

def wait_gips_state(host_ip):

    while True:
        try:
            update_connect(host_ip)
        except:
            syslog.syslog(syslog.LOG_ERR,'wate_gips_state: '+str(traceback.format_exc()))
            fwprint( str(traceback.format_exc()))
        time.sleep(3)

def pause_vm(uuid):

    cmd = '/usr/bin/python /usr/vmd/glusterfs/connect_serial0.py /var/run/%s/monit.sock stop' % (uuid)
    fwprint (cmd)
    os.system(cmd)       

def cont_vm(uuid):

    cmd = '/usr/bin/python /usr/vmd/glusterfs/connect_serial0.py /var/run/%s/monit.sock cont' % (uuid)
    fwprint (cmd)
    os.system(cmd)

def in_heal_state(volume_descrption,img):
    
    cmd = 'gluster volume heal %s info' % (volume_descrption)
    output = support.cmd_exe.cmd_exe(cmd)
    if not output[0]:
        return 0 
    
    lines = output[1]['stdout']
    
    for line in lines:
        fwprint (line)
        if  line.lower().find('poss') != -1:
            return 1 
        if line.lower().find(img) != -1:
            return 2        
    return  0
    

def heal_split_brain(uuid):

    THDS[uuid] = True
    fwprint ('pause vm')

    pause_vm(uuid)
    fwprint ('----------------------------------------')
    time.sleep(30)

    for storage_path in GSTORAGES: 
        xxx = 0 
        while True:
            if xxx == 0:
                # 集群存储切换，此处没有必要
                time.sleep(30)
            else:
                time.sleep(20)
            (flag, state) = operation.vstorage.storage_cmd_op.storage_path_writeable(storage_path)
            if flag:
                fwprint ('storage ok')
                xxx = xxx+1
                if xxx == 4:
                    break
                continue

            fwprint ('storage not ok')

            xxx = xxx +1
            syslog.syslog(syslog.LOG_ERR,'xxxxxxxxxxxxxxxxxxxxxxxxxx:  '+str(xxx))

            fwprint ('++++++++++++++++++++++++++++++++++++++++')
    running_imgs = vmlib.get_running_imgs(uuid,GSTORAGES)
    fwprint( str(running_imgs))
    PROCS = []
    for img in running_imgs:
        brick = ''
        for brick in BRICKS:
            if not os.path.exists(brick+'/'+img):
                continue
            else:
                break

        if not brick:
            continue

        while True:
            flag = in_heal_state(GVOLUME,img)
            if flag == 0:
                break

            if flag == 1:
                time.sleep(3)
                continue
            break

        src_brick = "%s:%s" % (HOST_IP,brick)
        cmd = ['gluster','volume','heal',GVOLUME,'split-brain','source-brick',src_brick,img]
        fwprint( str(cmd))
        child = subprocess.Popen(cmd)
        PROCS.append(child)

    for child in PROCS:
        child.wait()

    fwprint( '========================================')
    cont_vm(uuid)
    THDS[uuid] = False
    fwprint( 'cont vm')
  
def theal_split_brain(uuid):
    try:
        heal_split_brain(uuid)
    except:
        fwprint( str(traceback.format_exc()))

        syslog.syslog(syslog.LOG_ERR,'heal_split_brain: '+str(traceback.format_exc())) 
def sys_heal_vms():

    while True:
        for key in IPS:
            if gips_state.get(key) == 'starting':
                fwprint( 'host starting')
                cmd = '''find /var/run -name 'monit.sock' '''
                output = support.cmd_exe.cmd_exe(cmd)
                if not output[0]:
                    continue 
                lines = output[1]['stdout']
                for line in lines:
                    line = line.strip()
                    if len(line)!= 40:
                        continue
                    uuid = line[9:-11]
                    if not THDS.get(uuid):
                        new_subthread.addtosubthread("heal_split_brain", theal_split_brain,uuid)    
                gips_state[key] = 'running'

        time.sleep(3)

def tsys_heal_vms():
    try:
        sys_heal_vms()
    except:
        fwprint( str(traceback.format_exc()))

        syslog.syslog(syslog.LOG_ERR,'sys_heal_vms: '+str(traceback.format_exc()))
if __name__ == '__main__':

    route_file = '/usr/vmd/glusterfs/conf/route_ip'
    peer_file = '/usr/vmd/glusterfs/conf/peer_ip'
    host_file = '/usr/vmd/glusterfs/conf/host_ip'
    brick_file = '/usr/vmd/glusterfs/conf/source_brick'
    gstorage_file = '/usr/vmd/glusterfs/conf/gstorage'
    vol_file = '/usr/vmd/glusterfs/conf/gvolume'
    for line in file(peer_file).readlines():
        IPS.append(line[:-1])

    HOST_IP = file(host_file).readlines()[0].strip()
    ROUTE_IP = file(route_file).readlines()[0].strip()
    BRICKS = [x.strip() for x in file(brick_file).readlines() if x.strip()]
    GSTORAGES = [x.strip() for x in file(gstorage_file).readlines() if x.strip()]
    GVOLUME = file(vol_file).readlines()[0].strip()

    fwprint( 'HOST_IP    ' +HOST_IP)
    fwprint( 'ROUTE_IP   '+ROUTE_IP)
    fwprint( 'BRICKS     '+str(BRICKS))
    fwprint( 'GSTORAGES  '+str(GSTORAGES))

    no_fork = False
    if len(sys.argv) > 1:
        if sys.argv[1] == "-d":
            no_fork = True
    if not no_fork:
        create_daemon.daemonize()

    if global_params.vcflag:
        sys.exit(0)

    global_params.init_threadlock()
    
    for host_ip in IPS:
        new_subthread.addtosubthread("wait_gips_state", wait_gips_state,host_ip)
    new_subthread.addtosubthread("sys_heal_vms",tsys_heal_vms)
    new_subthread.addtosubthread("vm_route",vm_route.loop_route_state,'192.168.1.1')    
    while True:
        time.sleep(5)
    
