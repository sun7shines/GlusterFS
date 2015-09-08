# -*- coding: utf-8 -*-

import operation.gluster.peer_cmd
import vmd_utils

import os
import syslog
import traceback
import subprocess
import time

def do_stop_gluster():
    
    cmd = '/etc/init.d/glusterd stop'
    os.system(cmd)
    return True,''

def do_restart_gluster():
    
    cmd = '/etc/init.d/glusterd restart'
    os.system(cmd)
    time.sleep(10)
    return True,''
    
def gluster_has_rbtask(volume_desc):
    
    rbpath = '/var/lib/glusterd/vols/%s/rbstate' % (volume_desc)
    if os.path.exists(rbpath):
        try:
            f = open(rbpath)
            lines  = f.readlines()
            f.close()
        except:
            return False
        
        for line in lines:
            if line.strip() and line.strip() != 'rb_status=0':
                return True
        
    return False 


def gluster_status_running():

    cmd = ['/etc/init.d/glusterd','status']
    output=subprocess.Popen(cmd,
                            stdout=subprocess.PIPE,
                            stderr=open('/dev/null', 'w')
                            )

    out = output.stdout.read()

    if out.find("pid") != -1:
        return True
    else:
        return False
    
    
def active_gluster():
    
    pass
    #if not gluster_status_running():
    #    cmd = '/etc/init.d/glusterd restart'
    #    os.system(cmd)
  
def do_sync_volume(host_ip,volume_desc):
    
    cmd = 'gluster volume sync %s %s' % (host_ip,volume_desc)
    os.system(cmd)
    return True,''

