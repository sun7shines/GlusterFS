# -*- coding: utf-8 -*-

import syslog
import traceback

import support.cmd_exe
import os
import time

GLUSTER_INFO = '/var/lib/glusterd/glusterd.info'

def getsysid():
    
    try:
        if not os.path.exists(GLUSTER_INFO):
            os.system('gluster volume status')
            time.sleep(2)
            
        f = open(GLUSTER_INFO)
        lines = f.readlines()
        f.close()
        
        for line in lines:
            if line.startswith('UUID'):
                sysid = line.strip().split('UUID=')[1]
                return True,sysid
    except:
        syslog.syslog(syslog.LOG_ERR,'getsysid: '+str(traceback.format_exc()))
    return False,'get peer sysid failed'


def get_vol_user_info(volume_desc):

    vol_info = '/var/lib/glusterd/vols/%s/info' % (volume_desc)
    
    try:
        f = open(vol_info)
        lines = f.readlines()
        f.close()
    except:
        return False,{}
    
    vol_id = ''
    user_name = ''
    password = ''
    
    for line in lines:
        if line.startswith('volume-id'):
            vol_id = line.strip().split('=')[1].strip()
        if line.startswith('username'):
            user_name = line.strip().split('=')[1].strip()
        if line.startswith('password'):
            password = line.strip().split('=')[1].strip()
    return True,{'volume_id':vol_id,'username':user_name,'password':password}

def get_glfs_ip():
    try:
        fn = '/var/lib/glusterd/glfs_ip'
        if os.access(fn, os.F_OK):
            ip = file(fn).readline().strip()
            if ip:
                return ip
    except:
        syslog.syslog(syslog.LOG_ERR,'get_glfs_ip'+str(traceback.format_exc()))
    return ''


def get_single_volume_info(volume_descrption):
    
    tmp_info = {}
    
    cmd = 'gluster volume info %s' % (volume_descrption)
    output = support.cmd_exe.cmd_exe(cmd)
    if not output[0]:
        return tmp_info
    
    lines = output[1]['stdout']
    
    tmp_desc = ''
    tmp_type = ''
    tmp_id = ''
    tmp_status = ''
    tmp_exp = ''
    tmp_transport = ''
    tmp_bricks = []
    for line in lines:
        if not line.strip():
            continue
        if line.startswith('Volume Name'):
            tmp_desc = line.split(':')[1].strip()
            
        if line.startswith('Type'):
            tmp_type = line.split(':')[1].strip()
            
        if line.startswith('Volume ID'):
            tmp_id = line.split(':')[1].strip()
            
        if line.startswith('Status'):
            tmp_status = line.split(':')[1].strip()
            
        if line.startswith('Number of Bricks'):
            tmp_exp = line.split(':')[1].strip()
            
        if line.startswith('Transport-type'):
            tmp_transport = line.split(':')[1].strip()
            
        if line.startswith('Brick') and not line.startswith('Bricks'):
            tmp_bricks.append({'peer_ip':line.split(':')[1].strip(),'storage_path':line.split(':')[2].strip()[:-18]})
        
    if tmp_desc and tmp_type and tmp_id and tmp_status and tmp_exp and tmp_transport:
        tmp_info = {tmp_desc:{'description':tmp_desc,'type':tmp_type,'sysid':tmp_id,'status':tmp_status,
                              'expression':tmp_exp,'transport_type':tmp_transport,'bricks':tmp_bricks}}
                
    return tmp_info
    
def get_volume_infos(volume_descrption=''):
    
    cmd = 'gluster volume list'
    output = support.cmd_exe.cmd_exe(cmd)
    if not output[0]:
        return False,{}
    lines = output[1]['stdout']
    volume_infos = {}
    for line in lines:
        if not line.strip():
            continue
        tmp_desc = line.strip()
        tmp_info = get_single_volume_info(tmp_desc)
        if not tmp_info:
            continue
        if volume_descrption == tmp_desc:
            return True,tmp_info
        
        volume_infos.update(tmp_info)
    
        
    return True,volume_infos

def storage_in_gluster(storage_path):
    
    flag,volume_infos = get_volume_infos()
    for volume_desc in volume_infos:
        used_bricks = volume_infos[volume_desc]['bricks']
        for used_brick in used_bricks:
            if used_brick['storage_path'] == storage_path:
                return True 
    return False

def volume_info_exists(volume_desc):
    
    vol_dir = '/var/lib/glusterd/vols/%s' % (volume_desc)
    if os.path.exists(vol_dir):
        return True
    return False

