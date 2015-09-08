# -*- coding: utf-8 -*-

import os
import syslog
import time
import traceback

import system.network.dns_service_op
import operation.gluster.volume_db
import operation.gluster.volume_clr
import operation.gluster.volume_sys
import operation.gluster.volume_ifo
import operation.gluster.volume_cmd

import vmd_utils

def data_del(data,param):
    
    data['g_volume_uuid'] = param['volume_uuid']
    
    data['g_host_ip'] = system.network.dns_service_op.get_localhost_ip()
    
    return True,''

def cek_del(data):
    
    volume_uuid = data['g_volume_uuid']
    host_ip = data['g_host_ip']
    volumeobj = operation.gluster.volume_db.get_gluster_volumeobj(volume_uuid)
    if not volumeobj:
        return False,host_ip+'volume does not exists'
    
    data['g_volume_description'] = volumeobj['description']
     
    return True,''

def pre_del(data):
    
    operation.gluster.volume_clr.clear_resources()
    
    return True,''

def cmd_del(data):
    
    volume_description = data['g_volume_description']
    host_ip = data['g_host_ip']
    
    if not operation.gluster.volume_clr.delete_volume_by_cmd(volume_description, 10):
        #删除成功后是否恢复，间隔为10秒
        flag,msg = operation.gluster.volume_clr.gluster_delete_vols([volume_description])
        if not flag:
            return False,host_ip+msg
        
    return True,''
