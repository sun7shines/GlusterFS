# -*- coding: utf-8 -*-

import system.network.dns_service_op

import operation.gluster.volume_db
import operation.gluster.volume_clr
import operation.gluster.volume_sys
import operation.gluster.volume_ifo
import operation.gluster.volume_cmd

import os
import time
import syslog
import support.cmd_exe

def data_rmv(data,param):
    
    volume_uuid = param['volume_uuid']
    data['g_brick_uuids'] = param['brick_uuids']
    
    host_ip = system.network.dns_service_op.get_localhost_ip()


    volumeobj = operation.gluster.volume_db.get_gluster_volumeobj(volume_uuid)
    if not volumeobj:
        return False,host_ip+'volume does not exists' 
    
    data['g_volume_description'] = volumeobj['description']
    data['g_distribute_count'] = volumeobj['distributedcount']
    data['g_host_ip'] = host_ip
    data['g_volume_uuid'] = volume_uuid
    
    return True,''

def pre_rmv(data):
    
    brick_uuids = data['g_brick_uuids']
    distribute_count = data['g_distribute_count']
    host_ip = data['g_host_ip']
    
    operation.gluster.volume_clr.clear_resources()
    
    if len(brick_uuids) != distribute_count:
        return False,host_ip+'bricks count error ' + str(len(brick_uuids))
    
    #检查所删除brick的distributeid是否一致
    
    remove_bricks = operation.gluster.volume_db.get_remove_bricks(brick_uuids)
    if len(remove_bricks) != len(brick_uuids):
        return False,host_ip+'get remove bricks failed'
    
    data['g_remove_bricks'] = remove_bricks
    data['g_distribute_id'] = remove_bricks[0]['distribute_id']
    
    return True,''

def cmd_rmv(data):
    
    volume_description = data['g_volume_description']
    remove_bricks = data['g_remove_bricks']
    host_ip = data['g_host_ip']
    
    cmd = operation.gluster.volume_cmd.remove_brick_cmd(volume_description, remove_bricks)
    (cmdstat, rparams) = support.cmd_exe.cmd_exe(cmd)
    if not cmdstat:
        syslog.syslog(syslog.LOG_ERR,str(rparams))
        return False,host_ip+'移除分布式存储块单元失败'

    flag,volume_info = operation.gluster.volume_ifo.get_volume_infos()
    if not flag or not volume_info:
        return False,host_ip+'get volume info failed' 
    
    data['g_volume_info'] = volume_info
    return True,''
