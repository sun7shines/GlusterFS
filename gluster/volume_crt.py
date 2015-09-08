# -*- coding: utf-8 -*-

import system.network.dns_service_op
import operation.gluster.volume_db
import operation.gluster.volume_clr
import os
import syslog
import time
import support.cmd_exe

def data_crt(data,param):
    
    data['g_volume_description'] = param.get('volume_description')
    data['g_volume_type'] = param.get('volume_type')
    data['g_stripe_count'] = param.get('stripe_count')
    data['g_replica_count'] = param.get('replica_count')
    data['g_transport_type'] = param.get('transport_type')
    data['g_dc_uuid'] = param.get('dc_uuid')
    data['g_p_bricks'] = param.get('bricks')
    
    data['g_stripe_count'] = int(data['g_stripe_count'])
    data['g_replica_count'] = int(data['g_replica_count'])
    if data['g_stripe_count'] < 1:
        data['g_stripe_count'] = 1
        
    if data['g_replica_count'] < 1:
        data['g_replica_count'] = 1
    
    data['g_host_ip'] = system.network.dns_service_op.get_localhost_ip()
    
    return True,''

def cek_crt(data):
    
    volume_description = data['g_volume_description']
    host_ip = data['g_host_ip']
    bricks = data['g_p_bricks']
    stripe_count = data['g_stripe_count']
    replica_count = data['g_replica_count']
    transport_type = data['g_transport_type']
    
    brick_count = len(bricks)
     
    if operation.gluster.volume_db.volume_description_duplicated(volume_description):
        return False,host_ip + 'volume descrtpion duplicated'
        
    
    if brick_count % (stripe_count * replica_count) != 0:
        return False,host_ip+'bricks count error '+ str(brick_count)
    
    if transport_type not in ['tcp','rdma','tcp,rdma','rdma,tcp']:
        return False,host_ip+'gluster transporttype error'
    
    data['g_brick_count'] = brick_count
    return True,''

def pre_crt(data):
    
    host_ip = data['g_host_ip']
    new_bricks = data['g_new_bricks']
    operation.gluster.volume_clr.clear_resources()
    
    flag,volume_infos = operation.gluster.volume_ifo.get_volume_infos()
    if not flag:
        return False,host_ip+'get volume info failed' 
    
    flag,msg = operation.gluster.volume_clr.clear_used_bricks(volume_infos, new_bricks, host_ip)
    if not flag:
        return False,msg
    
    operation.gluster.volume_clr.delete_brick_attr(new_bricks,delete=True)
    
    return True,''

def cmd_crt(data):

    host_ip = data['g_host_ip']
    cmd = operation.gluster.volume_cmd.volume_create_cmd(data)
    (cmdstat, rparams) = support.cmd_exe.cmd_exe(cmd)
  
    if not cmdstat:
        syslog.syslog(syslog.LOG_ERR,str(rparams))

        return False,host_ip+'create volume failed '
    
    cmd = operation.gluster.volume_cmd.volume_start_cmd(data)
    (cmdstat, rparams) = support.cmd_exe.cmd_exe(cmd)
    if not cmdstat:
        syslog.syslog(syslog.LOG_ERR,str(rparams))

        return False,host_ip+'start volume failed'
    
    return True,''

def data_db_crt(data):
    
    # 搜集所有需要的数据 
    
    host_ip = data['g_host_ip']    
    
    flag,volume_info = operation.gluster.volume_ifo.get_volume_infos()
    if not flag or not volume_info:
        return False,host_ip+'get volume info failed'

    data['g_volume_info'] = volume_info
    return True,''
