# -*- coding: utf-8 -*-

import system.network.dns_service_op
import operation.gluster.volume_db
import operation.gluster.volume_clr
import operation.gluster.volume_sys
import operation.gluster.volume_ifo
import operation.gluster.volume_cmd
import operation.gluster.volume_rpl_cfg
import operation.gluster.volume_rpl_cmd

import os

def data_rpl(data,param):
    
    volume_uuid = param['volume_uuid']
    brick_uuid = param['brick_uuid']
    new_brick = param['new_brick']
    
    #检查brick 和 new_brick存储的大小是否一致,new_brick的容量必须大于brick
    host_ip = system.network.dns_service_op.get_localhost_ip()
    
    volumeobj = operation.gluster.volume_db.get_gluster_volumeobj(volume_uuid)
    if not volumeobj:
        return False,host_ip+'volume does not exists' 
    
    volume_description = volumeobj['description']
    
    data['g_volume_uuid'] = volume_uuid
    data['g_brick_uuid'] = brick_uuid
    data['g_new_brick'] = new_brick
    data['g_host_ip'] = host_ip
    data['g_volume_description'] = volume_description
    data['g_type'] = volumeobj['type']
    return True,''

def pre_rpl(data):
    
    new_brick = data['g_new_brick']
    brick_uuid = data['g_brick_uuid']
    host_ip = data['g_host_ip']
    
    operation.gluster.volume_clr.clear_resources()
    
    hostuuid = new_brick['hostUuid']
    peer_ip = operation.gluster.volume_db.get_peer_ip(hostuuid)
    
    host_ips = operation.gluster.peer_db.get_host_ips()
    if peer_ip not in host_ips:
        return False,'replace brick on wrong host'
    
    new_brick['peer_ip'] = peer_ip
    new_brick['target_ip'] = operation.gluster.volume_db.get_target_ip(hostuuid)
    
    flag,brick_info = operation.gluster.volume_db.get_brick_info(brick_uuid)
    if not flag:
        return False,host_ip+'get replaced brick info failed'
    
    storage_path = new_brick['storage_path']
    gluster_dir = '%s/glusterfs_storage' % (storage_path)
    if not os.path.exists(gluster_dir):
        return False,host_ip+'brick path %s not exists' % (gluster_dir)
    
    cmd = 'rm -rf %s;mkdir %s' % (gluster_dir,gluster_dir)
    os.system(cmd)
    
    operation.gluster.volume_clr.delete_brick_attr([new_brick],delete=True)
    
    data['g_new_brick'] = new_brick
    data['g_brick_info'] = brick_info
    return True,''

def cmd_rpl(data):
    
    volume_description = data['g_volume_description']
    brick_info = data['g_brick_info']
    new_brick = data['g_new_brick']
    host_ip = data['g_host_ip']
   
    ''' 
    flag,msg = operation.gluster.volume_rpl_cmd.replace_brick_commit(volume_description, brick_info, new_brick)
    if not flag:
        return False,host_ip+msg
    '''
    flag,msg = operation.gluster.volume_rpl_cfg.gluster_replace_brick(volume_description, brick_info, new_brick)
    if not flag:
        return False,host_ip+msg
    
    return True,''

def heal_rpl(data):
    
    if data['g_type'] == 'Striped-Replicate':
        return True,''
    volume_description = data['g_volume_description']
    new_brick = data['g_new_brick']
    
    cmd = 'gluster volume heal %s full' % (volume_description)
    os.system(cmd)
        
    operation.gluster.volume_rpl_cfg.wait_for_replace(volume_description,new_brick)
    
    return True,''

