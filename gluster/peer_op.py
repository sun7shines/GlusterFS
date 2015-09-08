# -*- coding: utf-8 -*-

import operation.gluster.peer_db
import operation.gluster.peer_cmd

import operation.gluster.volume_clr
import operation.gluster.volume_ifo

import system.network.dns_service_op
import vmd_utils
import support.uuid_op

import os

def create_peer(param):
    
    #没有错误返回，如果增加错误返回可以添加新的事件
    #检查主机，是否包含gluster cluster信息
    
    gluster_ip = param.get('gluster_ip') 
    
    operation.gluster.volume_clr.clear_peer_cfgs()
    flag,sysid = operation.gluster.volume_ifo.getsysid()
    if flag:
        operation.gluster.peer_db.insert_peer(sysid,gluster_ip)
    
    target_ip = param.get('target_ip')
    if target_ip and target_ip != 'None':
        (flag, psh) = vmd_utils.get_rpcConnection(target_ip)
        if not flag:
            return False,psh
        
        flag,msg = psh.do_probe_peer(gluster_ip)
        if not flag:
            return False,msg
        
    cmd = "echo '%s' > /var/lib/glusterd/glfs_ip" % (gluster_ip)
    os.system(cmd)
    
    return True,''

def delete_peer(param):
    
    #检查host上是否存在 被使用的brick
    dcuuid = operation.gluster.peer_db.get_host_dcuuid()
    
    gluster_ip = operation.gluster.peer_db.get_host_gluster_ip()
    if not gluster_ip:
        gluster_ip = system.network.dns_service_op.get_localhost_ip()
        
    is_vcuuid,vcuuid,vc_ip=support.uuid_op.get_vc_uuid()
    if is_vcuuid and vcuuid!="127.0.0.1":
        _,target_ip = operation.gluster.peer_db.get_available_peer_target_ip(dcuuid,gluster_ip, vcuuid,vc_ip)
        if target_ip and target_ip != 'None':
            operation.gluster.peer_cmd.detach_peer(target_ip,gluster_ip)
        
    operation.gluster.peer_db.clear_peer()
    operation.gluster.volume_clr.clear_peer_cfgs()
    return True,''


