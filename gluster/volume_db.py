# -*- coding: utf-8 -*-

import syslog
import traceback
import subprocess

import support.uuid_op
import support.cmd_exe
import vmd_utils
import operation.gluster.peer_db
import operation.gluster.peer_cmd

import operation.vstorage.storage_db_op
import dbmodule.db_module_interface

from dbmodule.db_op import *

import os

import time
import commands

def get_volume_uuid_by_desc(desc):
    #只能在vcenter端执行
    
    volume = db_get('glustervolume',{'description':desc})
    return volume.get('uuid')

def get_brick_uuids_by_storages(storage_uuids):
    #只能在vcenter端执行
    brick_uuids = []
    module_object = dbmodule.db_module_interface.DbMessageObject(db_name='glusterbrick')
    for storage_uuid in storage_uuids:
        module_object.message["field1"] = {'storage':{'db_name':'storages','field':{'uuid':storage_uuid}}}
        flag,msg = module_object.select_by_fkey()
        if flag and msg:
            brick_uuids.append(msg['uuid'])
    return brick_uuids

def get_peer_ip(hostuuid):
    is_vcuuid,vcuuid,vc_ip=support.uuid_op.get_vc_uuid()
    if is_vcuuid and vcuuid!="127.0.0.1":
        module_object = dbmodule.db_module_interface.DbMessageObject(db_name='glusterpeer',ip_d=vc_ip)
        module_object.message['field1'] = {'host':{'db_name':'hosts','field':{'uuid':hostuuid}}}
        flag,msg = module_object.select_by_fkey()
        if not flag or not msg:
            syslog.syslog(syslog.LOG_ERR,'get_peer_ip: '+str(traceback.format_exc()))
            return ''
        return msg[0]['glusterip']

def get_target_ip(hostuuid):
    
    is_vcuuid,vcuuid,vc_ip=support.uuid_op.get_vc_uuid()
    if is_vcuuid and vcuuid!="127.0.0.1":
        module_object = dbmodule.db_module_interface.DbMessageObject(db_name='hosts',ip_d=vc_ip)
        module_object.message['field1'] = {'uuid':hostuuid}
        flag,msg = module_object.select()
        if not flag or not msg:
            syslog.syslog(syslog.LOG_ERR,'get_peer_ip: '+str(traceback.format_exc()))
            return ''
        return msg[0]['host_ip']
    return ''

def get_symbol_ids(count):
    
    ids = []
    for i in range(0,count):
        symbol = support.uuid_op.get_random_word(8)
        ids.append(symbol)
    return ids

def get_new_bricks(data):
    
    bricks = data['g_p_bricks']
    host_ip = data['g_host_ip']
    
    new_bricks = []
    i = 0
    for brick in bricks:
        hostuuid = brick['hostUuid']
        storage_path = brick['storage_path']
        peer_ip = get_peer_ip(hostuuid)
        target_ip = get_target_ip(hostuuid)
        if not peer_ip:
            continue
        
        new_bricks.append({'hostUuid':hostuuid,'storage_path':storage_path,'peer_ip':peer_ip,'target_ip':target_ip,'stripeId':brick.get('stripeId'),
                           'replicaId':brick.get('replicaId'),'distributeId':brick.get('distributeId'),'description':brick.get('description')})
        i = i + 1
    
    if len(bricks) != len(new_bricks):
        return False,host_ip+'get new bricks error'
        
    data['g_new_bricks'] = new_bricks
    return True,''
    
def volume_description_duplicated(volume_description):
    
    #只能在vcenter端执行
    
    is_vcuuid,vcuuid,vcip=support.uuid_op.get_vc_uuid()
    if is_vcuuid and vcuuid!="127.0.0.1":
        volumes = db_values_vc('glustervolume',{'description':volume_description},vcip)
        if len(volumes) != 0:
            return True,
    return False

def insert_gluster_volume(data):
    
    dc_uuid = data['g_dc_uuid']
    volume_description = data['g_volume_description']
    stripe_count = data['g_stripe_count']
    replica_count = data['g_replica_count']
    volume_info = data['g_volume_info']
    cmdinfo  = data.get('g_cmdinfo','')
    host_ip = data['g_host_ip']
    
    if not volume_info.has_key(volume_description):
        return False,host_ip+'insert volume info error'
    volume_uuid = support.uuid_op.get_uuid()
    sysid = volume_info[volume_description].get('sysid')
    volume_type = volume_info[volume_description].get('type') # 替换掉函数的参数值 
    status = volume_info[volume_description].get('status')
    transport_type = volume_info[volume_description].get('transport_type') #替换掉函数的参数值
    expression = volume_info[volume_description].get('expression')
    distribute_count = stripe_count * replica_count
    
    is_vcuuid,vcuuid,vcip=support.uuid_op.get_vc_uuid()
    if is_vcuuid and vcuuid!="127.0.0.1":
        datacenter = db_get_vc('datacenters',{'uuid':dc_uuid},vcip)
        if not datacenter:
            return False,host_ip+'insert gluster volume info failed'
        insertparam = {'datacenter_id':datacenter.get('id'),
                       'uuid':volume_uuid,
                       'sysid':sysid,
                       'description':volume_description,
                       'type':volume_type,
                       'stripecount':stripe_count,
                       'replicacount':replica_count,
                       'distributedcount':distribute_count,
                       'status':status,
                       'expression':expression,
                       'transporttype':transport_type,
                       'cmdinfo':cmdinfo}
        
        flag,msg = db_save_vc('glustervolume',insertparam,vcip)
        if not flag:
            return False,host_ip+'insert gluster volume info failed'
        
    return True,''
          
def update_gluster_volume(data):
    
    volume_uuid = data['g_volume_uuid']
    volume_description = data['g_volume_description']
    volume_info = data['g_volume_info']
    host_ip = data['g_host_ip']
    
    if not volume_info.has_key(volume_description):
        return False,host_ip+'volume info error'
    
    volume_type = volume_info[volume_description].get('type') # 替换掉函数的参数值 
    status = volume_info[volume_description].get('status')
    expression = volume_info[volume_description].get('expression')
    
    is_vcuuid,vcuuid,vcip=support.uuid_op.get_vc_uuid()
    if is_vcuuid and vcuuid!="127.0.0.1":
        
        updateparam = {'type':volume_type,
                       'status':status,
                       'expression':expression}
        flag,msg = db_modify_vc('glustervolume',{'uuid':volume_uuid},updateparam,vcip)
        if not flag:
            return False,host_ip+'update gluster volume failed'
        
    return True,''

def insert_brick(volume_description,brick):
    
    # {'hostUuid':hostuuid,'storage_path':storage_path,'peer_ip':peer_ip,'distribute_id':distribute_id,'stripe_id':stripe_id,'replica_id':replica_id}
    hostuuid = brick.get('hostUuid')
    storage_path = brick.get('storage_path')
    distribute_id = brick.get('distributeId')
    stripe_id = brick.get('stripeId')
    replica_id = brick.get('replicaId')
    description = brick.get('description')
    brick_uuid = support.uuid_op.get_uuid()
    is_vcuuid,vcuuid,vcip=support.uuid_op.get_vc_uuid()
    
    module_object = dbmodule.db_module_interface.DbMessageObject(db_name = 'glusterbrick',ip_d=vcip)
    field1 = {'uuid':brick_uuid,'storage':{'db_name':'storages','field':{'mount_path':storage_path}},
              'host':{'db_name':'hosts','field':{'uuid':hostuuid}},
              'volume':{'db_name':'glustervolume','field':{'description':volume_description}},
              'stripeid':stripe_id,'replicaid':replica_id,'distributedid':distribute_id,'description':description}
    module_object.message['field1'] = field1
    flag,msg = module_object.insert_f()
    if not flag:
        return False,'insert brick failed'
        syslog.syslog(syslog.LOG_ERR,'insert_brick failed :'+str(traceback.format_exc()))
        syslog.syslog(syslog.LOG_ERR,'insert_brick failed :'+str(brick))
    return True,''

def insert_gluster_bricks(data):
    host_ip = data['g_host_ip']
    
    volume_description = data['g_volume_description']
    new_bricks = data['g_new_bricks']
    is_vcuuid,vcuuid,_=support.uuid_op.get_vc_uuid()
    if not is_vcuuid or not vcuuid!="127.0.0.1":
        return False,host_ip+'insert gluster brick: get volume info failed'
    
    for brick in new_bricks:
        flag,_ = insert_brick(volume_description,brick)
        
    return True,''

def get_gluster_volumeobj(volume_uuid):
    
    volumeobj = {}
    is_vcuuid,vcuuid,vcip=support.uuid_op.get_vc_uuid()
    if is_vcuuid and vcuuid!="127.0.0.1":
        volumeobj = db_get_vc('glustervolume',{'uuid':volume_uuid},vcip)
    return volumeobj

def get_gluster_volumeobj_vc(volume_uuid):
    
    volumeobj = db_get('glustervolume',{'uuid':volume_uuid})
    return volumeobj

def delete_gluster_bricks(data):
    
    volume_uuid=data.get('g_volume_uuid',None)
    distribute_id=data.get('g_distribute_id',None)
    host_ip = data.get('g_host_ip')
    
    msg = 'delete gluster brick info failed '
    
    try:
        is_vcuuid,vcuuid,vcip=support.uuid_op.get_vc_uuid()
        if is_vcuuid and vcuuid!="127.0.0.1":
            if volume_uuid and not distribute_id:
                volumeobj = get_gluster_volumeobj(volume_uuid)
                db_delete_vc('glusterbrick',{'volume_id':volumeobj['id']},vcip)
                return True,''
            
            elif distribute_id:
                
                db_delete_vc('glusterbrick',{'distributedid':distribute_id},vcip)
                return True,''
    except:
        syslog.syslog(syslog.LOG_ERR,msg+str(traceback.format_exc()))
        
    return False,host_ip+msg

def update_gluster_brick(data):
    
    brick_info = data['g_brick_info']
    new_brick = data['g_new_brick']
    host_ip = data['g_host_ip']
    
    msg = 'update gluster brick info failed '
    is_vcuuid,vcuuid,vcip=support.uuid_op.get_vc_uuid()
    if is_vcuuid and vcuuid!="127.0.0.1":
        module_object = dbmodule.db_module_interface.DbMessageObject(db_name='glusterbrick',ip_d=vcip)
        module_object.message['field1'] = {'uuid':brick_info['brick_uuid']}
        field2 = {'host':{'db_name':'hosts','field':{'uuid':new_brick['hostUuid']}},
                  'storage':{'db_name':'storages','field':{'mount_path':new_brick['storage_path']}},
                'description':new_brick['description']}
        module_object.message['field2'] = field2
        flag,msg = module_object.modify_f()
        if not flag:
            return False,host_ip+msg
        return True,''
    
    return False,host_ip+msg

def delete_gluster_volume(data):
    
    volume_uuid = data['g_volume_uuid']
    host_ip = data['g_host_ip']
    msg = 'delete gluster brick info failed '
    try:
        is_vcuuid,vcuuid,vcip=support.uuid_op.get_vc_uuid()
        if is_vcuuid and vcuuid!="127.0.0.1":
            
            db_delete_vc('glustervolume', {'uuid': volume_uuid}, vcip)
            return True,''
    except:
        syslog.syslog(syslog.LOG_ERR,msg+str(traceback.format_exc()))
    return False,host_ip+msg
    
def delete_gluster_volume_vc(volume_uuid):
    
    msg = 'delete gluster brick info failed '
    try:
        
        db_delete('glustervolume',{'uuid':volume_uuid})
        return True,''
    except:
        syslog.syslog(syslog.LOG_ERR,msg+str(traceback.format_exc()))
    return False,msg
  
def delete_gluster_bricks_vc(volume_uuid=None,distribute_id=None):
    
    msg = 'delete gluster brick info failed '
    
    try:
        if volume_uuid:
            
            volumeobj = db_get('gluservolume',{'uuid':volume_uuid})
            db_delete('glusterbrick',{'volume_id':volumeobj.get('id')})
            return True,''
        
        elif distribute_id:
            
            db_delete('glusterbrick',{'distributedid':distribute_id})
            return True,''
    except:
        syslog.syslog(syslog.LOG_ERR,msg+str(traceback.format_exc()))
    return False,msg
  
def get_gluster_cmd_host(volume_uuid):
    
    #只在vcenter端执行获取合适的执行命令的主机
    #vServer上不需要，自身即可作为执行命令的机器
    module_object = dbmodule.db_module_interface.DbMessageObject(db_name='glustervolume')
    module_object.message['field1'] = {'uuid':volume_uuid}
    flag,msg = module_object.select()
    if not flag or not msg:
        syslog.syslog(syslog.LOG_ERR,'get gluster cmd host failed ')
        return ''
    dc_id = msg[0]['datacenter_id']
    module_object = dbmodule.db_module_interface.DbMessageObject(db_name='hosts')
    module_object.message['field1'] = {'datacenter_id':dc_id,'host_state':'available'}
    flag ,msg = module_object.select()
    if not flag or not msg:
        syslog.syslog(syslog.LOG_ERR,'get gluster cmd host failed ')
        return ''
    return msg[0]['uuid']
    
def get_brick_info(brick_uuid):
    
    brick_info = {}
    try:
        is_vcuuid,vcuuid,vcip=support.uuid_op.get_vc_uuid()
        if is_vcuuid and vcuuid!="127.0.0.1":
            
            brickobj = db_get_vc('glusterbrick',{'uuid':brick_uuid},vcip)
            hostobj = xattrc('hosts',brickobj,'host_id',vcip)
            peerobj = db_get_vc('glusterpeer',{'host_id':hostobj.get('id')},vcip)
            
            brick_info['target_ip'] = hostobj.get('host_ip')
            brick_info['peer_ip'] = peerobj.get('glusterip')
            
            brick_info['storage_path'] = xattrc('storages',brickobj,'storage_id',vcip).get('mount_path')
            brick_info['distribute_id'] = brickobj.get('distributedid')
            brick_info['brick_uuid'] = brick_uuid
            
            return True,brick_info
    except:
        syslog.syslog(syslog.LOG_ERR,'get brick info '+str(traceback.format_exc()))
    return False,{}
 
def get_remove_bricks(brick_uuids):
    
    brick_infos = []
    for brick_uuid in brick_uuids:
        flag,brick_info = get_brick_info(brick_uuid)
        if not flag:
            continue
        brick_infos.append(brick_info) 
    return brick_infos

def get_delete_bricks(volume_uuid):
    
    try:
        is_vcuuid,vcuuid,vcip=support.uuid_op.get_vc_uuid()
        if is_vcuuid and vcuuid!="127":
            
            volumeobj = db_get_vc('glustervolume',{'uuid':volume_uuid},vcip)
            brickobjs = db_values_vc('glusterbrick',{'volume_id':volumeobj.get('id')},vcip)
            brick_uuids = []
            for brickobj in brickobjs:
                brick_uuids.append(brickobj['uuid'])
            return get_remove_bricks(brick_uuids)
    except:
        syslog.syslog(syslog.LOG_ERR,'get delete brick failed '+str(traceback.format_exc()))
    return []       
  
def get_vc_vols(host_uuid=None):
    
    if not host_uuid:
        _,host_uuid =support.uuid_op.get_vs_uuid()
        
    is_vcuuid,vcuuid,vc_ip=support.uuid_op.get_vc_uuid()
    if is_vcuuid and vcuuid!="127.0.0.1":
        module_object = dbmodule.db_module_interface.DbMessageObject(db_name='hosts',ip_d = vc_ip)
        module_object.message['field1'] = {'uuid':host_uuid}
        flag,msg = module_object.select()
        if not flag or not msg:
            return False,[]
        dc_id = msg[0]['datacenter_id']
        module_object = dbmodule.db_module_interface.DbMessageObject(db_name='glustervolume',ip_d = vc_ip)
        module_object.message['field1'] = {'datacenter_id':dc_id}
        flag,msg = module_object.select()
        if not flag or not msg:
            return False,[]
        return True ,msg
    return False,[]

def path_to_gluster(img_file):
    
    #file=gluster+tcp://1.2.3.4:24007/testvol/dir/a.img
    #对于/dev设备不处理
    try:
        if img_file.startswith('/mnt'):
            storage_path = ('/').join(img_file.split('/')[:3])
            if storage_path in ["/mnt/NFS", "/mnt/CIFS", "/mnt/LOCAL", "/mnt/iSCSI", "/mnt/SAN"]:
                storage_path = ('/').join(img_file.strip().split('/')[:5])
            img_path = ('/').join(img_file.split('/')[3:])
            
            storageparam = operation.vstorage.storage_db_op.get_storage(mount_path=storage_path)
            if storageparam['storage_type'] == 'GLUSTERFS':
                serviceip = storageparam['par']['serviceip'].strip()
                resource = storageparam['par']['resource'].strip()
                if resource.startswith('/'):
                    resource = resource[1:]
                
                #if serviceip.strip() == '127.0.0.1':
                #    transport = rdma
                #    transport = tcp
                gluster_path = 'gluster+tcp://%s:24007/%s/%s' % (serviceip,resource,img_path)
                return gluster_path
    except:
        syslog.syslog(syslog.LOG_ERR,'path to gluster: '+str(traceback.format_exc()))
        
    return img_file

def changeimg_cmd(src,dest):
    
    #return 'changeimg %s %s\n' % (path_to_gluster(src),path_to_gluster(dest))
    return 'changeimg %s %s\n' % (src,dest)

def get_removing_bricks(volume_desc,used_bricks):
    
    removing_bricks = []
    try:
        is_vcuuid,vcuuid,vcip=support.uuid_op.get_vc_uuid()
        if is_vcuuid and vcuuid!="127":
            
            volumeobj = db_get_vc('glustervolume',{'description':volume_desc},vcip)
            brickobjs = db_get_vc('glusterbrick',{'volume_id':volumeobj.get('id')},vcip)
            for use_brick in used_bricks:
                in_db = False
                for obj in brickobjs:
                    
                    if use_brick['storage_path'] == str(xattrc('storage',obj,'storage_id',vcip).get('mount_path')):
                        in_db = True
                        break
                if not in_db:
                    removing_bricks.append(use_brick)
    except:
        syslog.syslog(syslog.LOG_ERR,'get_removing_bricks: '+str(traceback.format_exc()))
        
    return removing_bricks
    
    
