# -*- coding: utf-8 -*-

import syslog
import traceback
import os
import time

import sys

import support.uuid_op
import support.cmd_exe
import vmd_utils

import system.network.sent_rpc

import operation.vcluster.cluster_db_op
import dbmodule.db_module_interface
from dbmodule.db_op import *



def insert_peer(sysid,gluster_ip,state=None,healonline='no',healpid=0,cmdinfo=None,vsuuid=None):
    
    #只插入vcenter的数据库
    if not vsuuid:
        vsuuid = support.uuid_op.get_vs_uuid()[1]
    is_vcuuid,vcuuid,vc_ip =support.uuid_op.get_vc_uuid()
    if is_vcuuid and vcuuid!="127.0.0.1":
        module_object = dbmodule.db_module_interface.DbMessageObject(db_name = "glusterpeer",ip_d = vc_ip)
        field1 = {"host":{"db_name":"hosts","field":{"uuid":vsuuid}},
                  "sysid":sysid,"state":state,"healonline":healonline,"healpid":healpid,"cmdinfo":cmdinfo,"glusterip":gluster_ip}
        module_object.message["field1"] = field1
        flag,msg = module_object.insert_f()
        if not flag:
            syslog.syslog(syslog.LOG_ERR, "DELETE DB FAILED:") # traceback.extract_stack()
            syslog.syslog(syslog.LOG_ERR, "FILE:%s,FUN:%s,LINE:%s,ERROR_MSG:%s" % (sys._getframe().f_code.co_filename,sys._getframe().f_code.co_name,sys._getframe().f_lineno,msg))
            return (False,"insert peer info failed")
    return True,""

def clear_peer(vsuuid=None):
    if not vsuuid:
        vsuuid = support.uuid_op.get_vs_uuid()[1]
    is_vcuuid,vcuuid,vc_ip = support.uuid_op.get_vc_uuid()
    if is_vcuuid and vcuuid!="127.0.0.1":
        module_object = dbmodule.db_module_interface.DbMessageObject(db_name = "glusterpeer",ip_d = vc_ip)
        field1 = {"host":{"db_name":"hosts","field":{"uuid":vsuuid}}}
        module_object.message["field1"] = field1
        flag,msg = module_object.delete_f()
        if not flag:
            syslog.syslog(syslog.LOG_ERR, "DELETE DB FAILED:") # traceback.extract_stack()
            syslog.syslog(syslog.LOG_ERR, "FILE:%s,FUN:%s,LINE:%s,ERROR_MSG:%s" % (sys._getframe().f_code.co_filename,sys._getframe().f_code.co_name,sys._getframe().f_lineno,msg))
            return (False,"clear peer failed")
    return (True,'')

def get_probe_target_ip(vsuuid=None):
    
    if not vsuuid:
        vsuuid = support.uuid_op.get_vs_uuid()[1]
    is_vcuuid,vcuuid,vc_ip = support.uuid_op.get_vc_uuid()
    if is_vcuuid and vcuuid!="127.0.0.1":
        module_object = dbmodule.db_module_interface.DbMessageObject(db_name = "hosts",ip_d = vc_ip)   
        module_object.message["field1"] = {"uuid":vsuuid}
        module_object.message["mt_attrname"] = "datacenter__uuid"
        flag,msg = module_object.get_with_mtinfo()
        if not flag:
            syslog.syslog(syslog.LOG_ERR, "SELECT DB FAILED:") # traceback.extract_stack()
            syslog.syslog(syslog.LOG_ERR, "FILE:%s,FUN:%s,LINE:%s,ERROR_MSG:%s" % (sys._getframe().f_code.co_filename,sys._getframe().f_code.co_name,sys._getframe().f_lineno,msg))
            return (False,"get friend ip failed") 
        dc_uuid = msg["mt_attr_value"]
        vsuuid = msg["uuid"]
        
        module_object.message["field1"] = {"datacenter":{"db_name":"datacenters","field":{"uuid":dc_uuid}},
                                           "host_state":'available'}
        flag,msg = module_object.select_by_fkey()
        if not flag or not msg:
            syslog.syslog(syslog.LOG_ERR, "SELECT DB FAILED:") # traceback.extract_stack()
            syslog.syslog(syslog.LOG_ERR, "FILE:%s,FUN:%s,LINE:%s,ERROR_MSG:%s" % (sys._getframe().f_code.co_filename,sys._getframe().f_code.co_name,sys._getframe().f_lineno,msg))
            return (False,"get friend ip failed") 
        if len(msg)==1:
            return True,""
        for x in msg:
            if x["uuid"] != vsuuid:
                return True,x["host_ip"]


def convert_to_target_states(states):
    #该函数不是随处都可以使用的
    tg_states = []
    
    
    is_vcuuid,vcuuid,vcip=support.uuid_op.get_vc_uuid()
    if is_vcuuid and vcuuid!="127.0.0.1": 
        for hostname,conn_state,peer_state in states:
            try:
                peerobj = db_get_vc('glusterpeer',{'glusterip':hostname},vcip)
                target_ip = xattrc('hosts',peerobj,'host_id',vcip).get('host_ip')
                tg_states.append((target_ip,conn_state,peer_state))
            except:
                tg_states.append((hostname,conn_state,peer_state))
                syslog.syslog(syslog.LOG_ERR,'convert to target states: '+str(traceback.format_exc()))

    return True,tg_states


def collect_ips(dbname,field1,iplist,vcip):
    
    module_object = dbmodule.db_module_interface.DbMessageObject(db_name=dbname,ip_d=vcip)
    module_object.message['field1'] = field1
    flag,msg = module_object.select_by_fkey()
    if flag and msg:
        for y in msg:
            iplist.append(y['ip'])
            
    return True,''

def get_datacenter_ips():
    
    is_vcuuid,vcuuid,vcip=support.uuid_op.get_vc_uuid()
    if not (is_vcuuid and vcuuid!="127.0.0.1"):
        return {}

    ips = {}
    vsuuid = support.uuid_op.get_vs_uuid()[1]
    
    module_object = dbmodule.db_module_interface.DbMessageObject(db_name='hosts',ip_d=vcip)
    field1 = {'uuid':vsuuid}
    module_object.message['field1'] = field1
    module_object.message["mt_attrname"] = "datacenter__uuid"
    flag,msg = module_object.get_with_mtinfo()
    if not flag or not msg:
        return {}
    dcuuid = msg['mt_attr_value']
    
    module_object = dbmodule.db_module_interface.DbMessageObject(db_name='hosts',ip_d=vcip)
    field1 = {'datacenter':{'db_name':'datacenters','field':{'uuid':dcuuid}}}
    module_object.message['field1'] = field1
    flag,msg = module_object.select_by_fkey()
    if not flag or not msg:
        return {}
    
    for x in msg:
        hostuuid = x['uuid']
        ips[hostuuid] = []
        
        field1 = {'host':{'db_name':'hosts','field':{'uuid':hostuuid}}}
        collect_ips('service_console',field1,ips[hostuuid],vcip)
        
        field1 = {'service_console':{'db_name':'service_console','field':{'host':{'db_name':'hosts','field':{'uuid':hostuuid}}}}}
        collect_ips('multi_ips',field1,ips[hostuuid],vcip)
                
        field1 = {'service_console':{'db_name':'service_console','field':{'host':{'db_name':'hosts','field':{'uuid':hostuuid}}}}}
        collect_ips('vlan',field1,ips[hostuuid],vcip)
                
        field1 = {'host':{'db_name':'hosts','field':{'uuid':hostuuid}}}
        collect_ips('netcard',field1,ips[hostuuid],vcip)
                
        field1 = {'netcard':{'db_name':'netcard','field':{'host':{'db_name':'hosts','field':{'uuid':hostuuid}}}}}
        collect_ips('multi_ips',field1,ips[hostuuid],vcip)
                
        field1 = {'service_console':{'db_name':'service_console','field':{'host':{'db_name':'hosts','field':{'uuid':hostuuid}}}}}
        collect_ips('vlan',field1,ips[hostuuid],vcip)
                
    return ips

def get_host_ips():
    
    hostuuid = support.uuid_op.get_vs_uuid()[1]
    host_ips = []
    
    module_object = dbmodule.db_module_interface.DbMessageObject(db_name='service_console')
    field1 = {'host':{'db_name':'hosts','field':{'uuid':hostuuid}}}
    module_object.message['field1'] = field1
    flag,msg = module_object.select_by_fkey()
    if flag and msg:
        for y in msg:
            host_ips.append(y['ip'])
            
    module_object = dbmodule.db_module_interface.DbMessageObject(db_name='multi_ips')
    field1 = {'service_console':{'db_name':'service_console','field':{'host':{'db_name':'hosts','field':{'uuid':hostuuid}}}}}
    module_object.message['field1'] = field1
    flag,msg = module_object.select_by_fkey()
    if flag and msg:
        for y in msg:
            host_ips.append(y['ip'])
    
    module_object = dbmodule.db_module_interface.DbMessageObject(db_name='vlan')
    field1 = {'service_console':{'db_name':'service_console','field':{'host':{'db_name':'hosts','field':{'uuid':hostuuid}}}}}
    module_object.message['field1'] = field1
    flag,msg = module_object.select_by_fkey()
    if flag and msg:
        for y in msg:
            host_ips.append(y['ip'])
            
            
    module_object = dbmodule.db_module_interface.DbMessageObject(db_name='netcard')
    field1 = {'host':{'db_name':'hosts','field':{'uuid':hostuuid}}}
    module_object.message['field1'] = field1
    flag,msg = module_object.select_by_fkey()
    if flag and msg:
        for y in msg:
            host_ips.append(y['ip'])
            
        
    module_object = dbmodule.db_module_interface.DbMessageObject(db_name='multi_ips')
    field1 = {'netcard':{'db_name':'netcard','field':{'host':{'db_name':'hosts','field':{'uuid':hostuuid}}}}}
    module_object.message['field1'] = field1
    flag,msg = module_object.select_by_fkey()
    if flag and msg:
        for y in msg:
            host_ips.append(y['ip'])
            
    module_object = dbmodule.db_module_interface.DbMessageObject(db_name='vlan')
    field1 = {'netcard':{'db_name':'netcard','field':{'host':{'db_name':'hosts','field':{'uuid':hostuuid}}}}}
    module_object.message['field1'] = field1
    flag,msg = module_object.select_by_fkey()
    if flag and msg:
        for y in msg:
            host_ips.append(y['ip'])
            
    return host_ips

def host_in_gluster(host_ip):

    ips = get_datacenter_ips()
    for hostuuid in ips:
        host_ips = ips[hostuuid]
        for target_ip in host_ips:
            if target_ip == host_ip:
                return True
                
    host_ips =  get_host_ips()
    for target_ip in host_ips:
        if target_ip == host_ip:
            return True
        
    return False

def delete_peer(hostuuid):
    
    # 主机推出数据中心时执行，只在vcenter端执行
    # 包含brick的volume的所有brick是否在该host上
    
    hostobj = db_get('hosts',{'uuid':hostuuid})
    if not hostobj:
        return True,''
    brickobjs = db_values('glusterbrick',{'host_id':hostobj.get('id')})
    for brick in brickobjs:
        volumeobj = xattr('glustervolume',brick,'volume_id')
        xbricks = db_values('glusterbrick',{'volume_id':volumeobj['id']})
        for xbrick in xbricks:
            if xattr('hosts',xbrick,'host_id').get('uuid') != hostuuid:
                return False,volumeobj['description']
            
    return True,''


def delete_volume_peer(hostuuid):
    
   
    hostobj = db_get('hosts',{'uuid':hostuuid})
    if not hostobj:
        return True,''
    brickobjs = db_values('glusterbrick',{'host_id':hostobj.get('id')})
    for brick in brickobjs:
        volumeobj = xattr('glustervolume',brick,'volume_id')
        flag = True
        xbricks = db_values('glusterbrick',{'volume_id':volumeobj['id']})
        for xbrick in xbricks:
            if xattr('hosts',xbrick,'host_id').get('uuid') != hostuuid:
                flag = False
        if flag:
            db_delete('glustervolume',{'id':volumeobj[id]})
            
    return True,''

def delete_peer_in_vs():

    vsuuid = support.uuid_op.get_vs_uuid()[1]
    is_vcuuid,vcuuid,vcip=support.uuid_op.get_vc_uuid()
    if is_vcuuid and vcuuid!="127.0.0.1":
        hostinfo = db_get_vc('hosts',{'uuid':vsuuid},vcip)
        if hostinfo and hostinfo['host_state'] == 'available':
            brickinfos = db_values_vc('glusterbrick',{'host_id':hostinfo.get('id')},vcip)
            if len(brickinfos) != 0:
                for brick in brickinfos:
                    volume = xattrc('glustervolume',brick,'volume_id',vcip)
                    xbricks = db_values_vc('glusterbrick',{'volume_id':volume['id']},vcip)
                    for xbrick in xbricks:
                        if xattrc('hosts',xbrick,'host_id',vcip).get('uuid') != vsuuid:
                            return False,volume.get('description')
    return True,''

def get_replace_peer_infos(host_old,host_new,storage_paths):
    
    param_infos = []
    module_object = dbmodule.db_module_interface.DbMessageObject(db_name='glusterbrick')
    module_object.message['field1'] = {'host':{'db_name':'hosts','field':{'uuid':host_old}}}
    flag,msg = module_object.select_by_fkey()
    if not flag or not msg or len(msg)>len(storage_paths):
        return False,[]
    
    for i,brick in enumerate(msg):
        storage_path = storage_paths[i]['storage_path']
        description = storage_paths[i]['description']
        brick_uuid = brick['uuid']
        volume_id = brick['volume_id']
        module_object = dbmodule.db_module_interface.DbMessageObject(db_name='glustervolume')
        module_object.message['field1'] = {'id':volume_id}
        flag,msg = module_object.select()
        if not flag or not msg:
            return []
        volume_uuid = msg[0]['uuid']
        new_brick = {'hostUuid':host_new,'storage_path':storage_path,'description':description}
        param_infos.append({'volume_uuid':volume_uuid,'brick_uuid':brick_uuid,'new_brick':new_brick})     
    return True,param_infos

def get_available_peer_target_ip(dcuuid,host_ip,vc_uuid=None,vc_ip=None):
    
    #只在vcneter端执行
    
    
    module_object = dbmodule.db_module_interface.DbMessageObject(db_name='hosts')
    if vc_ip:
        module_object.ip_d = vc_ip
    
    module_object.message['field1'] = {'datacenter':{'db_name':'datacenters','field':{'uuid':dcuuid}}}
    flag,msg = module_object.select_by_fkey()
    if not flag:
        return False,''
    if not msg:
        return True,''
    for x in msg:
        if x['host_ip'] == host_ip:
            continue
        target_ip = x['host_ip']
        (flag, psh) = vmd_utils.get_rpcConnection(target_ip)
        if not flag:
            continue
        output = system.network.sent_rpc.sent_rpc_getret(psh, "do_web_get_vserver_baseinfo")
        if ("flag" not in output) or (not output["flag"]):
            continue
        if "yes" == output["param"]["vCenter"]:
            return True,target_ip
    return False,''

def get_host_dcuuid():
    vsuuid = support.uuid_op.get_vs_uuid()[1]
    is_vcuuid,vcuuid,vc_ip=support.uuid_op.get_vc_uuid()
    if is_vcuuid and vcuuid!="127.0.0.1":
        module_object = dbmodule.db_module_interface.DbMessageObject(db_name='hosts',ip_d=vc_ip)
        module_object.message['field1'] = {'uuid':vsuuid}
        module_object.message["mt_attrname"] = "datacenter__uuid"
        flag,msg = module_object.get_with_mtinfo()
        if not flag or not msg:
            return ''
        return msg["mt_attr_value"]
    return ''

def get_host_gluster_ip():
    
    vsuuid = support.uuid_op.get_vs_uuid()[1]
    is_vcuuid,vcuuid,vc_ip=support.uuid_op.get_vc_uuid()
    if is_vcuuid and vcuuid!="127.0.0.1":
        module_object = dbmodule.db_module_interface.DbMessageObject(db_name='glusterpeer',ip_d=vc_ip)
        module_object.message['field1'] = {'host':{'db_name':'hosts','field':{'uuid':vsuuid}}}
        flag,msg = module_object.select_by_fkey()
        if not flag or not msg:
            return ''
        return msg[0]['glusterip']
    return ''
    
def get_ip_seg(ip, netmask):
    """
     根据ip和netmask计算ip网段
    """
    return ".".join(map(lambda x,y:str(int(x)&int(y)),ip.split("."), netmask.split(".")))
        
def check_glfs_ip(hostip,glusterip,dcuuid):
    # 在vcenter端执行
    
    dcobj = db_get('datacenters',{'uuid':dcuuid})
    if not dcobj:
        return True,''
    hobjs = db_values('hosts',{'datacenter_id':dcobj['id']})
    if not hobjs:
        return True,''
    
    try:
        (flag, psh) = vmd_utils.get_rpcConnection(hostip)
        if not flag:
            return False,'vserver rpc failed'
        (netmask) = psh.do_web_get_netmask({"host_ip":glusterip})
        if not netmask:
            return False,'get glusterip netmask failed'
    except:
        return False,'get glusterip netmask failed'
            
    for hobj in hobjs:
        if hobj['host_ip'] == hostip:
            continue
        
        pobj = db_get('glusterpeer',{'host_id':hobj['id']})
        if not pobj:
            continue
        glusterip1 = pobj['glusterip']
        
        netmask1 = operation.vcluster.cluster_db_op.get_netmask_by_ip(glusterip1)
        if netmask1:
            if get_ip_seg(glusterip1, netmask1) != get_ip_seg(glusterip, netmask):
                return False,'glusterfs network segment error'
            
    return True,''


