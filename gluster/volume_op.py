# -*- coding: utf-8 -*-

import os

import time

import optevent_db_op
import operation.gluster.volume_db
import operation.gluster.volume_crt
import operation.gluster.volume_del
import operation.gluster.volume_add
import operation.gluster.volume_rpl
import operation.gluster.volume_rmv


def create_volume_op(event):
    
    # 检查主机的数据库状态
    # 检查主机的gluster p status 状态
    data = {}
    operation.gluster.volume_crt.data_crt(data,event.param)
    
    flag,msg = operation.gluster.volume_crt.cek_crt(data)
    if not flag:
        return False,msg
    
    flag,msg = operation.gluster.volume_db.get_new_bricks(data)
    if not flag:
        return False,msg
    
    flag,msg = operation.gluster.volume_crt.pre_crt(data)
    if not flag:
        return False,msg

    flag,msg = operation.gluster.volume_crt.cmd_crt(data)
    if not flag:
        return False,msg 
        
    flag,msg = operation.gluster.volume_crt.data_db_crt(data)
    if not flag:
        return False,msg
    
    flag,msg = operation.gluster.volume_db.insert_gluster_volume(data)
    if not flag:
        return False,msg
    
    flag,msg  = operation.gluster.volume_db.insert_gluster_bricks(data)
    if not flag:
        return False,msg
    
    return True,''

def delete_volume_op(event):

    # 检查主机状态 检查存储状态 检查客户端是否有挂载
    # 数据库删除 命令行删除 仿照ft使用rpc清除存储目录上的残留信息
        
    data = {}
    flag,msg = operation.gluster.volume_del.data_del(data, event.param)
    if not flag:
        return False,msg
    
    flag,msg = operation.gluster.volume_del.cek_del(data)
    if not flag:
        return False,msg
    
    flag,msg = operation.gluster.volume_del.pre_del(data)
    if not flag:
        return False,msg
    
    flag,msg = operation.gluster.volume_del.cmd_del(data)
    if not flag:
        return False,msg
    
    flag,msg = operation.gluster.volume_db.delete_gluster_bricks(data)
    if not flag:
        return False,msg
    
    flag,msg = operation.gluster.volume_db.delete_gluster_volume(data)
    if not flag:
        return False,msg
    
    return True,''

def add_brick_op(event):

    # 容量应该大于 已占空间的二分之一
    
    data = {}
    flag,msg = operation.gluster.volume_add.data_add(data, event.param)
    if not flag:
        return False,msg
    
    flag,msg = operation.gluster.volume_add.pre_add(data)
    if not flag:
        return False,msg
    
    flag,msg = operation.gluster.volume_add.cmd_add(data)
    if not flag:
        return False,msg
    
    flag,msg = operation.gluster.volume_db.update_gluster_volume(data)
    if not flag:
        return False,msg
    
    flag,msg  = operation.gluster.volume_db.insert_gluster_bricks(data)
    if not flag:
        return False,msg
    
    return True,''

def remove_brick_op(event):
   
    # 剩余brick的空间应大于所有被使用的空间
    data = {}
    flag,msg = operation.gluster.volume_rmv.data_rmv(data, event.param)
    if not flag:
        return False,msg
    
    flag,msg = operation.gluster.volume_rmv.pre_rmv(data)
    if not flag:
        return False,msg
    
    flag,msg = operation.gluster.volume_rmv.cmd_rmv(data)
    if not flag:
        return False,msg
    
    flag,msg = operation.gluster.volume_db.update_gluster_volume(data)
    if not flag:
        return False,msg
        
    flag,msg = operation.gluster.volume_db.delete_gluster_bricks(data)
    if not flag:
        return False,+msg
   
    return True,''

def replace_brick_op(event):
     
    data = {}
    flag,msg = operation.gluster.volume_rpl.data_rpl(data,event.param)
    if not flag:
        return False,msg
    
    flag,msg = operation.gluster.volume_rpl.pre_rpl(data)
    if not flag:
        return False,msg
    
    
    flag,msg = operation.gluster.volume_rpl.cmd_rpl(data)
    if not flag:
        return False,msg
    
    flag,msg = operation.gluster.volume_db.update_gluster_brick(data)
    if not flag:
        return False,msg
    
    flag,msg = operation.gluster.volume_rpl.heal_rpl(data)
    if not flag:
        return False,msg
    
    return True,''

def create_volume(event):
    
    eventexestat = "successed"
    flag,message=create_volume_op(event)
    operation.gluster.volume_clr.clear_resources()
    if not flag:
        eventexestat = "failed"

    updateinfo = {"uuid":event.uuid,"eventexestat":eventexestat,"progress":100, "message":message}
    optevent_db_op.update_optevent(updateinfo,event)
    return (True, "suc")

def delete_volume(event):
    
    eventexestat = "successed"
    flag,message=delete_volume_op(event)
    operation.gluster.volume_clr.clear_resources()
    if not flag:
        eventexestat = "failed"

    updateinfo = {"uuid":event.uuid,"eventexestat":eventexestat,"progress":100, "message":message}
    optevent_db_op.update_optevent(updateinfo,event)
    return (True, "suc")

def add_brick(event):
    
    eventexestat = "successed"
    flag,message=add_brick_op(event)
    operation.gluster.volume_clr.clear_resources()
    if not flag:
        eventexestat = "failed"

    updateinfo = {"uuid":event.uuid,"eventexestat":eventexestat,"progress":100, "message":message}
    optevent_db_op.update_optevent(updateinfo,event)
    return (True, "suc")

def remove_brick(event):

    eventexestat = "successed"
    flag,message=remove_brick_op(event)
    operation.gluster.volume_clr.clear_resources()
    if not flag:
        eventexestat = "failed"

    updateinfo = {"uuid":event.uuid,"eventexestat":eventexestat,"progress":100, "message":message}
    optevent_db_op.update_optevent(updateinfo,event)
    return (True, "suc")

def replace_brick(event):

    eventexestat = "successed"
    flag,message=replace_brick_op(event)
    operation.gluster.volume_clr.clear_resources()
    if not flag:
        eventexestat = "failed"

    updateinfo = {"uuid":event.uuid,"eventexestat":eventexestat,"progress":100, "message":message}
    optevent_db_op.update_optevent(updateinfo,event)
    return (True, "suc")

def start_volume(event):
    return (True, "suc")

def stop_volume(event):
    return (True, "suc")

def rebalance_brick(event):
    return (True, "suc")
