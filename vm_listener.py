# -*- coding: utf-8 -*-
"""
# Copyright (c) 2011, www.fronware.com
# All rights reserved.
#
# Filename: vm_listener
# Note: 监听虚拟机的服务
#
# Author: chenjianfei
# Modify time: 2011-12-29
#
#
"""
import time
import syslog

import socket
import struct
import traceback

import support.log.log
from support.exception.vm_listener_exception import VmStopException
import new_subthread
import vm_listener_object
import vm_listener_middleware
import operation.vnetwork.vswitch_cmd_op
import operation.vm.vm_running_script
import operation.vm.cgroup_conf
import operation.vm.vm_params
import global_params
import do_operation
from dbmodule.db_op import db_get_vc

import operation.vhost.balloon
import support.message.message
import support.message.vmd_message_queue
import support.uuid_op
import operation.vcluster.cluster_interface
import operation.vm.vms_op
import operation.vm.vm_hd_hot
import operation.vm.conf_xml
import operation.vstorage.storage_cmd_op
import operation.vhost.hosts_db_op
import system.network.dns_service_op
import vmd_utils

HOST = "127.0.0.1"

HANDLE_FUNS = {
vm_listener_object.FRONVIO_PORT_OPENVSWITCH_HOST:vm_listener_middleware.vswitch_middleware,
vm_listener_object.FRONVIO_PORT_HOST:vm_listener_middleware.host_manager,
vm_listener_object.FRONVIO_PORT_FRONVARETOOL_HOST:vm_listener_middleware.fronware_tool_middleware,
}


def need_init_tools(uuid):
    
    init_tools_status_desc = "init_tools_status_" + uuid
    if do_operation.check_subthread(init_tools_status_desc):
        # 避免正在初始化过程的时候，反复初始化。
        return False
    
    if global_params.vms_process_info.get(uuid) and global_params.vms_process_info.get(uuid).get("tools_start") == "yes":
        # 避免初始化完成之后，反复初始化。
        return False
    
    return True

def get_vm_in_single(vmuuid, all_single_vms):

    for vminfo in all_single_vms:
        if vminfo["uuid"] == vmuuid:
            return (True, vminfo)
    return (False, "")

def get_vm_in_cfg(vmuuid, storage_path, hostip=None):

    # 先尝试：从配置文件读取
    (flag, state) = operation.vstorage.storage_cmd_op.storage_path_writeable(storage_path)
    if flag:
        vm_op = operation.vm.conf_xml.vm_operation(vmuuid, storage_path)
        (flag, allpara) = vm_op.get_vm_all_para()
        if flag and allpara:
            return (True, allpara)
            # 注销从数据库获取部分，只有存储可用的时候才能初始化vmcache，以保证初始化vmcache的时候，actionlimit值可正常获取，
#             # 后尝试：从vC数据库读取
#             if hostip:
#                 (flag, vminfo) = operation.vm.vms_op.get_vc_vminfo(vmuuid, hostip)
#                 if flag:
#                     return (True, vminfo)
    return (False, "")

def init_notrunning_single_vm_message_to_web(all_single_vms, already_init_and_send_uuids):

    again = True
    while again:
        again = False
        # 所有停止的单机，集群虚拟机：缓存对象初始化，以及发送刷新消息到WEB
        # vmstate = "stopped/saved" ，如果初始化为saved，则从此处获取，传递给函数init_vm_message_to_web
        # 未运行的单机虚拟机，
        for vminfo in all_single_vms:
            if vminfo["uuid"] in already_init_and_send_uuids:
                continue
            uuid = vminfo["uuid"]
            storage_path = vminfo["storage_path"]
            (flag, vminfo) = get_vm_in_cfg(uuid, storage_path)
            if not flag:
                again = True
                continue
            already_init_and_send_uuids.append(uuid)
            
            vmstate = "stopped"
            if vminfo["state"] == "saved":
                vmstate = "saved"
            vmType = vminfo["vms_type"]
            label = vminfo["description"]
            parentUuid = vminfo.get("vms_refrerence_uuid") or vminfo.get("belong") # 配置文件中获取则为belong，数据库则为vms_refrerence_uuid        
            vncPassword = None
            spicePassword = None
            vnc_port = 0
            istemplate = vminfo["modvm"]
            init_vm_message_to_web(uuid, storage_path, vmstate, vmType, label, parentUuid, vncPassword, spicePassword, vnc_port, istemplate)
        if again:
            # 未初始化完成的对象，间隔15秒再尝试一次
            time.sleep(15)

def init_notrunning_cluster_vm_message_to_web(all_cluster_vms, already_init_and_send_uuids):

    if not all_cluster_vms:
        return
    while True:
        if not operation.vcluster.cluster_interface.cluster_is_available():
            time.sleep(15)
            continue
        break
    
    again = True
    while again:
        again = False
        is_vcuuid,vcuuid,hostip = support.uuid_op.get_vc_uuid()
        for x in all_cluster_vms:
            if x["uuid"] in already_init_and_send_uuids:
                continue
            if not hostip:
                is_vcuuid,vcuuid,hostip = support.uuid_op.get_vc_uuid()
            uuid = x["uuid"]
            (flag, vminfo) = get_vm_in_cfg(uuid, x["storage_path"], hostip)
            if not flag:
                again = True
                continue
            already_init_and_send_uuids.append(uuid)
            
            storage_path = vminfo["storage_path"]
            vmstate = "stopped"
            if vminfo["state"] == "saved":
                vmstate = "saved"
            vmType = vminfo["vms_type"]
            label = vminfo["description"]
            parentUuid = vminfo.get("vms_refrerence_uuid") or vminfo.get("belong") # 配置文件中获取则为belong，数据库则为vms_refrerence_uuid        
            vncPassword = None
            spicePassword = None
            vnc_port = 0
            istemplate = vminfo["modvm"]
            init_vm_message_to_web(uuid, storage_path, vmstate, vmType, label, parentUuid, vncPassword, spicePassword, vnc_port, istemplate)
        if again:
            # 未初始化完成的对象，间隔15秒再尝试一次
            time.sleep(15)

def init_vm_listener():
    
    """
     开机初始化监听器。查找虚拟机进程，截取需要信息，开启监听器。
     在没有任何虚拟机监听器的时候调用，否则监听冲突 
    """
    vs_uuid = support.uuid_op.get_vs_uuid()[1]
    is_vcuuid,vcuuid,hostip = support.uuid_op.get_vc_uuid()

    # 从本地数据库，获取所有单机虚拟机列表，供刷新使用
    (flag, all_single_vms) = operation.vm.vms_op.get_vms(vs_uuid)
    # 从集群配置文件，获取所有集群虚拟机列表，供刷新使用
    all_cluster_vms = operation.vcluster.cluster_interface.get_all_cluster_vms()
    already_init_and_send_uuids = []
    
    #vCenterHA 虚拟机从vCenter数据库中获取
    cluster_uuid = operation.vcluster.cluster_interface.check_get_cluster_name()
    (clflag, clallvm) = operation.vm.vms_op.get_vc_vms(cluster_uuid)
    all_cluster_vms.extend(clallvm)
    
    vm_infos = operation.vm.vm_running_script.get_all_running_vm_by_vmprocess()
    for vmuuid, vnc_port, vncpassword,spicepassword,vmprocess in vm_infos:
        if not vncpassword:
            vncpassword = None
        if not spicepassword:
            spicepassword = None
        
        (flag, vminfo) = get_vm_in_single(vmuuid, all_single_vms)
        if flag:
            already_init_and_send_uuids.append(vmuuid)
        else:
            notexist = True
            for clx in all_cluster_vms:
                if clx["uuid"] == vmuuid:
                    (flag, vminfo) = get_vm_in_cfg(vmuuid, clx["storage_path"], hostip)
                    if flag:
                        notexist = False
                        already_init_and_send_uuids.append(vmuuid)
                    break
            if notexist:
                # 单机，集群:读取不到虚拟机信息，则杀掉此虚拟机。
                operation.vm.vm_running_script.kill_vm_process(vmuuid)
                continue
        vms_type = vminfo["vms_type"]
        storage_path = vminfo["storage_path"]
        usehugepages = vminfo["usehugepages"]
        reserved_mem, level = int(vminfo["reserved_mem"]), int(vminfo["level"])
        vmvhdstat = vminfo["vmvhdstat"]
        system_type = vminfo["system_type"]
        description = vminfo["description"]
        vms_refrerence_uuid = vminfo.get("vms_refrerence_uuid") or vminfo.get("belong") # 配置文件中获取则为belong，数据库则为vms_refrerence_uuid
        serial_ports = int(vminfo["serial_ports"])
        mem = int(vmprocess.split(" -m ")[1].strip().split()[0])
        # 此全局变量初始化的键，键值，必须与虚拟机启动初始化全局变量值一致
        global_params.vms_process_info[vmuuid] = {"startTime":time.time(), "mem":mem,
                                                  "tools_start":"no", "tools_ready":"no", "serial2_start":"no",
                                                  "vms_type":vms_type, "vmvhdstat":vmvhdstat,
                                                  "reserved_mem":reserved_mem, "level":level,
                                                  "usehugepages":usehugepages,
                                                  "description":vmprocess.split(" -name ")[1].strip().split()[0],
                                                  "vncpassword":vncpassword,"spicepassword":spicepassword,
                                                  "storage_path":storage_path,
                                                  "vnc_port":int(vnc_port),}

        if vms_type == "single":
            global_params.vms_process_info[vmuuid]["running_on"] = vs_uuid
        global_params.vms_process_info[vmuuid]['vms_type'] = vms_type

        balloon_mem = operation.vhost.balloon.get_process_balloon_actual(vmuuid)
        if balloon_mem:
            global_params.vms_process_info[vmuuid]["balloon_mem"] = balloon_mem
        
        global_params.vms_cache[vmuuid] = {"mem":mem, "usehugepages":usehugepages,}
        
        # 所有运行的单机，集群虚拟机：缓存对象初始化，以及发送刷新消息到WEB
        vmType = vms_type
        label = description
        parentUuid = vms_refrerence_uuid
        istemplate = "no"
        do_start_vm_port_listener(vmuuid, "serial1", "yes", storage_path, vmType, label, parentUuid, vncpassword, spicepassword, vnc_port, istemplate)
        if system_type == "firewall" and serial_ports >= 2:
            do_serial2_listener(vmuuid, "serial2", "reinit")

    # 未运行的单机虚拟机，启用线程处理，因为可能此时存储无法访问，无法初始化
    new_subthread.addtosubthread("init_notrunning_single_vm_message_to_web", init_notrunning_single_vm_message_to_web, all_single_vms, already_init_and_send_uuids)

    # 未运行的集群虚拟机，启用线程处理，因为集群可能此时存储和vC都无法访问，无法初始化，必须等存储或vC其中至少一方可访问时才可以初始化停止的集群虚拟机状态
    new_subthread.addtosubthread("init_notrunning_cluster_vm_message_to_web", init_notrunning_cluster_vm_message_to_web, all_cluster_vms, already_init_and_send_uuids)

def update_vCenterHAvm_to_global():
    # 如果是vCenterHA 的集群虚拟机，在vCenter 后起的情况下，全局变量中就没有集群虚拟机信息
    # 此函数在vCenter启动后发1800时调用
    
    cluster_uuid = operation.vcluster.cluster_interface.check_get_cluster_name()
    is_vcuuid,vcuuid,vc_ip = support.uuid_op.get_vc_uuid()
    if is_vcuuid and vcuuid!="127.0.0.1":
        (flag, clusterinfo) = operation.vcluster.cluster_op.get_vc_clusterinfo(cluster_uuid)
        if (not flag) or (not clusterinfo):
            return
        if clusterinfo['cluster_type']!='vCenterHa':
            return
    (clflag, clallvm) = operation.vm.vms_op.get_vc_vms(cluster_uuid)
    for vminfo in clallvm:
        uuid = vminfo['uuid']
        if global_params.vm_in_vmsObj(uuid):
            continue
        storage_path = vminfo['storage_path']
        allpara = None
        (flag, state) = operation.vstorage.storage_cmd_op.storage_path_writeable(storage_path)
        if flag:
            vm_op = operation.vm.conf_xml.vm_operation(uuid, storage_path)
            (flag, allpara) = vm_op.get_vm_all_para()
            if (not flag) or (not allpara):
                allpara = None
        actionLimit, shadowToSSD = operation.vm.vm_params.get_vm_permission(uuid, storage_path, allpara)
        UserDataDisk = operation.vm.vm_hd_hot.get_vm_userdatadisk_cache(uuid, allpara)
        isFT = 'no'
        if operation.vcluster.cluster_interface.clustervm_ft_is_enable(uuid):
            isFT = "yes"
        vmobj = {
 "vmType":'cluster',
 "uuid":vminfo['uuid'],
 "label":vminfo['description'],
 "state":'stopped',
 "parentUuid":cluster_uuid,
 "parentType":'cluster',
 "actionLimit":actionLimit,
 "vncPassword":None,
 "spicePassword":None,
 "shadowToSSD":shadowToSSD,
 "vnc_port":0,
 "ft_position":None,
 "running_on":None,
 "UserDataDisk":UserDataDisk,
 "tools":{},
 "rdp":{},
 "isFT":isFT,
 "objtype":'clusterVm',
        }
        global_params.append_to_vmsObj(vmobj)

def reinit_tools(uuid):
    
    # 虚拟机迁移到目标主机，FT的备变成主，
    # 虚拟机挂起，虚拟机电源保护关闭，这两种情况再启动虚拟机，
    # tools状态未正常初始化的情况下。
    # 10秒超时，在20秒一次发送虚拟机系统性能消息区间内。
    num = 0
    while num < 5:
        num += 1
        time.sleep(2)
        
        flag,_ = operation.vm.vm_running_script.check_kvm_process_exist(uuid)
        if not flag:
            # 检测虚拟机进程是否运行，进程未运行则直接return，退出处理
            syslog.syslog(syslog.LOG_ERR, "VM process not running, Tools noneed to reinit:" + uuid)
            return
        
        init_tools_status_desc = "init_tools_status_" + uuid
        if do_operation.check_subthread(init_tools_status_desc):
            # TOOLS初始化线程正在running，则直接return,退出处理
            syslog.syslog(syslog.LOG_ERR, "initThread is running, Tools noneed to reinit:" + uuid)
            return
        
        if global_params.vms_process_info.get(uuid) and global_params.vms_process_info.get(uuid).get("tools_start") == "yes":
            # TOOLS状态显示已经初始化完成，则直接return，退出处理
            syslog.syslog(syslog.LOG_ERR, "initFinished, Tools noneed to reinit:" + uuid)
            return

    # 用于触发重启tools，重新初始化汇报rdp状态
    # {"mission_id":"xxx", "mission_state":"queued", "progress":"0", "tag":"restart_tools"}
    content = {"tag":"restart_tools",}
    vm_listener_middleware.tools_request(uuid, content)
    syslog.syslog(syslog.LOG_ERR, "Tools reinit and restart:" + uuid)

def tools_listener_thread_isrunning(uuid):

    strs = "do_tools_listener" + uuid
    num = 0
    while num < 90:
        num = num + 1
        flag = do_operation.check_subthread(strs)
        if not flag:
            return False
        syslog.syslog(syslog.LOG_ERR, "VM tools thread is running:%s:%s" % (uuid, str(num)))
        time.sleep(1)
    return True
    
def do_start_vm_port_listener(uuid, port_name, rightnow, storage_path, vmType, label, parentUuid, vncPassword, spicePassword, vnc_port, istemplate):
    
    # port_name is serial1
    """
      为指定虚拟机开启监听服务线程
    """

    strs = "do_tools_listener" + uuid
    if not do_operation.check_subthread(strs):
        new_subthread.addtosubthread(strs, vm_listener, uuid, storage_path, port_name, vmType, label, parentUuid, vncPassword, spicePassword, vnc_port, istemplate)

    return True

def initSerial2connect(uuid, reinit):
    
    if reinit:
        # 重启vmd时立即发起一个,等待监听线程开启。
        time.sleep(10)
        # 必须使用这种无返回请求，否则请求会被状态卡住，造成无法发出。
        content = {"tag":"get_system_info", "port_name":"serial2"}
        vm_listener_middleware.tools_request(uuid, content)
        
        time.sleep(10) # 等待结果返回，再进行状态初始化
        if global_params.vms_process_info.get(uuid) and global_params.vms_process_info.get(uuid).get("serial2_ready") == "yes":
            global_params.vms_process_info.get(uuid)["serial2_start"] = "yes"
            
            content = {"tag":"get_system_info", "port_name":"serial2"}
            for response in vm_listener_middleware.tools_request_iter_responses(uuid, content):
                if response["mission_state"] == "successed":
                    # {u'mission_id': u'bONmcYjC-wnk2lA-p4sC', u'mission_state': u'successed', u'vFWversion': u'3.3.010.059.2_vFW12', u'progress': u'100'}
                    vFWversion = str(response["vFWversion"])
                    sys_version = vFWversion
                    sys_type = "vGate"                    
                    # 更新tools全局变量，发送消息，更新数据库
                    update_and_send_toosinfo(uuid, vFWversion, sys_version, sys_type, False)
            

def do_serial2_listener(uuid, port_name, reinit=None):
    
    # port_name is serial2
    
    """
      监听第二个串口，仅天融信防火墙用
    """
    strs = "do_serial2_listener" + uuid
    if not do_operation.check_subthread(strs):
        new_subthread.addtosubthread(strs, serial2_listener, uuid, port_name)
        new_subthread.addtosubthread("do_serial2_listener_initSerial2connect" + uuid, initSerial2connect, uuid, reinit)

    return True

def clean_vnet(allpara):
    if allpara:
        netcard_all_para = allpara["netcard_all_para"]
        vnetlist = map(lambda x:"vnet" + x["netcard_name"], netcard_all_para)
        operation.vnetwork.vswitch_cmd_op.vswitch_del_ports(vnetlist)
    
def _start_listener(uuid, port_name, s):
    # start listener will save port_name
    head_format = "BBBBI"
    head_size = struct.calcsize(head_format)
    first_msg = True
    while True:
        #接收消息头
        message_list = []
        need_length = head_size
        while need_length > 0:
            recv_length = need_length
            msg_result = s.recv(recv_length)
            if msg_result:
                if first_msg and "\xff" == msg_result:
                    # 全虚拟化串口，在虚拟机开机过程中会首发一个无意义的字符串"\xff"，需要屏蔽
                    # 第一个包过后，不再会有无意义字符串存在，故第一包之后均first_msg置为fale
                    continue
                first_msg = False
                #print "recv msg", repr(msg_result)
                #vm_listener_middleware.to_log(vm_info, "recv msg", repr(msg_result))
                message_list.append(msg_result)
                rel_length = len(msg_result)
                need_length -= rel_length
            else:
                raise VmStopException
        head_result = ''.join(message_list)

        if head_result:           
            # 表示tools/防火墙串口可以开始连接。
            if global_params.vms_process_info.get(uuid):
                global_params.vms_process_info.get(uuid)["tools_ready"] = "yes"
            
            #print "recv head", repr(head_result)
            #vm_listener_middleware.to_log(vm_info, "recv head", repr(head_result))
            try:
                version, agreement, dstport, srcport, length = \
                        struct.unpack(head_format, head_result)
                #print version,agreement,dstport,srcport,length
            except:
                vm_listener_middleware.to_log(uuid, "recv head analyze error", repr(head_result))
                continue
            
            #记录应用使用的通道
            vm_listener_object.APP_PORT.setdefault(uuid,{})[srcport] = port_name
            #更新虚拟机应用的上次接收消息时间
            vm_listener_middleware.update_guest_last_msg_time(srcport, uuid)
            
            #接收数据
            if length > 10240:
                vm_listener_middleware.to_log(uuid, "recv head length>10248", repr(head_result))
                continue
            message_list = []
            need_length = length - head_size
            while need_length > 0:
                if need_length > 4096:
                    recv_length = 4096
                else:
                    recv_length = need_length
                msg_result = s.recv(recv_length)
                if msg_result:
                    #print "recv msg", repr(msg_result)
                    #vm_listener_middleware.to_log(vm_info, "recv msg", repr(msg_result))
                    message_list.append(msg_result)
                    rel_length = len(msg_result)
                    need_length -= rel_length
                else:
                    raise VmStopException
                
            # recv version,agreement,dstport,srcport and then send back as head
            message = ''.join(message_list)  
            _to_log_recv_response_message(uuid, "recv", head_result, version, agreement, dstport, srcport, length, message)
            if dstport in HANDLE_FUNS:
                try:
                    handle_fun = HANDLE_FUNS[dstport]
                    for agreement, response in handle_fun(uuid, agreement, message):
                        length = len(response) + head_size
                        response_head = struct.pack(head_format, version, agreement,
                                                    srcport, dstport, length)
                        global_params.locklist["vm_tools_send_msg_lock"].acquire()
                        try:
                            s.send(response_head + response)
                        finally:
                            global_params.locklist["vm_tools_send_msg_lock"].release()
                        #print "response",repr(response_head + response)
                        #vm_listener_middleware.to_log(vm_info, "response", repr(response_head + response))
                        _to_log_recv_response_message(uuid, "response", response_head, version, agreement, srcport, dstport, length, response)
                except:
                    vm_listener_middleware.to_log(uuid, "handle error", traceback.format_exc()) 
                    continue
               
        else:
            raise VmStopException
        
def _to_log_recv_response_message(vm_info, msgtype, head, version, agreement, dstport, srcport, length, message):
    
    if agreement not in (vm_listener_object.FRONVIO_PROTO_KEEPALIVE, vm_listener_object.FRONVIO_PROTO_KEEPALIVE_ACK):
        log_message = ("head:%s\nversion:%s\nagreement:%s\ndstport:%s\nsrcport:%s\nlength:%s\nmessage:%s\n"
                        % (repr(head), version, agreement, dstport, srcport, length, message))
        vm_listener_middleware.to_log(vm_info, msgtype, log_message)   

def _serial2_start_listener(uuid, s):
    
    head_format = "BBBBI"
    head_size = struct.calcsize(head_format)
    first_msg = True
    while True:
        #接收消息头
        message_list = []
        need_length = head_size
        while need_length > 0:
            recv_length = need_length
            msg_result = s.recv(recv_length)
            if msg_result:
                if first_msg and "\xff" == msg_result:
                    # 全虚拟化串口，在虚拟机开机过程中会首发一个无意义的字符串"\xff"，需要屏蔽
                    # 第一个包过后，不再会有无意义字符串存在，故第一包之后均first_msg置为fale
                    continue
                first_msg = False
                message_list.append(msg_result)
                rel_length = len(msg_result)
                need_length -= rel_length
            else:
                raise VmStopException
        head_result = ''.join(message_list)

        if head_result:           
            # 表示tools/防火墙串口可以开始连接。
            if global_params.vms_process_info.get(uuid):
                global_params.vms_process_info.get(uuid)["tools_ready"] = "yes"
                global_params.vms_process_info.get(uuid)["serial2_ready"] = "yes"

        if head_result:           
            try:
                version, agreement, dstport, srcport, length = struct.unpack(head_format, head_result)
                #print version,agreement,dstport,srcport,length
            except:
                vm_listener_middleware.to_log(uuid, "Serial2:recv head analyze error", repr(head_result))
                continue
            
            #接收数据
            if length > 10240:
                vm_listener_middleware.to_log(uuid, "Serial2:recv head length>10248", repr(head_result))
                continue
            message_list = []
            need_length = length - head_size
            while need_length > 0:
                if need_length > 4096:
                    recv_length = 4096
                else:
                    recv_length = need_length
                msg_result = s.recv(recv_length)
                if msg_result:
                    message_list.append(msg_result)
                    rel_length = len(msg_result)
                    need_length -= rel_length
                else:
                    raise VmStopException
                
            message = ''.join(message_list)
            _to_log_recv_response_message(uuid, "Serial2:recv", head_result, version, agreement, dstport, srcport, length, message)
            if dstport in HANDLE_FUNS:
                try:
                    handle_fun = HANDLE_FUNS[dstport]
                    for agreement, response in handle_fun(uuid, agreement, message):
                        length = len(response) + head_size
                        response_head = struct.pack(head_format, version, agreement, srcport, dstport, length)
                        s.send(response_head + response)
                        _to_log_recv_response_message(uuid, "Serial2:response", response_head, version, agreement, srcport, dstport, length, response)
                except:
                    vm_listener_middleware.to_log(uuid, "Serial2:handle error", traceback.format_exc()) 
                    continue
        else:
            raise VmStopException

def serial2_listener(uuid, port_name):
    
    """
    监听第二个串口，仅天融信防火墙用
    """
    serial_file = "/var/run/%s/serial2.sock" % uuid
    if not uuid in vm_listener_object.VM_SOCKET:
        vm_listener_object.VM_SOCKET[uuid] = {}
    try:
        s = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
        s.connect(serial_file)
        s.settimeout(None)  # 设置成永不超时，否则会继承socket的默认超时，此默认值在rpc客户端创建时被初始化成了30秒
        # connect结束，不能立即recv，否则大多数情况都是timeout失败
        time.sleep(1)
        vm_listener_object.VM_SOCKET[uuid][port_name] = s
        try:
            _serial2_start_listener(uuid, s)
        finally:
            s.close()
    except:
        errstrs = "Serial2 Except:" + str(traceback.format_exc())
        syslog.syslog(syslog.LOG_ERR, errstrs)

def stopped_saved_clustervm_need_updatedb(uuid):
    
# 若为集群虚拟机，状态为stopped/saved，
#     读取数据库，
#     数据库若为running，则检测目标主机是否在running，是running，则不更新数据库
#     数据库若为stopped/saved，则不更新数据库，
    is_vcuuid,vcuuid,hostip = support.uuid_op.get_vc_uuid()
    if not hostip:
        syslog.syslog(syslog.LOG_ERR, uuid+" running_on,state,vnc_port will not be update:vc ip is not connect")
        return False
    
    (flag, vminfo) = operation.vm.vms_op.get_vc_vminfo(uuid, hostip)
    if not flag:
        syslog.syslog(syslog.LOG_ERR, uuid+" running_on,state,vnc_port will not be update:get vmfrom from vc failed")
        return False
    
    if vminfo.get("state") in ["stopped", "saved"]:
        syslog.syslog(syslog.LOG_ERR, uuid+" running_on,state,vnc_port will not be update:state is " + vminfo.get("state"))
        return False
    
    if vminfo.get("state") == "running":
        running_on = vminfo.get("running_on")
        if running_on:
            # e.g get target host form 
            (flag, hostform) = operation.vhost.hosts_db_op.get_ip_hostform(running_on,hostip)
            if flag and hostform:
                host_ip = hostform.get("host_ip")
                if host_ip:
                    (flag, _) = system.network.dns_service_op.host_ip_is_connect(host_ip)
                    if flag:
                        try:
                            (flag, psh) = vmd_utils.get_rpcConnection(host_ip)
                            (flag, targetVmobj) = psh.do_get_vmCache(uuid)
                            if flag and targetVmobj:
                                if targetVmobj["state"] == "running":
                                    syslog.syslog(syslog.LOG_ERR, uuid+" running_on,state,vnc_port will not be update:state is running in " + host_ip)
                                    return False
                                else:
                                    syslog.syslog(syslog.LOG_ERR, uuid+" running_on,state,vnc_port will be update:running_on host_ip is not running " + host_ip)
                        except:
                            pass
                    syslog.syslog(syslog.LOG_ERR, uuid+" running_on,state,vnc_port will be update:running_on host_ip is not connect " + host_ip)
                else:
                    syslog.syslog(syslog.LOG_ERR, uuid+" running_on,state,vnc_port will be update:get running_on hostform host_ip failed " + host_ip)
            else:
                syslog.syslog(syslog.LOG_ERR, uuid+" running_on,state,vnc_port will be update:get running_on hostform failed " + running_on)
        else:
            syslog.syslog(syslog.LOG_ERR, uuid+" running_on,state,vnc_port will be update:state is running but running_on is null")
    else:
        syslog.syslog(syslog.LOG_ERR, uuid+" running_on,state,vnc_port will be update:state is not running or stopped or saved")
    return True

def init_vm_message_to_web(uuid, storage_path, vmstate, vmType, label, parentUuid, vncPassword, spicePassword, vnc_port, istemplate):
    
    # vmd启动时，单机，集群虚拟机的缓存消息对象初始化
    # 可能存在运行，或停止的情况
    allpara = None
    (flag, state) = operation.vstorage.storage_cmd_op.storage_path_writeable(storage_path)
    if flag:
        vm_op = operation.vm.conf_xml.vm_operation(uuid, storage_path)
        (flag, allpara) = vm_op.get_vm_all_para()
        if (not flag) or (not allpara):
            allpara = None
    actionLimit, shadowToSSD = operation.vm.vm_params.get_vm_permission(uuid, storage_path, allpara)
    if allpara:
        # 虚拟机已经运行，且有数据盘，且vmd重启的情况，此时需要读取数据盘列表初始化，
        UserDataDisk = operation.vm.vm_hd_hot.get_vm_userdatadisk_cache(uuid, allpara)
#         diff_from = allpara["diff_from"]
        # 可读取配置文件时，检测可能存在的名字更新
        label = allpara["description"]
    else:
        UserDataDisk = ["init", ]
#         diff_from = None
    
    vs_uuid = support.uuid_op.get_vs_uuid()[1]
    isFT = "no"
    ft_position = None
    running_on = None
    if "single" == vmType:
        objtype = "singleVm"
        parentType = "VServer"
        if vmstate == "running":
            running_on = vs_uuid
    else:
        objtype = "clusterVm"
        parentType = "cluster"
        # 虚拟机缓存对象初始化，isFT值必须初始化
        if operation.vcluster.cluster_interface.clustervm_ft_is_enable(uuid):
            isFT = "yes"
        if vmstate == "running":
            if isFT != "yes":
                # 普通非FT虚拟机的运行主机
                ft_position = "primary"
                running_on = vs_uuid
            else:
                (flag, state) = operation.vm.vm_running_script.monitor_exec_base(uuid, "info status", 5)
                if flag and "running" in state:
                    # FT虚拟机的主
                    running_on = vs_uuid
                    ft_position = "primary"
                else:
                    # FT虚拟机的备
                    ft_position = "secondary"
        else:
            ft_position = "normal"
    
    if "yes" == istemplate:
        objtype = "modRootVm"
    if isFT == "yes":
        objtype = "clusterMirrorVm"
    
    vmobj = {
 "vmType":vmType,
 "uuid":uuid,
 "label":label,
 "state":vmstate,
 "parentUuid":parentUuid,
 "parentType":parentType,
 "actionLimit":actionLimit,
 "vncPassword":vncPassword,
 "spicePassword":spicePassword,
 "shadowToSSD":shadowToSSD,
 "vnc_port":vnc_port,
 "ft_position":ft_position,
 "running_on":running_on,
 "UserDataDisk":UserDataDisk,
 "tools":{},
 "rdp":{},
 "isFT":isFT,
 "objtype":objtype,
                  }
    
    if vmstate == "running":
        # 可能存在的某些虚拟机，进程刚启动，就丢失的问题，进行二次检验
        (flag, state) = operation.vm.vm_running_script.check_kvm_process_exist(uuid)
        if not flag:
            return (False, state)
    # 更新全局变量
    (flag, state) = global_params.append_to_vmsObj(vmobj)
    if not flag:
        return (False, state)
    # 更新数据库
    is_clustervm = False
    if "single" != vmType:
        is_clustervm = True
    try:
        update_db = True
        if is_clustervm and (vmstate == "stopped" or vmstate == "saved"):
            # 集群虚拟机，如果当前主机进程不存在，检测集群服务为运行，则不更新数据库，
            # 避免错误更新，造成vC的数据库错误，
            # 判定可能为stopped的类型：stopped stopping failed disabled uninitialized checking  此时，允许更新数据库为stopped
            # 判定可能为running的类型：enable started starting recoverable recovering migrating 此时，禁止更新数据库为stopped，且禁止发送更新消息给WEB，
            # 禁止发消息的类型，同样适用于1800同步时，此时也必须禁止，否则消息会错误，
            (flag, clvmstate,_) = operation.vm.check_vm_state.check_vm_state(uuid)
            if flag and clvmstate in ["enable", "started", "starting", "recoverable", "recovering", "migrating"]:
                return (True, allpara)
            if not flag:
                ## 集群状态异常时才借助vCenter的数据库和其他主机进行判断
                #if not stopped_saved_clustervm_need_updatedb(uuid):
                # 20150326集群状态异常，不更新数据库
                update_db = False
                
        if (vmstate == "running" and not running_on) or (not update_db):
            # FT 的备，消息照样发送，但不更新数据库
            pass
        else:
            operation.vm.vms_op.update_vm_state_for_loop({"vmuuid":uuid, "stat":vmstate, "vnc_port":vnc_port, "running_on":running_on}, is_clustervm)
    except:
        syslog.syslog(syslog.LOG_ERR,'update vm state failed: '+str(traceback.format_exc()))
    
    # 发送消息给WEB
    # 注意，None消息的处理: 可能为None/0但又必须发送的键为：vncPassword/spicePassword/vnc_port/ft_position/running_on/diff_from
    # 注释的部分，是在初始化过程中并不需要发送给WEB的属性键，但需要保存在缓存中，以备其他时候更新使用
    state_message = support.message.message.RefreshLeftTreeStateMessage(action = "update", 
                                                        label = label, 
                                                        uuid = uuid, 
                                                        state = vmstate,
                                                        #parentuuid = parentUuid,
                                                        objtype = objtype,
                                                        #parenttype = parentType,
                                                        actionLimit = actionLimit,
                                                        UserDataDisk = UserDataDisk,
                                                        vncpassword = vncPassword,
                                                        spicepassword = spicePassword,
                                                        #diff_from = diff_from,
                                                        shadowToSSD = shadowToSSD,
                                                        vnc_port = vnc_port,
                                                        ft_position = ft_position,
                                                        running_on = running_on,
                                                        )
    support.message.vmd_message_queue.put_statemsg_to_web(state_message)
    return (True, allpara)
    
def update_vm_state_message_to_web(uuid, storage_path, vmType, vmstate, vncPassword, spicePassword, vnc_port, noUpdateDb=None):
    
    allpara = None
    (flag, state) = operation.vstorage.storage_cmd_op.storage_path_writeable(storage_path)
    if flag:
        vm_op = operation.vm.conf_xml.vm_operation(uuid, storage_path)
        (flag, allpara) = vm_op.get_vm_all_para()
        if (not flag) or (not allpara):
            allpara = None
    actionLimit, shadowToSSD = operation.vm.vm_params.get_vm_permission(uuid, storage_path, allpara)
    if "stopped" == vmstate:
        # vmstate如果为stopped则需要判断可能为saved
        if allpara and "saved" == allpara["state"]:
            vmstate = "saved"
    label = None
    if allpara:
        # 可读取配置文件时，检测可能存在的名字更新
        label = allpara["description"]
    UserDataDisk = None
    if vmstate != "running":
        UserDataDisk = ["init", ]
    
    isFT = "no"
    ft_position = None
    running_on = None
    vs_uuid = support.uuid_op.get_vs_uuid()[1]
    if "single" == vmType:
        objtype = "singleVm"
        if vmstate == "running":
            running_on = vs_uuid
    else:
        objtype = "clusterVm"
        if operation.vcluster.cluster_interface.clustervm_ft_is_enable(uuid):
            isFT = "yes"
        if vmstate == "running":
            if isFT != "yes":
                # 普通非FT虚拟机的运行主机
                ft_position = "primary"
                running_on = vs_uuid
            else:
                (flag, state) = operation.vm.vm_running_script.monitor_exec_base(uuid, "info status", 5)
                if flag and "running" in state:
                    # FT虚拟机的主
                    running_on = vs_uuid
                    ft_position = "primary"
                else:
                    # FT虚拟机的备
                    ft_position = "secondary"
        else:
            ft_position = "normal"
    if isFT == "yes":
        objtype = "clusterMirrorVm"
    
    bsrunning_on = ""
    (flag, bsitemobj) = global_params.get_vmobj_for_send(uuid)
    if flag and bsitemobj:
        bsrunning_on = bsitemobj["running_on"]
    vmobj = {
 "uuid":uuid,
 "state":vmstate,
 "actionLimit":actionLimit,
 "vncPassword":vncPassword,
 "spicePassword":spicePassword,
 "shadowToSSD":shadowToSSD,
 "vnc_port":vnc_port,
 "ft_position":ft_position,
 "running_on":running_on,
 "isFT":isFT,
 "objtype":objtype,
                  }
    if label is not None:
        vmobj["label"] = label
    else:
        label = bsitemobj["label"]
    if vmstate != "running":
        # 只有由运行变成停止，才需要在此时同时更新数据盘，rdp信息
        vmobj["UserDataDisk"] = UserDataDisk
        vmobj["rdp"] = {}
    if vmstate == "running":
        # 可能存在的某些虚拟机，进程刚启动，就丢失的问题，进行二次检验
        (flag, state) = operation.vm.vm_running_script.check_kvm_process_exist(uuid)
        if not flag:
            return (False, state)
    # 更新全局变量
    (flag, state) = global_params.update_in_vmsObj(vmobj)
    if not flag:
        return (False, state)
    # 更新数据库
    is_clustervm = False
    if "single" != vmType:
        is_clustervm = True
    try:
        update_db = True
        if is_clustervm and vmstate == "stopped":
            # 判定可能为stopped的类型：stopped stopping failed disabled uninitialized checking  此时，允许更新数据库为stopped
            # 判定可能为running的类型：enable started starting recoverable recovering migrating 此时，禁止更新数据库为stopped，且禁止发送更新消息给WEB，
            
            
            # （系统关闭，动作触发）集群虚拟机系统关闭，关闭后，立即查询clustat仍然显示为started，此时需要发送消息，更新数据库：？？？bsrunning_on == vs_uuid and running_on_xx == vs_uuid
            # （迁移主机，动作触发）集群虚拟机迁移后，原主机虚拟机变成stopped，此时不允许进行数据库修改，仅发送消息：？？？bsrunning_on == vs_uuid and running_on_xx ！= vs_uuid
            # （1800同步，动作触发）集群虚拟机一开始则运行在别的主机，此时不允许进行数据库修改，仅发送消息：？？？bsrunning_on ！= vs_uuid and running_on_xx ！= vs_uuid
            # （异常情况）异常情况，不修改数据库，也不发消息：？？？bsrunning_on ！= vs_uuid and running_on_xx == vs_uuid
            (flag, clvmstate, running_on_ip) = operation.vm.check_vm_state.check_vm_state(uuid)
            if flag and clvmstate in ["enable", "started", "starting", "recoverable", "recovering", "migrating"]:
                if running_on_ip:
                    running_on_xx = ""
                    nodeinfo = operation.vcluster.cluster_interface.get_clusternode_info()
                    for x in nodeinfo:
                        if running_on_ip == x["cluster_ip"]:
                            running_on_xx = x["hostuuid"]
                            break
                    if not running_on_xx:
                        # 未读取到主机uuid，则不更新数据库，不更新消息
                        return (True, allpara)
                    if bsrunning_on == vs_uuid and running_on_xx == vs_uuid:
                        pass
                    elif bsrunning_on == vs_uuid and running_on_xx != vs_uuid:
                        update_db = False
                    elif bsrunning_on != vs_uuid and running_on_xx != vs_uuid:
                        update_db = False
                    elif bsrunning_on != vs_uuid and running_on_xx == vs_uuid:
                        return (True, allpara)
                else:
                    # 未读取到running ip，则可能其他主机正在尝试启动此虚拟机，或在进行recover恢复之类的操作，此时仅更新消息，不更新数据库
                    update_db = False
            if noUpdateDb:
                # vCenterHa特殊处理，迁移源端，进程关闭时，不更新数据库
                update_db = False
                
            if not operation.vcluster.cluster_interface.cluster_is_quorate() and not flag:
                # 20150326集群状态异常，不更新数据库
                update_db = False

                
        if (vmstate == "running" and not running_on) or (not update_db):
            # FT 的备，仅发送消息
            # 迁移虚拟机到其他主机，仅发送消息
            # 1800同步，其他主机运行，当前主机未运行，仅发送消息
            pass
        else:
            operation.vm.vms_op.update_vm_state_for_loop({"vmuuid":uuid, "stat":vmstate, "vnc_port":vnc_port, "running_on":running_on}, is_clustervm)
    except:
        syslog.syslog(syslog.LOG_ERR,'update vm state failed: '+str(traceback.format_exc()))
    # 发送消息给WEB，此时UserDataDisk如果为None则发送消息时不会被携带
    state_message = support.message.message.RefreshLeftTreeStateMessage(action = "update", 
                                                        label = label,
                                                        uuid = uuid, 
                                                        state = vmstate,
                                                        objtype = objtype,
                                                        actionLimit = actionLimit,
                                                        UserDataDisk = UserDataDisk,
                                                        vncpassword = vncPassword,
                                                        spicepassword = spicePassword,
                                                        shadowToSSD = shadowToSSD,
                                                        vnc_port = vnc_port,
                                                        ft_position = ft_position,
                                                        running_on = running_on,
                                                        )
    support.message.vmd_message_queue.put_statemsg_to_web(state_message)
    
    if vmstate != "running":
        # 标记rdp状态为关闭，关闭虚拟机后不可使用远程连接服务。
        msg = support.message.message.VmServiceStateMessage(uuid, "rdp", 0)
        support.message.vmd_message_queue.put_statemsg_to_web(msg)
    return (True, allpara)

def update_and_send_vm_config(uuid, storage_path, vmname):
    
    actionLimit, shadowToSSD = operation.vm.vm_params.get_vm_permission(uuid, storage_path)
    vmobj = {
 "uuid":uuid,
 "actionLimit":actionLimit,
 "shadowToSSD":shadowToSSD,
            }
    if vmname:
        vmobj["label"] = vmname
    # 更新全局变量
    (flag, state) = global_params.update_in_vmsObj(vmobj)
    if not flag:
        if state != "Not in vmsObj":
            return (False, state)
        is_vcuuid,vcuuid,hostip = support.uuid_op.get_vc_uuid()
        if not hostip:
            return (False, state)
        (flag, vminfo) = operation.vm.vms_op.get_vc_vminfo(uuid, hostip)
        if not flag:
            return (False, state)
        # 只有没有运行的集群虚拟机，才有可能出现缓存不存在的情况，需要做此处理
        vmstate = "stopped"
        if vminfo["state"] == "saved":
            vmstate = "saved"
        vmType = vminfo["vms_type"]
        label = vminfo["description"]
        parentUuid = vminfo.get("vms_refrerence_uuid") or vminfo.get("belong") # 配置文件中获取则为belong，数据库则为vms_refrerence_uuid        
        vncPassword = None
        spicePassword = None
        vnc_port = 0
        istemplate = vminfo["modvm"]
        init_vm_message_to_web(uuid, storage_path, vmstate, vmType, label, parentUuid, vncPassword, spicePassword, vnc_port, istemplate)

    # vmname 传递有时候为None，此时label信息不会被更新到WEB
    state_message = support.message.message.RefreshLeftTreeStateMessage(action = "update", label = vmname, 
                                                        uuid = uuid, actionLimit = actionLimit, shadowToSSD = shadowToSSD)
    support.message.vmd_message_queue.put_statemsg_to_web(state_message)

def update_and_send_vm_shadown(uuid, shadowToSSD):
    
    vmobj = {
 "uuid":uuid,
 "shadowToSSD":shadowToSSD,
            }
    # 更新全局变量
    (flag, state) = global_params.update_in_vmsObj(vmobj)
    if not flag:
        return (False, state)
    # 仅更新shadow信息
    state_message = support.message.message.RefreshLeftTreeStateMessage(action = "update", uuid = uuid, shadowToSSD = shadowToSSD)
    support.message.vmd_message_queue.put_statemsg_to_web(state_message)

def send_vm_objtype(uuid, objtype):

    # 仅更新objtype信息
    state_message = support.message.message.RefreshLeftTreeStateMessage(action = "update", uuid = uuid, objtype = objtype)
    support.message.vmd_message_queue.put_statemsg_to_web(state_message)

def set_ft_running_vmobj(uuid, objtype):
    
    vmobj = {
 "uuid":uuid,
 "isFT":"yes",
 "objtype":objtype,
            }
    # 更新全局变量
    (flag, state) = global_params.update_in_vmsObj(vmobj)
    if not flag:
        return (False, state)
    send_vm_objtype(uuid, objtype)    
    
def update_and_send_vm_objtype(uuid, objtype):
    
    vmobj = {
 "uuid":uuid,
 "objtype":objtype,
            }
    # 更新全局变量
    (flag, state) = global_params.update_in_vmsObj(vmobj)
    if not flag:
        if state != "Not in vmsObj":
            return (False, state)
        is_vcuuid,vcuuid,hostip = support.uuid_op.get_vc_uuid()
        if not hostip:
            return (False, state)
        (flag, vminfo) = operation.vm.vms_op.get_vc_vminfo(uuid, hostip)
        if not flag:
            return (False, state)
        # 只有没有运行的集群虚拟机，才有可能出现缓存不存在的情况，需要做此处理
        vmstate = "stopped"
        if vminfo["state"] == "saved":
            vmstate = "saved"
        vmType = vminfo["vms_type"]
        label = vminfo["description"]
        parentUuid = vminfo.get("vms_refrerence_uuid") or vminfo.get("belong") # 配置文件中获取则为belong，数据库则为vms_refrerence_uuid        
        vncPassword = None
        spicePassword = None
        vnc_port = 0
        istemplate = vminfo["modvm"]
        storage_path = vminfo["storage_path"]
        init_vm_message_to_web(uuid, storage_path, vmstate, vmType, label, parentUuid, vncPassword, spicePassword, vnc_port, istemplate)
    send_vm_objtype(uuid, objtype)

def update_and_send_vm_diff_from(uuid, storage_path, diff_from):
    
    actionLimit, shadowToSSD = operation.vm.vm_params.get_vm_permission(uuid, storage_path)
    vmobj = {
 "uuid":uuid,
 "actionLimit":actionLimit,
 "shadowToSSD":shadowToSSD,
            }
    # 更新全局变量
    (flag, state) = global_params.update_in_vmsObj(vmobj)
    if not flag:
        if state != "Not in vmsObj":
            return (False, state)
        is_vcuuid,vcuuid,hostip = support.uuid_op.get_vc_uuid()
        if not hostip:
            return (False, state)
        (flag, vminfo) = operation.vm.vms_op.get_vc_vminfo(uuid, hostip)
        if not flag:
            return (False, state)
        # 只有没有运行的集群虚拟机，才有可能出现缓存不存在的情况，需要做此处理
        vmstate = "stopped"
        if vminfo["state"] == "saved":
            vmstate = "saved"
        vmType = vminfo["vms_type"]
        label = vminfo["description"]
        parentUuid = vminfo.get("vms_refrerence_uuid") or vminfo.get("belong") # 配置文件中获取则为belong，数据库则为vms_refrerence_uuid        
        vncPassword = None
        spicePassword = None
        vnc_port = 0
        istemplate = vminfo["modvm"]
        storage_path = vminfo["storage_path"]
        init_vm_message_to_web(uuid, storage_path, vmstate, vmType, label, parentUuid, vncPassword, spicePassword, vnc_port, istemplate)
    # 更新diff_from，操作限制，影子信息，其中diff_from为None则表示由子虚拟机更新为普通虚拟机
    state_message = support.message.message.RefreshLeftTreeStateMessage(action = "update", 
                                                        uuid = uuid, 
                                                        actionLimit = actionLimit,
                                                        diff_from = diff_from,
                                                        shadowToSSD = shadowToSSD,
                                                        )
    support.message.vmd_message_queue.put_statemsg_to_web(state_message)

def update_and_send_vm_ft_position(uuid, ft_position, running_on):
    
    # 普通迁移的目的端，以及FT的备触发HA变成主的情况，进入此函数
    # 更新消息的同时，需要更新数据库
    vmobj = {
 "uuid":uuid,
 "ft_position":ft_position,
 "running_on":running_on,
            }
    # 更新全局变量
    (flag, state) = global_params.update_in_vmsObj(vmobj)
    if not flag:
        return (False, state)
    
    (flag, bsvmobj) = global_params.get_vmobj_for_send(uuid)
    if flag and bsvmobj:
        try:
            is_clustervm = True
            operation.vm.vms_op.update_vm_state_for_loop({"vmuuid":uuid, "stat":bsvmobj["state"], "vnc_port":bsvmobj["vnc_port"], "running_on":bsvmobj["running_on"]}, is_clustervm)
        except:
            syslog.syslog(syslog.LOG_ERR,'update vm state failed: '+str(traceback.format_exc()))
    
    # 更新running_on,ft状态备变成主，消息更新，缓存变量更新，
    state_message = support.message.message.RefreshLeftTreeStateMessage(action = "update", 
                                                        uuid = uuid, 
                                                        ft_position = ft_position,
                                                        running_on = running_on,
                                                        )
    support.message.vmd_message_queue.put_statemsg_to_web(state_message)

def update_and_send_vm_add_to_cluster(uuid, parentuuid):

    # 更新：parentuuid=belong,parenttype='cluster',objtype='clusterVm',ft_position,vmType=‘cluster’
    vmobj = {
 "uuid":uuid,
 "parentUuid":parentuuid,
 "parentType":"cluster",
 "objtype":"clusterVm",
 "vmType":"cluster",
            }
    ft_position = "normal"
    (flag, bsvmobj) = global_params.get_vmobj_for_send(uuid)
    if not flag or not bsvmobj:
        return (False, bsvmobj)
    if flag and bsvmobj["state"] == "running":
        ft_position = "primary"
    vmobj["ft_position"] = ft_position
    # 更新全局变量
    (flag, state) = global_params.update_in_vmsObj(vmobj)
    if not flag:
        return (False, state)
    
    # 虚拟机加入集群，先发送删除虚拟机消息，
    state_message = support.message.message.RefreshLeftTreeStateMessage(action = "delete",
                                                                        uuid = uuid)
    support.message.vmd_message_queue.put_statemsg_to_web(state_message)
    
    # 单机虚拟机加入集群，需要更新数据库。
    # 此处无法正确更新数据库，将造成：运行的虚拟机加入集群后，vms表running_on字段为空，vnc_port字段为0的问题
    # 此处更新数据库实际不生效，实际更新数据库由虚拟机加入集群任务端missions执行
    try:
        is_clustervm = True
        operation.vm.vms_op.update_vm_state_for_loop({"vmuuid":uuid, "stat":bsvmobj["state"], "vnc_port":bsvmobj["vnc_port"], "running_on":bsvmobj["running_on"]}, is_clustervm)
    except:
        pass
    
    # 后发送，add虚拟机消息，
    # 发送所有键名消息给WEB
    state_message = support.message.message.RefreshLeftTreeStateMessage(action = "add", 
                                                        label = bsvmobj["label"], 
                                                        uuid = uuid, 
                                                        state = bsvmobj["state"],
                                                        parentuuid = parentuuid,
                                                        objtype = "clusterVm",
                                                        parenttype = "cluster",
                                                        actionLimit = bsvmobj["actionLimit"],
                                                        UserDataDisk = bsvmobj["UserDataDisk"],
                                                        vncpassword = bsvmobj["vncPassword"],
                                                        spicepassword = bsvmobj["spicePassword"],
                                                        shadowToSSD = bsvmobj["shadowToSSD"],
                                                        vnc_port = bsvmobj["vnc_port"],
                                                        ft_position = ft_position,
                                                        running_on = bsvmobj["running_on"],
                                                        )
    support.message.vmd_message_queue.put_statemsg_to_web(state_message)
    
    # tools消息
    if bsvmobj["tools"]:
        tools_version = bsvmobj["tools"]["tools_version"]
        sys_version = bsvmobj["tools"]["sys_version"]
        sys_type = bsvmobj["tools"]["sys_type"]
        msg = support.message.message.VmServiceStateMessage(uuid, "tools", tools_version, sys_version, sys_type)
        support.message.vmd_message_queue.put_statemsg_to_web(msg)
    
    # rdp消息
    if not bsvmobj["rdp"]:
        msg = support.message.message.VmServiceStateMessage(uuid, "rdp", 0)
        support.message.vmd_message_queue.put_statemsg_to_web(msg)
    else:
        rpd_status = bsvmobj["rdp"]["rpd_status"]
        userList = bsvmobj["rdp"]["userList"]
        msg = support.message.message.VmRdpUserInfoMessage(uuid, "rdpUserInfo", rpd_status, userList)
        support.message.vmd_message_queue.put_statemsg_to_web(msg)

def add_and_send_notrunning_cluster_vm(uuid, storage_path, vmname, parentuuid, vm_state, objtype, diff_from = "init_para"):
    
    # 仅针对未运行的，集群虚拟机
    actionLimit, shadowToSSD = operation.vm.vm_params.get_vm_permission(uuid, storage_path)
    UserDataDisk = ["init",]
    vmobj = {
 "vmType":"cluster",
 "uuid":uuid,
 "label":vmname,
 "state":vm_state,
 "parentUuid":parentuuid,
 "parentType":"cluster",
 "actionLimit":actionLimit,
 "vncPassword":None,
 "spicePassword":None,
 "shadowToSSD":shadowToSSD,
 "vnc_port":0,
 "ft_position":"normal",
 "running_on":None,
 "UserDataDisk":UserDataDisk,
 "tools":{},
 "rdp":{},
 "isFT":"no",
 "objtype":objtype,
                  }
    # 加到全局变量中
    (flag, state) = global_params.append_to_vmsObj(vmobj)
    if not flag:
        return (False, state)
    # 发送消息
    state_message = support.message.message.RefreshLeftTreeStateMessage(action = "add",
                                                    label = vmname, 
                                                    uuid = uuid, 
                                                    state = vm_state,
                                                    parentuuid = parentuuid,
                                                    objtype = objtype,
                                                    parenttype = "cluster",
                                                    actionLimit = actionLimit,
                                                    UserDataDisk = UserDataDisk,
                                                    vncpassword = None,
                                                    spicepassword = None,
                                                    shadowToSSD = shadowToSSD,
                                                    vnc_port = 0,
                                                    ft_position = "normal",
                                                    running_on = None,
                                                    diff_from = diff_from,
                                                        )
    support.message.vmd_message_queue.put_statemsg_to_web(state_message)

def add_and_send_notrunning_single_vm(uuid, storage_path, vmname, parentuuid, vm_state, objtype, diff_from = "init_para"):
    
    # 仅针对未运行的，单机虚拟机
    actionLimit, shadowToSSD = operation.vm.vm_params.get_vm_permission(uuid, storage_path)
    UserDataDisk = ["init",]
    vmobj = {
 "vmType":"single",
 "uuid":uuid,
 "label":vmname,
 "state":vm_state,
 "parentUuid":parentuuid,
 "parentType":"VServer",
 "actionLimit":actionLimit,
 "vncPassword":None,
 "spicePassword":None,
 "shadowToSSD":shadowToSSD,
 "vnc_port":0,
 "ft_position":None,
 "running_on":None,
 "UserDataDisk":UserDataDisk,
 "tools":{},
 "rdp":{},
 "isFT":"no",
 "objtype":objtype,
                  }
    # 加到全局变量中
    (flag, state) = global_params.append_to_vmsObj(vmobj)
    if not flag:
        return (False, state)
    # 发送消息
    state_message = support.message.message.RefreshLeftTreeStateMessage(action = "add",
                                                    label = vmname, 
                                                    uuid = uuid, 
                                                    state = vm_state,
                                                    parentuuid = parentuuid,
                                                    objtype = objtype,
                                                    parenttype = "VServer",
                                                    actionLimit = actionLimit,
                                                    UserDataDisk = UserDataDisk,
                                                    vncpassword = None,
                                                    spicepassword = None,
                                                    shadowToSSD = shadowToSSD,
                                                    vnc_port = 0,
                                                    ft_position = None,
                                                    running_on = None,
                                                    diff_from = diff_from,
                                                        )
    support.message.vmd_message_queue.put_statemsg_to_web(state_message)
    
def update_and_send_vm_quit_from_cluster(uuid, storage_path, vmname, parentuuid, vm_state):

    # 更新：
    vmobj = {
 "uuid":uuid,
 "parentUuid":parentuuid,
 "parentType":"VServer",
 "objtype":"singleVm",
 "vmType":"single",
 "ft_position":None,
 "label":vmname,
            }
    (flag, bsvmobj) = global_params.get_vmobj_for_send(uuid)
    if flag and bsvmobj:
        # 更新全局变量
        (flag, state) = global_params.update_in_vmsObj(vmobj)
        if not flag:
            return (False, state)
        bsvmobj["label"] = vmname
    else:
        # 停止的集群虚拟机，退出到另一个从来没有初始化过缓存的主机上
        # 虚拟机退出集群，先发送删除虚拟机消息，
        state_message = support.message.message.RefreshLeftTreeStateMessage(action = "delete",
                                                                            uuid = uuid)
        support.message.vmd_message_queue.put_statemsg_to_web(state_message)
        # 后发送add消息。
        add_and_send_notrunning_single_vm(uuid, storage_path, vmname, parentuuid, vm_state, "singleVm")
        return (True, "")
    
    # 虚拟机退出集群，先发送删除虚拟机消息，
    state_message = support.message.message.RefreshLeftTreeStateMessage(action = "delete",
                                                                        uuid = uuid)
    support.message.vmd_message_queue.put_statemsg_to_web(state_message)
    
    # 集群退出到单机，需要更新数据库。
    try:
        is_clustervm = False
        operation.vm.vms_op.update_vm_state_for_loop({"vmuuid":uuid, "stat":bsvmobj["state"], "vnc_port":bsvmobj["vnc_port"], "running_on":bsvmobj["running_on"]}, is_clustervm)
    except:
        pass
    
    # 后发送，add虚拟机消息，
    # 发送所有键名消息给WEB
    state_message = support.message.message.RefreshLeftTreeStateMessage(action = "add", 
                                                        label = bsvmobj["label"], 
                                                        uuid = uuid, 
                                                        state = bsvmobj["state"],
                                                        parentuuid = parentuuid,
                                                        objtype = "singleVm",
                                                        parenttype = "VServer",
                                                        actionLimit = bsvmobj["actionLimit"],
                                                        UserDataDisk = bsvmobj["UserDataDisk"],
                                                        vncpassword = bsvmobj["vncPassword"],
                                                        spicepassword = bsvmobj["spicePassword"],
                                                        shadowToSSD = bsvmobj["shadowToSSD"],
                                                        vnc_port = bsvmobj["vnc_port"],
                                                        ft_position = None,
                                                        running_on = bsvmobj["running_on"],
                                                        )
    support.message.vmd_message_queue.put_statemsg_to_web(state_message)
    
    # tools消息
    if bsvmobj["tools"]:
        tools_version = bsvmobj["tools"]["tools_version"]
        sys_version = bsvmobj["tools"]["sys_version"]
        sys_type = bsvmobj["tools"]["sys_type"]
        msg = support.message.message.VmServiceStateMessage(uuid, "tools", tools_version, sys_version, sys_type)
        support.message.vmd_message_queue.put_statemsg_to_web(msg)
    
    # rdp消息
    if not bsvmobj["rdp"]:
        msg = support.message.message.VmServiceStateMessage(uuid, "rdp", 0)
        support.message.vmd_message_queue.put_statemsg_to_web(msg)
    else:
        rpd_status = bsvmobj["rdp"]["rpd_status"]
        userList = bsvmobj["rdp"]["userList"]
        msg = support.message.message.VmRdpUserInfoMessage(uuid, "rdpUserInfo", rpd_status, userList)
        support.message.vmd_message_queue.put_statemsg_to_web(msg)

def send_delete_vm(uuid):
    
    state_message = support.message.message.RefreshLeftTreeStateMessage(action = "delete",  
                                                        uuid = uuid)
    support.message.vmd_message_queue.put_statemsg_to_web(state_message)

def update_and_send_delete_vm(uuid):
    
    # 更新全局变量
    global_params.delete_from_vmsObj(uuid)
    # 发送消息
    state_message = support.message.message.RefreshLeftTreeStateMessage(action = "delete",  
                                                        uuid = uuid)
    support.message.vmd_message_queue.put_statemsg_to_web(state_message)

def update_and_send_UserDataDisk(uuid, UserDataDisk):

    vmobj = {}
    vmobj["uuid"] = uuid
    vmobj["UserDataDisk"] = UserDataDisk
    # 更新全局变量
    (flag, state) = global_params.update_in_vmsObj(vmobj)
    if not flag:
        return (False, state)
    state_message = support.message.message.RefreshLeftTreeStateMessage(action = "update",
                                                uuid = uuid,
                                                UserDataDisk = UserDataDisk)
    support.message.vmd_message_queue.put_statemsg_to_web(state_message)

def update_UserDataDisk_to_init(uuid):
    
    UserDataDisk = ["init", ]
    update_and_send_UserDataDisk(uuid, UserDataDisk)

def update_rdp_status(uuid, rpd_status, userList):
    
    vmobj = {}
    vmobj['uuid'] = uuid
    vmobj["rdp"] = {"rpd_status":rpd_status, "userList":userList}
    # 更新全局变量
    (flag, state) = global_params.update_in_vmsObj(vmobj)
    if not flag:
        return (False, state)
    msg = support.message.message.VmRdpUserInfoMessage(uuid, "rdpUserInfo", rpd_status, userList)
    support.message.vmd_message_queue.put_statemsg_to_web(msg)

def update_rdp_stopped(uuid):
    
    vmobj = {}
    vmobj['uuid'] = uuid
    vmobj["rdp"] = {}
    # 更新全局变量
    (flag, state) = global_params.update_in_vmsObj(vmobj)
    if not flag:
        return (False, state)
    # 标记rdp状态为关闭，关闭虚拟机后不可使用远程连接服务。
    msg = support.message.message.VmServiceStateMessage(uuid, "rdp", 0)
    support.message.vmd_message_queue.put_statemsg_to_web(msg)

def update_and_send_toosinfo(uuid, tools_version, sys_version, sys_type, is_clustervm):
    
    toolsinfo = {"tools_version":tools_version,"sys_version":sys_version,"sys_type":sys_type}
    vmobj = {
 "uuid":uuid,
 "tools":toolsinfo,
             }
    # 更新全局变量
    (flag, state) = global_params.update_in_vmsObj(vmobj)
    if not flag:
        return (False, state)
    msg = support.message.message.VmServiceStateMessage(uuid, "tools", tools_version, sys_version, sys_type)
    support.message.vmd_message_queue.put_statemsg_to_web(msg)
    
    try:
        if "NoVersion" == tools_version:
            # 卸载tools时被调度
            operation.vm.vmsystem_db_op.add_or_update_vmsystem(uuid, is_clustervm = is_clustervm, tools_state = None)
        else:
            operation.vm.vmsystem_db_op.add_or_update_vmsystem(uuid, is_clustervm = is_clustervm, tools_state = tools_version, sys_type = sys_type, sys_version = sys_version) 
    except:
        syslog.syslog(syslog.LOG_ERR,'Update tools version error: '+str(traceback.format_exc()))

def do_update_vmCache_state(uuid, vmstate):
    
    # 集群虚拟机挂起后，恢复时更新所有集群主机状态为stopped
    vmobj = {
 "uuid":uuid,
 "state":vmstate,
                  }
    # 更新全局变量
    (flag, state) = global_params.update_in_vmsObj(vmobj)
    if not flag:
        return (False, state)
    state_message = support.message.message.RefreshLeftTreeStateMessage(action = "update",
                                                uuid = uuid,
                                                state = vmstate)
    support.message.vmd_message_queue.put_statemsg_to_web(state_message)
    return (True, "")

def do_send_vm_state_only(uuid, vmstate):

    (flag, bsvmobj) = global_params.get_vmobj_for_send(uuid)
    if flag and bsvmobj:
        vnc_port = bsvmobj["vnc_port"]
        running_on = bsvmobj["running_on"]
    else:
        vnc_port = 0
        running_on = None
    try:
        is_clustervm = True
        operation.vm.vms_op.update_vm_state_for_loop({"vmuuid":uuid, "stat":vmstate, "vnc_port":vnc_port, "running_on":running_on}, is_clustervm)
    except:
        syslog.syslog(syslog.LOG_ERR,'update vm state failed: '+str(traceback.format_exc()))
    state_message = support.message.message.RefreshLeftTreeStateMessage(action = "update",
                                                uuid = uuid,
                                                state = vmstate)
    support.message.vmd_message_queue.put_statemsg_to_web(state_message)
    return (True, "")

def resend_all_vms_for_sync_all():
#    (1)将全局变量消息重发一遍，四类：vm消息，tools消息，rdp消息，数据盘消息
#       特殊处理：
#       集群虚拟机退出集群，残留的缓存，判断，“不在集群中”，此缓存无效，删除缓存，不发消息
    clstrs = ""
    all_cluster_vms = []
    clusterVmUuids = []
    vcConnFlag = False
    cluster_type = 'vServerHa'
    try:
        clstrs = file("/etc/cluster/cluster.conf").read()
        is_vcuuid,vcuuid,vc_ip = support.uuid_op.get_vc_uuid()
        if clstrs and is_vcuuid and vcuuid!="127.0.0.1":
            clusteruuid = operation.vcluster.cluster_interface.check_get_cluster_name()
            (flag, cl_vms) = operation.vm.vms_op.get_vms(clusteruuid, vc_ip)
            if flag:
                all_cluster_vms = cl_vms
                if all_cluster_vms:
                    clusterVmUuids = [x["uuid"] for x in all_cluster_vms]
                    vcConnFlag = True
            clusterinfo = db_get_vc('clusters',{'uuid':clusteruuid},vc_ip)
            if clusterinfo:
                cluster_type=clusterinfo['cluster_type']
    except:
        pass
    global_params.locklist['vms_obj_lock'].acquire()
    try:
        will_be_delete_uuids = []
        for itemobj in global_params.vmsObj:
            uuid = itemobj["uuid"]
            vmType = itemobj["vmType"]
            if "single" != vmType:
                if uuid not in clusterVmUuids and vcConnFlag:
                    # 集群虚拟机退出集群，残留的缓存，判断，“不在集群中”，此缓存无效，删除缓存，不发消息
                    will_be_delete_uuids.append(uuid)
                    continue
        # 集群虚拟机退出集群，残留的缓存，判断，“不在集群中”，此缓存无效，删除缓存，不发消息
        for uuid in will_be_delete_uuids:
            for itemobj in global_params.vmsObj:
                if itemobj["uuid"] == uuid:
                    global_params.vmsObj.remove(itemobj)
    finally:
        global_params.locklist["vms_obj_lock"].release()

    has_cached_vm = []
    for itemobj in global_params.vmsObj:
        uuid = itemobj["uuid"]
        vmType = itemobj["vmType"]
        has_cached_vm.append(uuid)
        is_clustervm = False
        update_db = True
        if "single" != vmType:
            is_clustervm = True
            if uuid not in clusterVmUuids and vcConnFlag:
                continue
            # 更新集群虚拟机全局变量：objtype(转变成模版、虚拟机、ft)
            # 这些值可能会在其他主机编辑修改，此时HA的其他主机成员，可能出现缓存信息不一致的问题，
            isFT = "no"
            objtype = "clusterVm"
            vmname = ""
            vmstate = ""
            if operation.vcluster.cluster_interface.clustervm_ft_is_enable(uuid):
                isFT = "yes"
            if isFT == "yes":
                objtype = "clusterMirrorVm"
            
            (flag, itemobj) = global_params.get_vmobj_for_send(uuid)
            for x in all_cluster_vms:
                if x["uuid"] == uuid:
                    if "yes" == x["modvm"]:
                        objtype = "modRootVm"
                    vmname = x["description"]
                    if x["state"] == "saved":
                        vmstate = "saved"
                    elif flag and itemobj:
                        if itemobj["state"] == "saved":
                            vmstate = "stopped"
                    break
            # 更新全局变量
            vmobj = {
         "uuid":uuid,
         "isFT":isFT,
         "objtype":objtype,
                     }
            if vmname:
                vmobj["label"] = vmname
            if vmstate:
                vmobj["state"] = vmstate
            # 更新全局变量
            global_params.update_in_vmsObj(vmobj)
            (flag, itemobj) = global_params.get_vmobj_for_send(uuid)
            if not flag or not itemobj:
                continue
            if cluster_type != 'vCenterHa' and (itemobj["state"] == "stopped" or itemobj["state"] == "saved"):
                # 集群虚拟机，如果当前主机进程不存在，检测集群服务为运行，则不更新数据库，
                # 避免错误更新，造成vC的数据库错误，
                # 判定可能为stopped的类型：stopped stopping failed disabled uninitialized checking  此时，允许更新数据库为stopped
                # 判定可能为running的类型：enable started starting recoverable recovering migrating 此时，禁止更新数据库为stopped，且禁止发送更新消息给WEB，
                # 禁止发消息的类型，同样适用于1800同步时，此时也必须禁止，否则消息会错误，
                (flag, clvmstate,_) = operation.vm.check_vm_state.check_vm_state(uuid)
                if flag and clvmstate in ["enable", "started", "starting", "recoverable", "recovering", "migrating"]:
                    # 当前主机该集群虚拟机状态为stopped，saved，而集群服务为中间状态，或处于运行，则不更新消息也不更新数据库
                    continue
                if not flag:
                    # 集群状态异常时才借助vCenter的数据库和其他主机进行判断
                    #if not stopped_saved_clustervm_need_updatedb(uuid):
                    #update_db = False
                    #主机刚开机，集群初始化未完成，状态异常时，不推送消息
                    continue

            elif cluster_type != 'vCenterHa':
                vs_uuid = support.uuid_op.get_vs_uuid()[1]
                bsrunning_on = itemobj["running_on"]

                # ******此处修改：集群状态可用并且存在FT 虚拟机的备，将vCenter数据库 的running_on字段置空的问题。
                # 判定可能为stopped的类型：stopped stopping failed disabled uninitialized checking  此时，允许更新数据库为stopped
                # 判定可能为running的类型：enable started starting recoverable recovering migrating 此时，禁止更新数据库为stopped，且禁止发送更新消息给WEB，         # （系统关闭，动作触发）集群虚拟机系统关闭，关闭后，立即查询clustat仍然显示为started，此时需要发送消息，更新数据库：？？？bsrunning_on == vs_uuid and running_on_xx == vs_uuid
                # （迁移主机，动作触发）集群虚拟机迁移后，原主机虚拟机变成stopped，此时不允许进行数据库修改，仅发送消息>：？？？bsrunning_on == vs_uuid and running_on_xx ！= vs_uuid
                # （1800同步，动作触发）集群虚拟机一开始则运行在别的主机，此时不允许进行数据库修改，仅发送消息：？？？bsrunning_on ！= vs_uuid and running_on_xx ！= vs_uuid
                # （异常情况）异常情况，不修改数据库，也不发消息：？？？bsrunning_on ！= vs_uuid and running_on_xx == vs_uuid

                (flag, clvmstate, running_on_ip) = operation.vm.check_vm_state.check_vm_state(uuid)
                if flag and clvmstate in ["enable", "started", "starting", "recoverable", "recovering", "migrating"]:
                    if running_on_ip:
                        running_on_xx = ""
                        nodeinfo = operation.vcluster.cluster_interface.get_clusternode_info()
                        for x in nodeinfo:
                            if running_on_ip == x["cluster_ip"]:
                                running_on_xx = x["hostuuid"]
                                break
                        if not running_on_xx:
                            # 未读取到主机uuid，则不更新数据库，不更新消息
#                             return (True, allpara)
                            update_db = False
                        if bsrunning_on == vs_uuid and running_on_xx == vs_uuid:
                            pass
                        elif bsrunning_on == vs_uuid and running_on_xx != vs_uuid:
                            update_db = False
                        elif bsrunning_on != vs_uuid and running_on_xx != vs_uuid:
                            update_db = False
                        elif bsrunning_on != vs_uuid and running_on_xx == vs_uuid:
                            update_db = False
                            #return (True, allpara)
                    else:
                        # 未读取到running ip，则可能其他主机正在尝试启动此虚拟机，或在进行recover恢复之类的操作，此时仅>更新消息，不更新数据库
                        update_db = False
                if not flag:
                    # 20150326集群状态异常，不更新数据库
                    update_db = False

            if update_db and cluster_type == 'vCenterHa' and itemobj["state"] != "running":
                nodeinfo = operation.vcluster.cluster_interface.get_clusternode_info()
                vs_uuid = support.uuid_op.get_vs_uuid()[1]
                for x in nodeinfo:
                     if vs_uuid == x["hostuuid"]:
                         continue
                     try:
                         (flag, psh) = vmd_utils.get_rpcConnection(x["cluster_ip"])
                         if psh.do_check_vm_rstat(uuid):
                             # vCenterHa类虚拟机，别的主机在运行，则不更新数据库
                             update_db = False
                             break
                     except:
                         pass
        if update_db:
            #operation.vm.vms_op.update_vm_state_for_loop({"vmuuid":uuid, "stat":itemobj["state"], "vnc_port":itemobj["vnc_port"], "running_on":itemobj["running_on"]}, is_clustervm)
            # 虚拟机数量众多时，消息同步缓慢，主要耗时点在这里
            # 此处临时线程处理，暂时回避数据库同步缓慢的问题。
            vmIstate = {"vmuuid":uuid, "stat":itemobj["state"], "vnc_port":itemobj["vnc_port"], "running_on":itemobj["running_on"]}
            new_subthread.addtosubthread("update_vm_state_for_loop_in_all_"+uuid, operation.vm.vms_op.update_vm_state_for_loop, vmIstate, is_clustervm)
        # vm消息，数据盘消息
        # 注释键，不需要发送
        state_message = support.message.message.RefreshLeftTreeStateMessage(action = "update", 
                                                            #label = itemobj["label"], 
                                                            uuid = uuid, 
                                                            state = itemobj["state"],
                                                            #parentuuid = itemobj["parentUuid"],
                                                            #objtype = itemobj["objtype"],
                                                            #parenttype = itemobj["parentType"],
                                                            actionLimit = itemobj["actionLimit"],
                                                            UserDataDisk = itemobj["UserDataDisk"],
                                                            vncpassword = itemobj["vncPassword"],
                                                            spicepassword = itemobj["spicePassword"],
                                                            #diff_from = None,
                                                            shadowToSSD = itemobj["shadowToSSD"],
                                                            vnc_port = itemobj["vnc_port"],
                                                            ft_position = itemobj["ft_position"],
                                                            running_on = itemobj["running_on"],
                                                            )
        support.message.vmd_message_queue.put_statemsg_to_web(state_message)
        
        # tools消息
        if itemobj["tools"]:
            tools_version = itemobj["tools"]["tools_version"]
            sys_version = itemobj["tools"]["sys_version"]
            sys_type = itemobj["tools"]["sys_type"]
            msg = support.message.message.VmServiceStateMessage(uuid, "tools", tools_version, sys_version, sys_type)
            support.message.vmd_message_queue.put_statemsg_to_web(msg)
        
        # rdp消息
        if not itemobj["rdp"]:
            msg = support.message.message.VmServiceStateMessage(uuid, "rdp", 0)
            support.message.vmd_message_queue.put_statemsg_to_web(msg)
        else:
            rpd_status = itemobj["rdp"]["rpd_status"]
            userList = itemobj["rdp"]["userList"]
            msg = support.message.message.VmRdpUserInfoMessage(uuid, "rdpUserInfo", rpd_status, userList)
            support.message.vmd_message_queue.put_statemsg_to_web(msg)
    # 处理 global_params.vmsObj 中没有的虚拟机
    for clvm in all_cluster_vms:
        if clvm['uuid'] in has_cached_vm:
            continue

        # 如果这两个线程还存活，则开机初始化未完成，不需要1800进行补充初始化，直接返回
        # init_notrunning_single_vm_message_to_web
        # init_notrunning_cluster_vm_message_to_web
        for x in ["init_notrunning_single_vm_message_to_web", "init_notrunning_cluster_vm_message_to_web"]:
            td = global_params.thread_list.get(x)
            if td and td["threadobject"].isAlive():
                return

        # 如果虚拟机运行在此主机上则在 global_params.vmsObj中必定有此虚拟机信息。
        init_vm_message_to_web(clvm['uuid'], clvm['storage_path'], clvm['state'], clvm['vms_type'], clvm['description'], clvm['vms_refrerence_uuid'], None, None, None, clvm['modvm'])
    

def vm_listener(uuid, storage_path, port_name, vmType, label, parentUuid, vncPassword, spicePassword, vnc_port, istemplate):
    
    """
      监听vm消息的服务    
    """
    # 初始化vm running全局变量，并发送给web
    # 此处处理失败，后续操作仍然继续，目的在于清理可能存在的虚拟机进程的其他缓存对象值
    if global_params.vm_in_vmsObj(uuid):
        update_vm_state_message_to_web(uuid, storage_path, vmType, "running", vncPassword, spicePassword, vnc_port)
    else:
        init_vm_message_to_web(uuid, storage_path, "running", vmType, label, parentUuid, vncPassword, spicePassword, vnc_port, istemplate)

    #
    serial_file = "/var/run/%s/virtio-serial1.sock" % uuid
    #初始化回复队列
    if not uuid in vm_listener_object.VM_SOCKET:
        vm_listener_object.VM_SOCKET[uuid] = {}
    try:
        while True:
            s = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
            s.connect(serial_file)
            s.settimeout(None)  # 设置成永不超时，否则会继承socket的默认超时，此默认值在rpc客户端创建时被初始化成了30秒
            # connect结束，不能立即recv，否则大多数情况都是timeout失败
            time.sleep(1)

            vm_listener_object.VM_SOCKET[uuid][port_name] = s           
            try:
                try:
                    _start_listener(uuid, port_name, s)
                except:
#                     print "except:",traceback.format_exc()
                    tn_message = " ".join(["vm_listener except:",  
                                           uuid, traceback.format_exc()])
                    support.log.log.Log(tn_message).error()
                    ##如果虚拟机进程还在，重新尝试链接，否则退出
                    #is_exist, _ = operation.vm.vm_running_script.check_kvm_process_exist(uuid)
                    #if not is_exist:
                    #    support.log.log.Log(tn_message).error()
                    #    return
                    #time.sleep(5)

                    # tools连接异常，则退出，不再反复尝试连接tools
                    return
            finally:
                try:
                    s.close()
                except:
                    pass
                vm_listener_object.VM_SOCKET[uuid].pop(port_name, None)
    finally:
        target_storage_path = None
        if global_params.vms_process_info.get(uuid):
            target_storage_path = global_params.vms_process_info[uuid].get("target_storage_path")
        targetvmType = None
        noUpdateDb = None
        if global_params.vms_process_info.get(uuid):
            targetvmType = global_params.vms_process_info[uuid].get("targetvmType")
            noUpdateDb = global_params.vms_process_info[uuid].get("noUpdateDb")
        
        # 清理global_params.vms_process_info缓存
        global_params.vms_process_info.pop(uuid, None)
        # 清理启动缓存
        global_params.vms_cache.pop(uuid, None)
        
        #清理应用           
        vm_listener_object.APP_PORT.pop(uuid, None)
        
        vm_listener_object.VM_SOCKET.pop(uuid, None)
        #清理虚拟机的tools订阅
        vm_listener_object.TOOLS_SUBCRIBE.pop(uuid, None)
        #清理虚拟机应用最新一次消息时间记录
        vm_listener_object.VM_GUEST_LAST_MSG_TIME.pop(uuid, None)
                   
        #清理虚拟机内存情况缓存
        global_params.VM_MEM_INFO.pop(uuid, None)
        
        for cgcpu in operation.vm.cgroup_conf.CGCPUS:
            cgcpu.remove_vm(uuid)
        
        if target_storage_path:
            # 解决运行中的虚拟机存储迁移，发生storage_path变更，而tools监控线程却不知情，导致actionlimit信息更新失败的问题
            storage_path = target_storage_path
        if targetvmType:
            # 解决运行中的单机虚拟机加入集群，类型发生变更的问题
            vmType = targetvmType
        
        # 更新全局变量，并发送给web
        (flag, allpara) = update_vm_state_message_to_web(uuid, storage_path, vmType, "stopped", None, None, 0, noUpdateDb)
        
        #虚拟机关机清理
        clean_vnet(allpara)
