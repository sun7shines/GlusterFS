# -*- coding: utf-8 -*-

import os
import datetime
import time
import struct
import Queue
try:
    import json
except ImportError:
    import simplejson as json
    
import operation.vnetwork.vswitch_interface
import operation.vm.vm_tools_request
import vm_listener_object
import support.uuid_op
import global_params


def vswitch_middleware(uuid, agreement, message_raw):
    
    if agreement == vm_listener_object.FRONVIO_PROTO_DATA:
        flag, message = operation.vnetwork.vswitch_interface.run_vswitch_cmd(message_raw)
        mess_length = len(message)
        if flag:
            response = struct.pack("B%ss" % mess_length, 0, message)
        else:
            response = struct.pack("B%ss" % mess_length, 1, message)
        yield vm_listener_object.FRONVIO_PROTO_DATA_ACK, response
    elif agreement == vm_listener_object.FRONVIO_PROTO_KEEPALIVE:
        yield vm_listener_object.FRONVIO_PROTO_KEEPALIVE_ACK, message_raw
    
    
def host_manager(uuid, agreement, message_raw):
    
    if agreement == vm_listener_object.FRONVIO_PROTO_KEEPALIVE:
        yield vm_listener_object.FRONVIO_PROTO_KEEPALIVE_ACK, message_raw
  
    
def fronware_tool_middleware(uuid, agreement, message_raw):
       
    if agreement == vm_listener_object.FRONVIO_PROTO_CONNECT:
        operation.vm.vm_tools_request.init_status(uuid)
        yield vm_listener_object.FRONVIO_PROTO_CONNECT_ACK, ""
    
    if agreement == vm_listener_object.FRONVIO_PROTO_CONNECT_SERIAL2:
        # 防火墙连接后， 初始化系统系统IP
        operation.vm.vm_tools_request.init_status_serial2(uuid)
        yield vm_listener_object.FRONVIO_PROTO_CONNECT_ACK, ""

    if agreement == vm_listener_object.FRONVIO_PROTO_KEEPALIVE:
        # 心跳消息。
        yield vm_listener_object.FRONVIO_PROTO_KEEPALIVE_ACK, message_raw
    elif agreement == vm_listener_object.FRONVIO_PROTO_JSON:
        message_raw = '''"'''.join(message_raw.split("'"))
        content = json.JSONDecoder().decode(message_raw)
        for response in operation.vm.vm_tools_request.handle_request(uuid, content):
            response = str(json.JSONEncoder(ensure_ascii = False).encode(response))
            yield vm_listener_object.FRONVIO_PROTO_JSON_ACK, response
    elif agreement == vm_listener_object.FRONVIO_PROTO_JSON_ACK:
        message_raw = '''"'''.join(message_raw.split("'"))
        content = json.JSONDecoder().decode(message_raw)
        if content["mission_id"] in  vm_listener_object.TOOLS_RESPONSES:
            vm_listener_object.TOOLS_RESPONSES[content["mission_id"]].put(content)
    elif agreement == vm_listener_object.FRONVIO_PROTO_LEAVE:
        vm_listener_object.APP_PORT[uuid][vm_listener_object.FRONVIO_PORT_FRONVARETOOL_GUEST] = None
    
def update_guest_last_msg_time(guest, vm_info):
    
    vm_listener_object.VM_GUEST_LAST_MSG_TIME.setdefault(vm_info,{})[guest] = time.time()
    

def tools_request_single_iter_response(content, uuid, timeout = 150):
    
    return tools_request_iter_responses(uuid, content, timeout)

def tools_request_multitude_iter_response(content, response_handler, timeout = 150, filter_fun = None):
    
    for vm_str in vm_listener_object.VM_SOCKET:
        if not filter_fun or filter_fun(vm_str):
            for response in tools_request_iter_responses(vm_str, content, timeout):
                response_handler(vm_str, response)
            
def tools_send_multitude(content, filter_fun = None):
    
    for vm_str in vm_listener_object.VM_SOCKET:
        if not filter_fun or filter_fun(vm_str):
            tools_request(vm_str, content)
        

def send_base(vm_info, content, agreement, guest_port, host_port):
     
    def get_port_name(vm_info, content):
        if content.get("port_name"):
            return content.get("port_name")
        if vm_listener_object.APP_PORT.get(vm_info) and \
           vm_listener_object.APP_PORT[vm_info].get(guest_port):
            port_name = vm_listener_object.APP_PORT[vm_info][guest_port]
        else:
            port_name = "serial1"
        return port_name
    
    message =  str(json.JSONEncoder(ensure_ascii = False).encode(content))
    port_name = get_port_name(vm_info, content)
    if  vm_listener_object.VM_SOCKET.get(vm_info) and vm_listener_object.VM_SOCKET[vm_info].get(port_name):
        s = vm_listener_object.VM_SOCKET[vm_info][port_name]
        head_format = "BBBBI"
        head_size = struct.calcsize(head_format)
        length = len(message) + head_size
        response_head = struct.pack(head_format, vm_listener_object.VERSION, agreement,
                                        guest_port, host_port, length)
        
        global_params.locklist["vm_tools_send_msg_lock"].acquire()
        res = ""
        try:
            res = s.send(response_head + message)
        finally:
            global_params.locklist["vm_tools_send_msg_lock"].release()
        
        to_log(vm_info, "send", repr(response_head + message + ":length:" + str(length) + ":res:" + str(res)))
        return True
    return False

def tools_request(vm_info, content, mission_id = None):
    
    if not mission_id:
        mission_id = support.uuid_op.get_uuid()
    content["mission_id"] = mission_id
    content["progress"] = "0"
    content["mission_state"] = "queued"
    return send_base(vm_info, content, vm_listener_object.FRONVIO_PROTO_JSON, 
                     vm_listener_object.FRONVIO_PORT_FRONVARETOOL_GUEST, 
                     vm_listener_object.FRONVIO_PORT_FRONVARETOOL_HOST)
    
def tools_request_iter_responses(vm_info, content, timeout = 150):
    
    mission_id = support.uuid_op.get_uuid()    
    vm_listener_object.TOOLS_RESPONSES[mission_id] = Queue.Queue()
    try:
        flag = tools_request(vm_info, content, mission_id)
        if flag:
            if global_params.vms_process_info.get(vm_info) and global_params.vms_process_info.get(vm_info).get("tools_ready") == "yes":
                try:
                    while True:
                        response = vm_listener_object.TOOLS_RESPONSES[mission_id].get(timeout = timeout)
                        yield response
                        if response["mission_state"] in ("successed", "failed", "canceled"):
                            break
                except Queue.Empty:
                    to_log(vm_info, "recv msg", "Send msg:%s failed:Timeout %s" % (str(content), timeout))
                    yield {"mission_state":"failed", "failed_code":"Timeout %s seconds" % (str(timeout))}
            else:
                yield {"mission_state":"failed", "failed_code":"Tools is not ready"}
        else:
            yield {"mission_state":"failed", "failed_code":"Tools is not ready or not installed"}
    finally:
        del vm_listener_object.TOOLS_RESPONSES[mission_id]

def to_log(vm_info, msg_type, message):

    if global_params.IS_DEBUG_MESSAGE: 
        if not os.path.exists("/var/log/tools/"):
            os.mkdir("/var/log/tools/")
        log_file_path = "/var/log/tools/%s.log" % vm_info.replace("/", "@")
        f = file(log_file_path,"a")
        try:
            
            nowtime = datetime.datetime.now().ctime()
            f.write("".join([nowtime,"\n",msg_type,"\n",message,"\n\n"]))
        finally:
            f.close()
