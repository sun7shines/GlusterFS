# -*- coding: utf-8 -*-

import operation.vm.vm_running_script
import operation.gluster.volume_db
import global_params

def rollback(event,pos,tgfiles):

    for tgfn in tgfiles:
        if global_params.zcp_file_cancel.has_key(tgfn.strip().split('/')[-1]):
            global_params.zcp_file_cancel.pop(tgfn.strip().split('/')[-1])
            
    tvm_uuid = event.param.get('tvm_uuid')
    storage_path = event.param.get('tstorage_path')
    
    vm_op = operation.vm.conf_xml.vm_operation(tvm_uuid, storage_path)
    (_, allpara) = vm_op.get_vm_all_para(tvm_uuid)
        
    (flag, _) = operation.vm.vm_running_script.check_kvm_process_exist(tvm_uuid)
    if flag:
        for x in allpara["hd"]:
            cmd = operation.gluster.volume_db.changeimg_cmd("%s/%s/%s" % (storage_path, tvm_uuid, x["runningimg"]),
                                                                "%s/%s/%s" % (storage_path, tvm_uuid, x["runningimg"]))
            (ret, strs) = operation.vm.vm_running_script.monitor_exec(tvm_uuid, cmd, 30)
            if not ret:
                return (False, strs)
        
        cmd = "cont\n"
        (ret, _) = operation.vm.vm_running_script.monitor_exec(tvm_uuid, cmd, 2)
        if (not ret):
            return (False,"suspend vm failed")
        
    return True,''

