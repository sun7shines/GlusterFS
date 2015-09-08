# -*- coding: utf-8 -*-
""""""

import os
import syslog

import execomd
import operation.vm.conf_xml
import support.cmd_exe
import global_params
import operation.vm.vm_running_script
import operation.vm.vm_params
import operation.vstorage.vm_cache
import operation.gluster.volume_db

def rollback(event,pos,tgfiles):

        
    for tgfn in tgfiles:
        if global_params.zcp_file_cancel.has_key(tgfn.strip().split('/')[-1]):
            global_params.zcp_file_cancel.pop(tgfn.strip().split('/')[-1])
        
    ##pos =1,2,3的操作相同

    vmuuid = event.param["vmuuid"]
    vmtype = event.param["vmtype"]
    storage_path = event.param["storage_path"] # e.g newparam,targetstoragepath
    new_uuid = event.param["target_vmuuid"] 
    
    (flag, bsstorage_path) = operation.vm.vm_params.get_vm_storage_path(vmuuid)
    if not flag:
        return (False, bsstorage_path)

    syslog.syslog(syslog.LOG_ERR,'src_vm:%s src_storage:%s dst_vm:%s dst_storage:%s' % (vmuuid,bsstorage_path,new_uuid,storage_path))
    
    vm_op = operation.vm.conf_xml.vm_operation(vmuuid, bsstorage_path)
    (flag, allpara) = vm_op.get_vm_all_para()
    if (not flag) or (not allpara):
        return (False, "vmconfig failed")

    #删除拷贝的文件，删除创建的目录，逆向切换img

    dir_name = "%s/%s" % (storage_path, new_uuid)
    cmd = 'rm -rf %s' % (dir_name)
    os.system(cmd)

    (vmrunningflag, state) = operation.vm.vm_running_script.check_kvm_process_exist(vmuuid)
    if vmrunningflag:
        cmd = "stop\n"
        (ret, strs) = operation.vm.vm_running_script.monitor_exec(vmuuid, cmd, 2)
        if (not ret):
            return (False,"suspend vm failed")
        for vhd in allpara["hd"]:
            if vhd.get("persistence") == "no":
                continue
            cmd = execomd.vmcomd["fvmm-vdisk"]+" commit %s/%s/tmp_%s" % (bsstorage_path, vmuuid, vhd["baseimg"].split("/")[-1])
            (cmdstat, rparams) =support.cmd_exe.cmd_exe(cmd)
            if not cmdstat:
                return (False, rparams)
                # e.g change img
            if ("/mnt/" in vhd["baseimg"]) and (vhd["runningimg"] == vhd["baseimg"].split("/")[-1]):
                cmd = operation.gluster.volume_db.changeimg_cmd("%s/%s/tmp_%s" % (bsstorage_path, vmuuid, vhd["baseimg"].split("/")[-1]),
                                                                "%s" % (vhd["baseimg"]))
                (ret, strs) = operation.vm.vm_running_script.monitor_exec(vmuuid, cmd, 30)
                if not ret:
                    return (False, strs)
            else:
                # e.g change img
                cmd = operation.gluster.volume_db.changeimg_cmd("%s/%s/tmp_%s" % (bsstorage_path, vmuuid, vhd["baseimg"].split("/")[-1]),
                                                                "%s/%s/%s" % (bsstorage_path, vmuuid, vhd["runningimg"]))
                (ret, strs) = operation.vm.vm_running_script.monitor_exec(vmuuid, cmd, 30)
                if not ret:
                    return (False, strs)
            # e.g clear from storage
            tmp_img = "%s/%s/tmp_%s" % (bsstorage_path, vmuuid, vhd["baseimg"].split("/")[-1])
            t_tmp_img = "%s/%s/tmp_%s" % (storage_path, new_uuid, vhd["baseimg"].split("/")[-1])
            cmd = "rm -rf " + tmp_img + " " + t_tmp_img
            os.system(cmd)
            
        cmd = "cont\n"
        (ret, strs) = operation.vm.vm_running_script.monitor_exec(vmuuid, cmd, 5)
        if (not ret):
            return (False,"continue vm failed")
        
    if "single" == vmtype:
        (flag, state) = operation.vstorage.vm_cache.create_vm_ssd_cache(vmuuid, bsstorage_path)
        if not flag:
            return (False, state)

    return True,''
