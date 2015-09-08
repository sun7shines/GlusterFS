# -*- coding: utf-8 -*-
""""""

import os
import syslog

import execomd
import support.cmd_exe
import operation.vm.conf_xml
import global_params
import operation.vm.vm_running_script
import operation.vstorage.vm_cache
import operation.vm.vm_params
import operation.gluster.volume_db



def rollback(event,pos,tgfiles):
 
    for tgfn in tgfiles:
        if global_params.zcp_file_cancel.has_key(tgfn.strip().split('/')[-1]):
            global_params.zcp_file_cancel.pop(tgfn.strip().split('/')[-1]) 
               
    # pos =1,2,3 操作相同
    
    vmuuid = event.param["vmuuid"]
    vmtype = event.param["vmtype"]
    backup_type = event.param["backup_type"]
    targetstorage_path = event.param["targetstorage_path"] # e.g target storage path
    
    # e.g check target storage
    (_, storage_path) = operation.vm.vm_params.get_vm_storage_path(vmuuid)
    
    vm_op = operation.vm.conf_xml.vm_operation(vmuuid, storage_path)
    (flag, allpara) = vm_op.get_vm_all_para()
    if (not flag) or (not allpara):
        return (False, "vmconfig failed")
    
    num = 1
    while True:
        if num >= 3600:
            break
        if os.access("%s/backup_%s/%s/%d/%s" % (targetstorage_path, vmuuid, backup_type, num, vmuuid), os.F_OK):
            num += 1
            continue
        break
    num = num-1
    if num == 3600:
        return (False, "vm completebackup limit 3600")
    targetdir = "%s/backup_%s/%s/%d" % (targetstorage_path, vmuuid, backup_type, num)
    
    syslog.syslog(syslog.LOG_ERR,'vm:%s storage:%s num:%s targetdir:%s' % (vmuuid,storage_path,num,targetdir))
    
    cmd = 'rm -rf %s' % (targetdir)
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
            cmd = execomd.vmcomd["fvmm-vdisk"]+" commit %s/%s/tmp_%s" % (storage_path, vmuuid, vhd["baseimg"].split("/")[-1])
            (cmdstat, rparams) = support.cmd_exe.cmd_exe(cmd)
            if not cmdstat:
                return (False, rparams)
                # e.g change img
            if ("/mnt/" in vhd["baseimg"]) and (vhd["runningimg"] == vhd["baseimg"].split("/")[-1]):
                cmd = operation.gluster.volume_db.changeimg_cmd("%s/%s/tmp_%s" % (storage_path, vmuuid, vhd["baseimg"].split("/")[-1]),
                                                                "%s" % (vhd["baseimg"]))
                (ret, strs) = operation.vm.vm_running_script.monitor_exec(vmuuid,  cmd, 30)
                if not ret:
                    return (False, strs)
            else:
                # e.g change img
                cmd = operation.gluster.volume_db.changeimg_cmd("%s/%s/tmp_%s" % (storage_path, vmuuid, vhd["baseimg"].split("/")[-1]), 
                                                                "%s/%s/%s" % (storage_path, vmuuid, vhd["runningimg"]))
                (ret, strs) = operation.vm.vm_running_script.monitor_exec(vmuuid,cmd, 30)
                if not ret:
                    return (False, strs)
            cmd = "rm -rf %s/%s/tmp_%s" % (storage_path, vmuuid, vhd["baseimg"].split("/")[-1])
            os.system(cmd)
        cmd = "cont\n"
        (ret, strs) = operation.vm.vm_running_script.monitor_exec(vmuuid, cmd, 5)
        if (not ret):
            return (False,"continue vm failed")

    if "single" == vmtype:
        (flag, state) = operation.vstorage.vm_cache.create_vm_ssd_cache(vmuuid, storage_path)
        if not flag:
            return (False, state)
                   
    return True,''
