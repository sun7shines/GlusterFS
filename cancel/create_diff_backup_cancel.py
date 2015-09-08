# -*- coding: utf-8 -*-
""""""

import os
import global_params
import operation.vm.delete_snapshot_option

def rollback(event,pos,tgfiles):

    for tgfn in tgfiles:
        if global_params.zcp_file_cancel.has_key(tgfn.strip().split('/')[-1]):
            global_params.zcp_file_cancel.pop(tgfn.strip().split('/')[-1])
    
    #pos = 1,2 执行步骤相同
       
    vmuuid = event.param["vmuuid"]
    targetstorage_path = event.param["targetstorage_path"]
    backup_type = event.param["backup_type"]
    
    target_backup_path = targetstorage_path + "/backup_" + \
                           vmuuid + "/" + backup_type
        
    (_, storage_path) = operation.vm.vm_params.get_vm_storage_path(vmuuid)
            
    for tgfn in tgfiles:
        cmd = 'rm -f %s' % (tgfn)
        os.system(cmd)
        
    snpfile = "%s/%s/snap_cfg.xml" % (storage_path, vmuuid)
    all_snap = []
    if os.access(snpfile, os.F_OK):
        (snflag, all_snap) = operation.vm.refresh_vmsnapshot.get_vmsnapshot(vmuuid, storage_path)
        if (not snflag) or (not all_snap):
            return False,''

    youarehere_parent_id = ""
    for x in all_snap:
        if x["name"] != "You Are Here":
            continue
        youarehere_parent_id = x["parent_id"]
    if not youarehere_parent_id:
        return (False, "snapshot info error")
                
    event.param["snname"] = str(youarehere_parent_id) + "_" + vmuuid
    
    (flag, state) = operation.vm.delete_snapshot_option.option(event)
    if not flag:
        return (False, state)
    
    if event.param.get('firstsnp') == True:
        
        cmd = 'rm -rf %s' % (target_backup_path)
        os.system(cmd)

        snpfile = "%s/%s/snap_cfg.xml" % (storage_path, vmuuid)
        all_snap = []
        if os.access(snpfile, os.F_OK):
            (snflag, all_snap) = operation.vm.refresh_vmsnapshot.get_vmsnapshot(vmuuid, storage_path)
            if (not snflag) or (not all_snap):
                return False,''
                
        youarehere_parent_id = ""
        for x in all_snap:
            if x["name"] != "You Are Here":
                continue
            youarehere_parent_id = x["parent_id"]
        if not youarehere_parent_id:
            return (False, "snapshot info error")
                    
        event.param["snname"] = str(youarehere_parent_id) + "_" + vmuuid
    
        (flag, state) = operation.vm.delete_snapshot_option.option(event)
        if not flag:
            return (False, state)
                            
    return True,''