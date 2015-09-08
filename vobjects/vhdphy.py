# -*- coding: utf-8 -*-

import os
import syslog
import support.cmd_exe
import copy


import operation.vm.vm_running_script

import execomd
import support.fileutil.zcp_option
import operation.vm.cmd_util

from operation.vobjects.vhd import VHD

    
class VHDPHY(VHD):
    def __init__(self,*args, **kwargs):
        VHD.__init__(self,*args, **kwargs)
        return

    def create_snapshot(self):
        vmuuid = self.vmuuid
        storage_path = self.storage_path
        vhd = self.vhd
        
        nowdesc = self.nowdesc
        crnowdesc = self.crnowdesc
        firstsnp = self.firstsnp
        youarehere_parent_id = self.youarehere_parent_id
        youarehere_id = self.youarehere_id
        target_ip = self.target_ip
        vm_is_running = self.vm_is_running

        if firstsnp or (vhd["runningimg"].split("/")[-1] == vhd["baseimg"].split("/")[-1]) or (vhd["runningimg"] == vhd["baseimg"]):
            # e.g 第一次创建快照 或 某个硬盘第一次创建快照.
            # e.g 如果是某个硬盘第一次创建快照，则判断该硬盘的curr_snap信息是否和当前快照信息一致，不一致则不允许创建快照.
            #允许物理硬盘创建快照。baseimg=/dev/sdc,runningimg=/dev/sdc
            if not firstsnp:
                if (not vhd.get("curr_snap")) or (vhd.get("curr_snap") != str(youarehere_parent_id)):# 不一致
                    return True,''
      
            if not os.access(vhd['baseimg'],os.F_OK):
                return False,str(vhd["baseimg"])+' not exists'
            
            aliasimg = vhd.get('aliasimg')
            if not aliasimg:
                return False,'hd conf error,no aliasimg: '+str(vhd["baseimg"])
            
            os.system("rm -rf %s_%s" % (nowdesc, aliasimg))
            cmd = "cd %s/%s && fvmm-vdisk " % (storage_path, vmuuid) + " create -f fronfs -o backing_file=%s %s_%s && cd -" % (vhd["baseimg"], nowdesc, aliasimg)
            (cmdstat, rparams) = support.cmd_exe.cmd_exe(cmd)

            if not cmdstat:
                return (False, rparams)
     
            cfile = storage_path + "/" + vmuuid + "/" + nowdesc + "_" + vhd["aliasimg"]
            if not os.access(cfile, os.F_OK):
                state = "Create diff img failed, File not exist: " + cfile + " vmuuid: " + vmuuid + " storage_path: " + storage_path
                syslog.syslog(syslog.LOG_ERR, state)
                return (False, state)

            os.system("rm -rf %s_%s" % (crnowdesc, vhd["aliasimg"]))
            cmd = "cd %s/%s && fvmm-vdisk " % (storage_path, vmuuid) + " create -f fronfs -o backing_file=%s_%s %s_%s && cd -" % (nowdesc, vhd["aliasimg"], crnowdesc, vhd["aliasimg"])
            (cmdstat, rparams) = support.cmd_exe.cmd_exe(cmd)

            if not cmdstat:
                return (False, rparams)

        else:
            
            if (youarehere_id != vhd["runningimg"].split("/")[-1].split("_")[0]):
                # e.g 如果某个硬盘在子节点上有快照，在当前节点无快照。则不允许在此时创建快照。
                return True,''

            #物理硬盘之前创建过快照
            cfile = storage_path + "/" + vmuuid + "/" + vhd["runningimg"]
            if not os.access(cfile, os.F_OK):
                state = "Create diff img failed, File not exist: " + cfile + " vmuuid: " + vmuuid + " storage_path: " + storage_path
                syslog.syslog(syslog.LOG_ERR, state)
                return (False, state)

            os.system("rm -rf %s_%s" % (crnowdesc, vhd["aliasimg"]))
            cmd = "cd %s/%s && fvmm-vdisk " % (storage_path, vmuuid) + " create -f fronfs -o backing_file=%s %s_%s && cd -" % (vhd["runningimg"], crnowdesc, vhd["aliasimg"])
            (cmdstat, rparams) = support.cmd_exe.cmd_exe(cmd)

            if not cmdstat:
                return (False, rparams)
                
        if vm_is_running:

            if (firstsnp or (vhd["runningimg"] == vhd["baseimg"])) and ("/dev" in vhd["baseimg"]):
                ## operation.gluster.volume_db.changeimg_cmd
                cmd = "changeimg %s %s/%s/%s_%s\n" % (vhd["baseimg"], storage_path, vmuuid, crnowdesc, vhd["aliasimg"])
            else:
                cmd = "changeimg %s/%s/%s %s/%s/%s_%s\n" % (storage_path, vmuuid, vhd["runningimg"], storage_path, vmuuid, crnowdesc, vhd["aliasimg"])
                
            (ret, strs) = operation.vm.vm_running_script.monitor_exec(vmuuid, cmd, 30)
            if not ret:
                return (False, strs)
            
            # e.g change ft vm img.
            (flag, state) = operation.vm.ftvm_interface.do_send_cmd_to_target_host_vm(vmuuid, cmd, 5, target_ip)
            if not flag:
                return (False, state)
        
        # e.g update vmconf runningimg info
        vhdinfo = copy.deepcopy(vhd)
        if (firstsnp or (vhd["runningimg"].split("/")[-1] == vhd["baseimg"].split("/")[-1]) or (vhd["runningimg"] == vhd["baseimg"])):
            #对物理硬盘适用
            vhdinfo["youarehere_parent_id"] = nowdesc
        else:
            #对物理硬盘适用
            vhdinfo["youarehere_parent_id"] = vhd["runningimg"].split("_")[0]
      
        vhdinfo['runningimg'] = crnowdesc + "_" + vhd["aliasimg"]
            
        vm_op = operation.vm.conf_xml.vm_operation(vmuuid, storage_path)
        (flag, state) = vm_op.set_vmhd_t(vhdinfo)
        if not flag:
            return (False, state)
                
        return True,''
        
    def revert_snapshot(self):

        storage_path = self.storage_path
        vmuuid = self.vmuuid
        vhd = self.vhd
        
        targetsnapshotid = self.targetsnapshotid
        crnowdesc= self.crnowdesc
        snxml_file = self.snxml_file
        snxml_flag = self.snxml_flag

        tdev = "%s/%s/%s_%s" % (storage_path, vmuuid, targetsnapshotid, vhd["aliasimg"])
            
        if  (tdev and not os.access(tdev, os.F_OK)):
            # e.g 如果虚拟硬盘无目标快照，则不进行任何操作。等重启加载或不加载该硬盘。
            return True,''
            
        os.system("rm -rf %s_%s" % (crnowdesc, vhd["aliasimg"]))
        cmd = "cd %s/%s && fvmm-vdisk " % (storage_path, vmuuid) + " create -f fronfs -o backing_file=%s_%s %s_%s && cd -" % (targetsnapshotid, vhd["aliasimg"], crnowdesc, vhd["aliasimg"])
        (cmdstat, rparams) = support.cmd_exe.cmd_exe(cmd)

        if not cmdstat:
            return (False, rparams)
        
        if os.access(snxml_file, os.F_OK) and snxml_flag:
            
            vhdinfo = {"baseimg":vhd["baseimg"], "runningimg":crnowdesc + "_" + vhd["aliasimg"]}
            
            snxml_op = operation.vm.conf_xml.vm_operation(vmuuid, storage_path, targetsnapshotid + ".xml")
            (flag, state) = snxml_op.set_vmhd_t(vhdinfo)
            if not flag:
                return (False, "snxml config failed")
            return True,''

        # 旧版本vServer创建的快照，不保护对应快照节点的xml文件。
        # e.g update running vhd
        vhdinfo = copy.deepcopy(vhd)
        
        vhdinfo["runningimg"] = crnowdesc + "_" + vhd["aliasimg"]
        
        vhdinfo["youarehere_parent_id"] = targetsnapshotid
        vm_op = operation.vm.conf_xml.vm_operation(vmuuid, storage_path)
        (flag, state) = vm_op.set_vmhd_t(vhdinfo)
        if not flag:
            return (False, state)
        
        return True,''
       
    def changeimg416_1(self):
        
        storage_path = self.storage_path
        vmuuid = self.vmuuid
        vhd = self.vhd
                
        if vhd.get("persistence") == "no":
            return True,''
        # e.g 
        if  vhd["runningimg"] == vhd["baseimg"]:
            
            os.system("rm -rf tmp_%s" % vhd["aliasimg"])
            cmd = "cd %s/%s && fvmm-vdisk" % (storage_path, vmuuid) + " create -f fronfs -o backing_file=%s tmp_%s && cd -" % (vhd["baseimg"], vhd["aliasimg"])
            (cmdstat, rparams) = support.cmd_exe.cmd_exe(cmd)

            if not cmdstat:
                return (False, rparams)
            # e.g change img
            cmd = "changeimg %s %s/%s/tmp_%s\n" % (vhd["baseimg"], storage_path, vmuuid, vhd["aliasimg"])
            (ret, strs) = operation.vm.vm_running_script.monitor_exec(vmuuid, cmd, 30)
            if not ret:
                return (False, strs)
            
        else:
            os.system("rm -rf tmp_%s" % vhd["aliasimg"])
            cmd = "cd %s/%s && fvmm-vdisk" % (storage_path, vmuuid) + " create -f fronfs -o backing_file=%s tmp_%s && cd -" % (vhd["runningimg"], vhd["aliasimg"])
            (cmdstat, rparams) = support.cmd_exe.cmd_exe(cmd)

            if not cmdstat:
                return (False, rparams)
            # e.g change img
            cmd = "changeimg %s/%s/%s %s/%s/tmp_%s\n" % (storage_path, vmuuid, vhd["runningimg"], storage_path, vmuuid, vhd["aliasimg"])
            (ret, strs) = operation.vm.vm_running_script.monitor_exec(vmuuid, cmd, 30)
            if not ret:
                return (False, strs)
                
        return True,''
    
    def changeimg416_2(self):
        vhd = self.vhd
        storage_path = self.storage_path
        vmuuid = self.vmuuid

        
        if vhd.get("persistence") == "no":
            return True,''
        
        cmd = execomd.vmcomd["fvmm-vdisk"]+" commit %s/%s/tmp_%s" % (storage_path, vmuuid, vhd["aliasimg"])
        (cmdstat, rparams) = support.cmd_exe.cmd_exe(cmd)
        if not cmdstat:
            return (False, rparams)
            # e.g change img
                
        if vhd["runningimg"] == vhd["baseimg"]:
            cmd = "changeimg %s/%s/tmp_%s %s\n" % (storage_path, vmuuid, vhd["aliasimg"], vhd["baseimg"])
            (ret, strs) = operation.vm.vm_running_script.monitor_exec(vmuuid, cmd, 30)
            if not ret:
                return (False, strs)
            
        else:
            cmd = "changeimg %s/%s/tmp_%s %s/%s/%s\n" % (storage_path, vmuuid, vhd["aliasimg"], storage_path, vmuuid, vhd["runningimg"])
            (ret, strs) = operation.vm.vm_running_script.monitor_exec(vmuuid, cmd, 30)
            if not ret:
                return (False, strs)
        cmd = "rm -rf %s/%s/tmp_%s" % (storage_path, vmuuid, vhd["aliasimg"])
        os.system(cmd)
                    
        return True,''

    def delete_snapshot(self):
        
        vhd = self.vhd
        storage_path = self.storage_path
        vmuuid = self.vmuuid
        targetsnapshotid = self.targetsnapshotid
        childsnaplist = self.childsnaplist
        reopen_img = self.reopen_img
        parents_idlist = self.parents_idlist
        copy_level = self.copy_level
        
        event = self.fparam['event']
        i = self.fparam['i']
        d = self.fparam['d']
        
        tdev = "%s/%s/%s_%s" % (storage_path, vmuuid, targetsnapshotid, vhd["aliasimg"])
            
        if (tdev and not os.access(tdev, os.F_OK)):
            # e.g 该虚拟硬盘不包含将要删除的快照信息.
            return True,''
        if childsnaplist:

            #前面检测过tdev
            (flag, stat) = support.fileutil.zcp_option.file_to_file("%s/%s/%s_%s" % (storage_path, vmuuid, targetsnapshotid, vhd["aliasimg"]), "%s/%s/tmp_%s_%s" % (storage_path, vmuuid, targetsnapshotid, vhd["aliasimg"]),copy_level)
            if not flag:
                return (False, stat)

            if reopen_img:
                # 如果虚拟机处于运行状态，且将删除的快照在当前img的父分支节点，则进行以下处理
                # 保证第一个进行commit处理的为当前running的img的父分支节点的快照子节点
                for xx in childsnaplist:
                    if xx["id"] in parents_idlist:
                        childsnaplist.remove(xx)
                        childsnaplist.insert(0, xx)
                        break

            for x in range(0, len(childsnaplist)):

                if not os.access("%s/%s/%s_%s" % (storage_path, vmuuid, targetsnapshotid, vhd["aliasimg"]), os.F_OK):
                    #xxxxxx
                    if x == (len(childsnaplist) - 1):
                        if not os.access("%s/%s/tmp_%s_%s"%(storage_path, vmuuid, targetsnapshotid, vhd["aliasimg"]),os.F_OK):
                            continue
                        cmd = "mv -f %s/%s/tmp_%s_%s %s/%s/%s_%s" % (storage_path, vmuuid, targetsnapshotid, vhd["aliasimg"], storage_path, vmuuid, targetsnapshotid, vhd["aliasimg"])
                        (cmdstat, rparams) = support.cmd_exe.cmd_exe(cmd)
                        if not cmdstat:
                            return (False, rparams)
                    else:
                        # 复制的目的文件名字发生了变更，此时，必须以目的名字为“进度监控源”
                        src_list = []
                        src_list.append("%s/%s/%s_%s" % (storage_path, vmuuid, targetsnapshotid, vhd["aliasimg"]))
                        src_list.append("%s/%s/tmp_%s_%s" % (storage_path, vmuuid, targetsnapshotid, vhd["aliasimg"]))
                        operation.vm.progressbar_copy.any_to_any(src_list, "%s/%s" % (storage_path, vmuuid), event, 1 + 90 * x/len(childsnaplist), 1 + 90 * (x + 1)/len(childsnaplist))
                        
                        # e.g change to zcp
                        (flag, stat) = support.fileutil.zcp_option.file_to_file("%s/%s/tmp_%s_%s" % (storage_path, vmuuid, targetsnapshotid, vhd["aliasimg"]), "%s/%s/%s_%s" % (storage_path, vmuuid, targetsnapshotid, vhd["aliasimg"]),copy_level)
                        if not flag:
                            return (False, stat)

                childsnap = childsnaplist[x]
 
                cmd = execomd.vmcomd["fvmm-vdisk"]+" commit %s/%s/%s_%s" % (storage_path, vmuuid, childsnap["id"], vhd["aliasimg"])
                
                flag,stat = operation.vm.cmd_util.get_cmd_process(event,cmd,i*d+0.1,(i+1)*d+0.1)
                i = i + 1
                #(cmdstat, rparams) = support.cmd_exe.cmd_exe(cmd)
                if not flag:
                    return (False, str(stat))
                
                if reopen_img and childsnap["id"] in parents_idlist:
                    # 处理当前runningimg的父级分支的时候，需要特殊处理
                    cmd = "stop\n"
                    (ret, _) = operation.vm.vm_running_script.monitor_exec(vmuuid, cmd, 2)
                    if (not ret):
                        return (False, "suspend vm failed")
                
                cmd = "mv %s/%s/%s_%s %s/%s/tmp_will_be_delete_%s_%s"  % (storage_path, vmuuid, childsnap["id"], vhd["aliasimg"], storage_path, vmuuid, childsnap["id"], vhd["aliasimg"])
                (cmdstat, rparams) = support.cmd_exe.cmd_exe(cmd)
                if not cmdstat:
                    return (False, rparams)

                cmd = "mv -f %s/%s/%s_%s %s/%s/%s_%s" % (storage_path, vmuuid, targetsnapshotid, vhd["aliasimg"], storage_path, vmuuid, childsnap["id"], vhd["aliasimg"])
                (cmdstat, rparams) = support.cmd_exe.cmd_exe(cmd)
                if not cmdstat:
                    return (False, rparams)
                
                if reopen_img and childsnap["id"] in parents_idlist:
                    # 处理当前runningimg的父级分支的时候，需要特殊处理
                    #物理硬盘的diff文件同样适用
                    running_img_fn = "%s/%s/%s" % (storage_path, vmuuid, vhd["runningimg"])
                    cmd = "changeimg %s %s" % (running_img_fn, running_img_fn)
                    (ret, strs) = operation.vm.vm_running_script.monitor_exec(vmuuid, cmd, 30)
                    if not ret:
                        return (False, strs)

                    cmd = "cont\n"
                    (ret, _) = operation.vm.vm_running_script.monitor_exec(vmuuid, cmd, 2)
                    if (not ret):
                        return (False, "suspend vm failed")
                
                cmd = "rm -rf %s/%s/tmp_will_be_delete_%s_%s"  % (storage_path, vmuuid, childsnap["id"], vhd["aliasimg"])
                (cmdstat, rparams) = support.cmd_exe.cmd_exe(cmd)
                if not cmdstat:
                    return (False, rparams) 
                
                #当只有一个child时，不会删除tmp_targetid_img.之前是mv，所以不会出现。改为zcp后，不会执行label xxxxxx 处。增加rm -f 命令。
                if os.access('%s/%s/tmp_%s_%s' % (storage_path, vmuuid, targetsnapshotid, vhd["aliasimg"]), os.F_OK):
                    cmd = 'rm -f %s/%s/tmp_%s_%s' % (storage_path, vmuuid, targetsnapshotid, vhd["aliasimg"])
                    (cmdstat, rparams) = support.cmd_exe.cmd_exe(cmd)
                    if not cmdstat:
                        return (False, rparams) 
                
        else:
            cmd = "rm -rf %s/%s/%s_%s" % (storage_path, vmuuid, targetsnapshotid, vhd["aliasimg"])
            (cmdstat, rparams) = support.cmd_exe.cmd_exe(cmd)
            if not cmdstat:
                return (False, rparams)
            

        self.fparam['i'] = i
        self.fparam['event'] = event    
        return True,''
    
    
class VHDHD(VHDPHY):
    def __init__(self,*args, **kwargs):
        VHDPHY.__init__(self,*args, **kwargs)
        return
    
class VHDPT(VHDPHY):
    def __init__(self,*args, **kwargs):
        VHDPHY.__init__(self,*args, **kwargs)
        return
    

