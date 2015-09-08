# -*- coding: utf-8 -*-

import os
import syslog
import support.cmd_exe
import copy

import operation.gluster.volume_db
import operation.vm.vm_running_script
import operation.vstorage.disk_cmd_op
import execomd
import support.fileutil.zcp_option
import support.uuid_op

class VHD():
    
    def __init__(self,*args, **kwargs):
        self.vhd = kwargs.get('vhd')
        
        self.fparam = None
        
        self.vmuuid = None
        self.storage_path = None
        
        #create_snapshot
        self.nowdesc = None
        self.crnowdesc = None
        self.firstsnp = None
        self.youarehere_parent_id = None
        self.youarehere_id = None
        self.target_ip = None
        self.vm_is_running = None
            
        #delete_snapshot
        self.targetsnapshotid = None
        self.childsnaplist = None
        self.reopen_img = None
        self.parents_idlist = None
        self.copy_level = None
    
        #rw_diff
        self.new_uuid = None
        
        #vm_migrate_storage
        self.target_storage_path = None
        self.sync_flag = False
        
        return
    
    def create_snapshot(self):
        return True,''
    
class VHDIMG(VHD):
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
            if not firstsnp:
                if (not vhd.get("curr_snap")) or (vhd.get("curr_snap") != str(youarehere_parent_id)):# 不一致
                    return True,''
            
            if (not vhd.get("use_pattern")) and (vhd["runningimg"].split("/")[-1] != vhd["baseimg"].split("/")[-1]):
                return (False, "vm vhd stat failed")
            # e.g first create snapshot
            if ("/mnt/" in vhd["baseimg"]):
                # e.g
                cfile = vhd["baseimg"]
                if not os.access(cfile, os.F_OK):
                    state = "Create diff img failed, File not exist: " + cfile + " vmuuid: " + vmuuid
                    syslog.syslog(syslog.LOG_ERR, state)
                    return (False, state)

                os.system("rm -rf %s_%s" % (nowdesc, vhd["baseimg"].split("/")[-1]))
                cmd = "cd %s/%s && fvmm-vdisk " % (storage_path, vmuuid) + " create -f fronfs -o backing_file=%s %s_%s && cd -" % (vhd["baseimg"], nowdesc, vhd["baseimg"].split("/")[-1])
                (cmdstat, rparams) = support.cmd_exe.cmd_exe(cmd)

                if not cmdstat:
                    return (False, rparams)

            else:
                # e.g relative dir
                cfile = storage_path + "/" + vmuuid + "/" + vhd["baseimg"].split("/")[-1]
                if not os.access(cfile, os.F_OK):
                    state = "Create diff img failed, File not exist: " + cfile + " vmuuid: " + vmuuid
                    syslog.syslog(syslog.LOG_ERR, state)
                    return (False, state)

                os.system("rm -rf %s_%s" % (nowdesc, vhd["baseimg"].split("/")[-1]))
                cmd = "cd %s/%s && fvmm-vdisk " % (storage_path, vmuuid) + " create -f fronfs -o backing_file=%s %s_%s && cd -" % (vhd["baseimg"].split("/")[-1], nowdesc, vhd["baseimg"].split("/")[-1])
                (cmdstat, rparams) = support.cmd_exe.cmd_exe(cmd)

                if not cmdstat:
                    return (False, rparams)

            cfile = storage_path + "/" + vmuuid + "/" + nowdesc + "_" + vhd["baseimg"].split("/")[-1]
            if not os.access(cfile, os.F_OK):
                state = "Create diff img failed, File not exist: " + cfile + " vmuuid: " + vmuuid + " storage_path: " + storage_path
                syslog.syslog(syslog.LOG_ERR, state)
                return (False, state)

            os.system("rm -rf %s_%s" % (crnowdesc, vhd["baseimg"].split("/")[-1]))
            cmd = "cd %s/%s && fvmm-vdisk " % (storage_path, vmuuid) + " create -f fronfs -o backing_file=%s_%s %s_%s && cd -" % (nowdesc, vhd["baseimg"].split("/")[-1], crnowdesc, vhd["baseimg"].split("/")[-1])
            (cmdstat, rparams) = support.cmd_exe.cmd_exe(cmd)

            if not cmdstat:
                return (False, rparams)

        else:
            
            if (youarehere_id != vhd["runningimg"].split("/")[-1].split("_")[0]):
                # e.g 如果某个硬盘在子节点上有快照，在当前节点无快照。则不允许在此时创建快照。
                return True,''

            # e.g not first create snapshot
            # e.g relative dir
            cfile = storage_path + "/" + vmuuid + "/" + vhd["runningimg"]
            if not os.access(cfile, os.F_OK):
                state = "Create diff img failed, File not exist: " + cfile + " vmuuid: " + vmuuid + " storage_path: " + storage_path
                syslog.syslog(syslog.LOG_ERR, state)
                return (False, state)

            os.system("rm -rf %s_%s" % (crnowdesc, vhd["baseimg"].split("/")[-1]))
            cmd = "cd %s/%s && fvmm-vdisk " % (storage_path, vmuuid) + " create -f fronfs -o backing_file=%s %s_%s && cd -" % (vhd["runningimg"], crnowdesc, vhd["baseimg"].split("/")[-1])
            (cmdstat, rparams) = support.cmd_exe.cmd_exe(cmd)

            if not cmdstat:
                return (False, rparams)
           
        if vm_is_running:
            
            if (firstsnp or (vhd["runningimg"] == vhd["baseimg"].split("/")[-1])) and ("/mnt/" in vhd["baseimg"]):
                # e.g 第一次创建快照 或 某个硬盘第一次创建快照.
                cmd = operation.gluster.volume_db.changeimg_cmd("%s" % (vhd["baseimg"]), 
                                                                "%s/%s/%s_%s" % (storage_path, vmuuid, crnowdesc, vhd["baseimg"].split("/")[-1]))
            else:
                cmd = operation.gluster.volume_db.changeimg_cmd("%s/%s/%s" % (storage_path, vmuuid, vhd["runningimg"]), 
                                                                "%s/%s/%s_%s" % (storage_path, vmuuid, crnowdesc, vhd["baseimg"].split("/")[-1]))
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
            vhdinfo["youarehere_parent_id"] = nowdesc
        else:
            vhdinfo["youarehere_parent_id"] = vhd["runningimg"].split("_")[0]
        
        vhdinfo["runningimg"] = crnowdesc + "_" + vhd["baseimg"].split("/")[-1]
        
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
        if ("/mnt/" in vhd["baseimg"]) and (vhd["runningimg"] == vhd["baseimg"].split("/")[-1]):

            os.system("rm -rf tmp_%s" % vhd["baseimg"].split("/")[-1])
            cmd = "cd %s/%s && fvmm-vdisk " % (storage_path, vmuuid) + " create -f fronfs -o backing_file=%s tmp_%s && cd -" % (vhd["baseimg"], vhd["baseimg"].split("/")[-1])
            (cmdstat, rparams) = support.cmd_exe.cmd_exe(cmd)

            if not cmdstat:
                return (False, rparams)
            # e.g change img
            cmd = operation.gluster.volume_db.changeimg_cmd(vhd["baseimg"],
                                                            "%s/%s/tmp_%s" % (storage_path, vmuuid, vhd["baseimg"].split("/")[-1]))
            (ret, strs) = operation.vm.vm_running_script.monitor_exec(vmuuid, cmd, 30)
            if not ret:
                return (False, strs)
        else:

            os.system("rm -rf tmp_%s" % vhd["baseimg"].split("/")[-1])
            cmd = "cd %s/%s && fvmm-vdisk " % (storage_path, vmuuid) + " create -f fronfs -o backing_file=%s tmp_%s && cd -" % (vhd["runningimg"], vhd["baseimg"].split("/")[-1])
            (cmdstat, rparams) = support.cmd_exe.cmd_exe(cmd)

            if not cmdstat:
                return (False, rparams)
            # e.g change img
            cmd = operation.gluster.volume_db.changeimg_cmd("%s/%s/%s" % (storage_path, vmuuid, vhd["runningimg"]),
                                                            "%s/%s/tmp_%s" % (storage_path, vmuuid, vhd["baseimg"].split("/")[-1]))
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
        
        tdev = ""
        if vhd["baseimg"].startswith("/dev/"):
            tdev = "/dev/%s/%s_%s" % (vhd["baseimg"].split("/")[-2], targetsnapshotid, vhd["baseimg"].split("/")[-1])
            (flag, state) = operation.vstorage.disk_cmd_op.active_hard_vhd_device(vhd)
            if not flag:
                return (False, state)

        if (not os.access("%s/%s/%s_%s" % (storage_path, vmuuid, targetsnapshotid, vhd["baseimg"].split("/")[-1]), os.F_OK)) and \
            (tdev and not os.access(tdev, os.F_OK)):
            # e.g 该虚拟硬盘不包含将要删除的快照信息.
            return True,''
        if childsnaplist:
            
            if not os.access("%s/%s/%s_%s"%(storage_path, vmuuid, targetsnapshotid, vhd["baseimg"].split("/")[-1]),os.F_OK):
                return True,''
#                 cmd = "mv -f %s/%s/%s_%s %s/%s/tmp_%s_%s" % (storage_path, vmuuid, targetsnapshotid, vhd["baseimg"].split("/")[-1], storage_path, vmuuid, targetsnapshotid, vhd["baseimg"].split("/")[-1])
#                 (cmdstat, rparams) = support.cmd_exe.cmd_exe(cmd)
#                 if not cmdstat:
#                     return (False, rparams)
            # e.g 临时img之所以不用mv，是为了避免running的虚拟机的父节点使用的时候读取错误
            (flag, stat) = support.fileutil.zcp_option.file_to_file("%s/%s/%s_%s" % (storage_path, vmuuid, targetsnapshotid, vhd["baseimg"].split("/")[-1]), "%s/%s/tmp_%s_%s" % (storage_path, vmuuid, targetsnapshotid, vhd["baseimg"].split("/")[-1]),copy_level)
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
                if not os.access("%s/%s/%s_%s" % (storage_path, vmuuid, targetsnapshotid, vhd["baseimg"].split("/")[-1]), os.F_OK):
                    if x == (len(childsnaplist) - 1):
                        if not os.access("%s/%s/tmp_%s_%s"%(storage_path, vmuuid, targetsnapshotid, vhd["baseimg"].split("/")[-1]),os.F_OK):
                            continue
                        cmd = "mv -f %s/%s/tmp_%s_%s %s/%s/%s_%s" % (storage_path, vmuuid, targetsnapshotid, vhd["baseimg"].split("/")[-1], storage_path, vmuuid, targetsnapshotid, vhd["baseimg"].split("/")[-1])
                        (cmdstat, rparams) = support.cmd_exe.cmd_exe(cmd)
                        if not cmdstat:
                            return (False, rparams)
                    else:
                        
                        # 复制的目的文件名字发生了变更，此时，必须以目的名字为“进度监控源”
                        src_list = []
                        src_list.append("%s/%s/%s_%s" % (storage_path, vmuuid, targetsnapshotid, vhd["baseimg"].split("/")[-1]))
                        src_list.append("%s/%s/tmp_%s_%s" % (storage_path, vmuuid, targetsnapshotid, vhd["baseimg"].split("/")[-1]))
                        operation.vm.progressbar_copy.any_to_any(src_list, "%s/%s" % (storage_path, vmuuid), event, 1 + 90 * x/len(childsnaplist), 1 + 90 * (x + 1)/len(childsnaplist))
                        
                        # e.g change to zcp
                        (flag, stat) = support.fileutil.zcp_option.file_to_file("%s/%s/tmp_%s_%s" % (storage_path, vmuuid, targetsnapshotid, vhd["baseimg"].split("/")[-1]), "%s/%s/%s_%s" % (storage_path, vmuuid, targetsnapshotid, vhd["baseimg"].split("/")[-1]),copy_level)
                        if not flag:
                            return (False, stat)

                childsnap = childsnaplist[x]
                cmd = execomd.vmcomd["fvmm-vdisk"]+" commit %s/%s/%s_%s" % (storage_path, vmuuid, childsnap["id"], vhd["baseimg"].split("/")[-1])
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
                
                cmd = "mv %s/%s/%s_%s %s/%s/tmp_will_be_delete_%s_%s"  % (storage_path, vmuuid, childsnap["id"], vhd["baseimg"].split("/")[-1], storage_path, vmuuid, childsnap["id"], vhd["baseimg"].split("/")[-1])
                (cmdstat, rparams) = support.cmd_exe.cmd_exe(cmd)
                if not cmdstat:
                    return (False, rparams)

                cmd = "mv -f %s/%s/%s_%s %s/%s/%s_%s" % (storage_path, vmuuid, targetsnapshotid, vhd["baseimg"].split("/")[-1], storage_path, vmuuid, childsnap["id"], vhd["baseimg"].split("/")[-1])
                (cmdstat, rparams) = support.cmd_exe.cmd_exe(cmd)
                if not cmdstat:
                    return (False, rparams)
                
                if reopen_img and childsnap["id"] in parents_idlist:
                    # 处理当前runningimg的父级分支的时候，需要特殊处理
                    running_img_fn = "%s/%s/%s" % (storage_path, vmuuid, vhd["runningimg"])
                    cmd = "changeimg %s %s" % (running_img_fn, running_img_fn)
                    (ret, strs) = operation.vm.vm_running_script.monitor_exec(vmuuid, cmd, 30)
                    if not ret:
                        return (False, strs)

                    cmd = "cont\n"
                    (ret, _) = operation.vm.vm_running_script.monitor_exec(vmuuid, cmd, 2)
                    if (not ret):
                        return (False, "suspend vm failed")
                
                cmd = "rm -rf %s/%s/tmp_will_be_delete_%s_%s"  % (storage_path, vmuuid, childsnap["id"], vhd["baseimg"].split("/")[-1])
                (cmdstat, rparams) = support.cmd_exe.cmd_exe(cmd)
                if not cmdstat:
                    return (False, rparams)
                
            if os.access('%s/%s/tmp_%s_%s' % (storage_path, vmuuid, targetsnapshotid, vhd["baseimg"].split("/")[-1]), os.F_OK):
                cmd = 'rm -f %s/%s/tmp_%s_%s' % (storage_path, vmuuid, targetsnapshotid, vhd["baseimg"].split("/")[-1])
                (cmdstat, rparams) = support.cmd_exe.cmd_exe(cmd)
                if not cmdstat:
                    return (False, rparams) 
    
        else:
            cmd = "rm -rf %s/%s/%s_%s" % (storage_path, vmuuid, targetsnapshotid, vhd["baseimg"].split("/")[-1])
            (cmdstat, rparams) = support.cmd_exe.cmd_exe(cmd)
            if not cmdstat:
                return (False, rparams)

        self.fparam['i'] = i
        self.fparam['event'] = event    
        return True,''
    
    def create_diff_img(self):
        
        vhd = self.vhd
        storage_path = self.storage_path
        vmuuid = self.vmuuid
        new_uuid = self.new_uuid
        nvhdsinfo = self.fparam.get('nvhdsinfo')
        
        vhdbaseimg = vhd["baseimg"]
        if "/mnt/" not in vhd["baseimg"] and not vhd.get("use_pattern"):
            vhdbaseimg = storage_path + "/" + vmuuid + "/" + vhd["baseimg"]

        newvhduuid = support.uuid_op.get_uuid() + ".img"

        nvhdsinfo.append({"baseimg":vhdbaseimg, "newvhduuid":newvhduuid})

        currvhdfile = vhdbaseimg
        currrunningimg = vhd["runningimg"]
        if currrunningimg != vhdbaseimg.split("/")[-1]:
            currvhdfile = storage_path + "/" + vmuuid + "/" + currrunningimg
        targetvhdfile = storage_path + "/" + new_uuid + "/" + newvhduuid
        if storage_path+"/"+vmuuid+"/"+currrunningimg == currvhdfile:
            # e.g 源虚拟硬盘在当前存储目录，且在虚拟机文件夹目录下。支持相对路径diffclone。
            cmd = "cd %s/%s && fvmm-vdisk " % (storage_path, new_uuid) + \
                " create -f fronfs -o backing_file=../%s/%s %s && cd -" % \
                (vmuuid, currrunningimg, newvhduuid)
            tgvhd = newvhduuid
        elif storage_path+"/"+currrunningimg == currvhdfile:
            # e.g 源虚拟硬盘在当前存储目录的根目录。支持相对路径diffclone。
            cmd = "cd %s/%s && fvmm-vdisk " % (storage_path, new_uuid) + \
                " create -f fronfs -o backing_file=../%s %s && cd -" % \
                (currrunningimg, newvhduuid)
            tgvhd = newvhduuid
        else:
            # e.g 源虚拟硬盘在当前存储目录的非根目录，非虚拟机子目录的其他目录。或源虚拟硬盘不在当前存储目录。
            cmd = "cd %s/%s && fvmm-vdisk " % (storage_path, new_uuid) + \
                " create -f fronfs -o backing_file=%s %s && cd -" % \
                (currvhdfile, targetvhdfile)
            tgvhd = targetvhdfile

        os.system("rm -rf %s" % tgvhd)
        cmdstat, _ = support.cmd_exe.cmd_exe(cmd)

        if not cmdstat:
            return (False, "Create diff vhd failed")
        
        self.fparam['nvhdsinfo'] = nvhdsinfo 
        return True,''
    
    def revert_snapshot(self):

        storage_path = self.storage_path
        vmuuid = self.vmuuid
        vhd = self.vhd
        
        targetsnapshotid = self.targetsnapshotid
        crnowdesc= self.crnowdesc
        snxml_file = self.snxml_file
        snxml_flag = self.snxml_flag
        
        if (not os.access(storage_path + "/" + vmuuid + "/" + targetsnapshotid + "_" + vhd["baseimg"].split("/")[-1], os.F_OK)):
            # e.g 如果虚拟硬盘无目标快照，则不进行任何操作。等重启加载或不加载该硬盘。
            return True,''

        
        os.system("rm -rf %s_%s" % (crnowdesc, vhd["baseimg"].split("/")[-1]))
        cmd = "cd %s/%s && fvmm-vdisk " % (storage_path, vmuuid) + " create -f fronfs -o backing_file=%s_%s %s_%s && cd -" % (targetsnapshotid, vhd["baseimg"].split("/")[-1], crnowdesc, vhd["baseimg"].split("/")[-1])
        (cmdstat, rparams) = support.cmd_exe.cmd_exe(cmd)

        if not cmdstat:
            return (False, "fvmm-vdisk fail")
        
        if os.access(snxml_file, os.F_OK) and snxml_flag:
            vhdinfo = {"baseimg":vhd["baseimg"], "runningimg":crnowdesc + "_" + vhd["baseimg"].split("/")[-1],"vhd_type":vhd["vhd_type"]}
            if vhd["vhd_type"] == "SCSI:Virtio Para-Virtual":
                vhdinfo["pci_addr"] = vhd.get("pci_addr")
            snxml_op = operation.vm.conf_xml.vm_operation(vmuuid, storage_path, targetsnapshotid + ".xml")
            (flag, state) = snxml_op.set_vmhd_t(vhdinfo)
            if not flag:
                return (False, "snxml config failed")
            return True,''

        # 旧版本vServer创建的快照，不保护对应快照节点的xml文件。
        # e.g update running vhd
        vhdinfo = copy.deepcopy(vhd)
        vhdinfo["runningimg"] = crnowdesc + "_" + vhd["baseimg"].split("/")[-1]
        vhdinfo["youarehere_parent_id"] = targetsnapshotid
        vm_op = operation.vm.conf_xml.vm_operation(vmuuid, storage_path)
        (flag, state) = vm_op.set_vmhd_t(vhdinfo)
        if not flag:
            return (False, state)
        
        return True,''
    
    def commit_tmp_data(self):
        vhd = self.vhd
        target_storage_path = self.target_storage_path
        vmuuid = self.vmuuid
        
        if vhd.get("persistence") == "no":
            return True,''
        
        # REBASE 虚拟机 临时IMG。
        cmd =  execomd.vmcomd["fvmm-vdisk"]+" rebase -f fronfs -u -b %s/%s/%s -F fronfs %s/%s/tmp_%s" % (target_storage_path, vmuuid, vhd["runningimg"], target_storage_path, vmuuid, vhd["baseimg"].split("/")[-1])
        (cmdstat, rparams) = support.cmd_exe.cmd_exe(cmd)
        if not cmdstat:
            return (False, rparams)
        
        # commit 虚拟机 临时IMG。
        cmd = execomd.vmcomd["fvmm-vdisk"]+" commit %s/%s/tmp_%s" % (target_storage_path, vmuuid, vhd["baseimg"].split("/")[-1])
        (cmdstat, rparams) = support.cmd_exe.cmd_exe(cmd)
        if not cmdstat:
            return (False, rparams)

        # 切换到目标存储IMG运行。
        #cmd = "changeimg %s/%s/tmp_%s %s/%s/%s\n" % (target_storage_path, vmuuid, vhd["baseimg"].split("/")[-1], target_storage_path, vmuuid, vhd["runningimg"])
        cmd = operation.gluster.volume_db.changeimg_cmd("%s/%s/tmp_%s" % (target_storage_path, vmuuid, vhd["baseimg"].split("/")[-1]), 
                                                        "%s/%s/%s" % (target_storage_path, vmuuid, vhd["runningimg"]))
        (ret, strs) = operation.vm.vm_running_script.monitor_exec(vmuuid, cmd, 30)
        if not ret:
            return (False, strs)
        
        cmd = "rm -rf %s/%s/tmp_%s" % (target_storage_path, vmuuid, vhd["runningimg"])
        os.system(cmd)
        
        return True,''
    def clear_error(self):
        vhd = self.vhd
        target_storage_path = self.target_storage_path
        vmuuid = self.vmuuid
        storage_path = self.storage_path
        
        if vhd.get("persistence") == "no":
            return True,''
        
        cmd = execomd.vmcomd["fvmm-vdisk"]+" commit %s/%s/tmp_%s" % (target_storage_path, vmuuid, vhd["baseimg"].split("/")[-1])
        (cmdstat, rparams) = support.cmd_exe.cmd_exe(cmd)
        if not cmdstat:
            return (False, rparams)
        
        if ("/mnt/" in vhd["baseimg"]) and (vhd["runningimg"] == vhd["baseimg"].split("/")[-1]):
            # e.g change img
            #cmd = "changeimg %s/%s/tmp_%s %s\n" % (target_storage_path, vmuuid, vhd["baseimg"].split("/")[-1], vhd["baseimg"])
            cmd = operation.gluster.volume_db.changeimg_cmd("%s/%s/tmp_%s" % (target_storage_path, vmuuid, vhd["baseimg"].split("/")[-1]),
                                                            "%s" % (vhd["baseimg"]))
            (ret, strs) = operation.vm.vm_running_script.monitor_exec(vmuuid, cmd, 30)
            if not ret:
                return (False, strs)
        else:
            # e.g change img
            #cmd = "changeimg %s/%s/tmp_%s %s/%s/%s\n" % (target_storage_path, vmuuid, vhd["baseimg"].split("/")[-1], storage_path, vmuuid, vhd["runningimg"])
            cmd = operation.gluster.volume_db.changeimg_cmd("%s/%s/tmp_%s" % (target_storage_path, vmuuid, vhd["baseimg"].split("/")[-1]),
                                                            "%s/%s/%s" % (storage_path, vmuuid, vhd["runningimg"]))
            (ret, strs) = operation.vm.vm_running_script.monitor_exec(vmuuid, cmd, 30)
            if not ret:
                return (False, strs)
                    
        return True,''
    
    def prev_dealwith_runningvm(self):

        vhd = self.vhd
        storage_path = self.storage_path
        vmuuid = self.vmuuid
        
        target_storage_path = self.target_storage_path
        
        if vhd.get("persistence") == "no":
            return True,''
        
        # 删除可能残留的临时文件
        cmd = "rm -rf %s/%s/tmp_%s" % (storage_path, vmuuid, vhd["baseimg"].split("/")[-1])
        os.system(cmd)
        cmd = "rm -rf %s/%s/tmp_%s" % (target_storage_path, vmuuid, vhd["baseimg"].split("/")[-1])
        os.system(cmd)
        
        if ("/mnt/" in vhd["baseimg"]) and (vhd["runningimg"] == vhd["baseimg"].split("/")[-1]):
            cmd = execomd.vmcomd["fvmm-vdisk"]+" create -f fronfs -o backing_file=%s %s/%s/tmp_%s" % (vhd["baseimg"], target_storage_path, vmuuid, vhd["baseimg"].split("/")[-1])
            (cmdstat, rparams) = support.cmd_exe.cmd_exe(cmd)
            if not cmdstat:
                return (False, rparams)
            # e.g change img
            #cmd = "changeimg %s %s/%s/tmp_%s\n" % (vhd["baseimg"], target_storage_path, vmuuid, vhd["baseimg"].split("/")[-1])
            cmd = operation.gluster.volume_db.changeimg_cmd("%s" % (vhd["baseimg"]),
                                                            "%s/%s/tmp_%s" % (target_storage_path, vmuuid, vhd["baseimg"].split("/")[-1]))
            (ret, strs) = operation.vm.vm_running_script.monitor_exec(vmuuid, cmd, 30)
            if not ret:
                return (False, strs)
        else:
            cmd = execomd.vmcomd["fvmm-vdisk"]+" create -f fronfs -o backing_file=%s/%s/%s %s/%s/tmp_%s" % (storage_path, vmuuid, vhd["runningimg"], target_storage_path, vmuuid, vhd["baseimg"].split("/")[-1])
            (cmdstat, rparams) = support.cmd_exe.cmd_exe(cmd)
            if not cmdstat:
                return (False, rparams)
            # e.g change img
            #cmd = "changeimg %s/%s/%s %s/%s/tmp_%s\n" % (storage_path, vmuuid, vhd["runningimg"], target_storage_path, vmuuid, vhd["baseimg"].split("/")[-1])
            cmd = operation.gluster.volume_db.changeimg_cmd("%s/%s/%s" % (storage_path, vmuuid, vhd["runningimg"]),
                                                            "%s/%s/tmp_%s" % (target_storage_path, vmuuid, vhd["baseimg"].split("/")[-1]))
            (ret,strs) = operation.vm.vm_running_script.monitor_exec(vmuuid, cmd, 30)
            if not ret:
                return (False, strs)
            
        return True,''
    
    def changeimg504_1(self):
        vhd = self.vhd
        bsstorage_path = self.bsstorage_path
        vmuuid = self.vmuuid
        
        if vhd.get("persistence") == "no":
            return True,''
        # e.g
        if ("/mnt/" in vhd["baseimg"]) and (vhd["runningimg"] == vhd["baseimg"].split("/")[-1]):

            os.system("rm -rf tmp_%s" % vhd["baseimg"].split("/")[-1])
            cmd = "cd %s/%s && fvmm-vdisk " % (bsstorage_path, vmuuid) + " create -f fronfs -o backing_file=%s tmp_%s && cd -" % (vhd["baseimg"], vhd["baseimg"].split("/")[-1])
            (cmdstat, rparams) = support.cmd_exe.cmd_exe(cmd)

            if not cmdstat:
                return (False, rparams)
            # e.g change img
            #cmd = "changeimg %s %s/%s/tmp_%s\n" % (vhd["baseimg"], bsstorage_path, vmuuid, vhd["baseimg"].split("/")[-1])
            cmd = operation.gluster.volume_db.changeimg_cmd("%s" % (vhd["baseimg"]),
                                                            "%s/%s/tmp_%s" % (bsstorage_path, vmuuid, vhd["baseimg"].split("/")[-1]))
            (ret, strs) = operation.vm.vm_running_script.monitor_exec(vmuuid, cmd, 30)
            if not ret:
                return (False, strs)
        else:

            os.system("rm -rf tmp_%s" % vhd["baseimg"].split("/")[-1])
            cmd = "cd %s/%s && fvmm-vdisk " % (bsstorage_path, vmuuid) + " create -f fronfs -o backing_file=%s tmp_%s && cd -" % (vhd["runningimg"], vhd["baseimg"].split("/")[-1])
            (cmdstat, rparams) = support.cmd_exe.cmd_exe(cmd)

            if not cmdstat:
                return (False, rparams)
            # e.g change img
            #cmd = "changeimg %s/%s/%s %s/%s/tmp_%s\n" % (bsstorage_path, vmuuid, vhd["runningimg"], bsstorage_path, vmuuid, vhd["baseimg"].split("/")[-1])
            cmd = operation.gluster.volume_db.changeimg_cmd("%s/%s/%s" % (bsstorage_path, vmuuid, vhd["runningimg"]),
                                                            "%s/%s/tmp_%s" % (bsstorage_path, vmuuid, vhd["baseimg"].split("/")[-1]))
            (ret, strs) = operation.vm.vm_running_script.monitor_exec(vmuuid, cmd, 30)
            if not ret:
                return (False, strs)
                    
        return True,''
    
    def changeimg504_2(self):
        
        vhd = self.vhd
        bsstorage_path = self.bsstorage_path
        vmuuid =self.vmuuid
        storage_path = self.storage_path
        new_uuid = self.new_uuid
        
        
        if vhd.get("persistence") == "no":
            return True,''
        
        cmd = execomd.vmcomd["fvmm-vdisk"]+" commit %s/%s/tmp_%s" % (bsstorage_path, vmuuid, vhd["baseimg"].split("/")[-1])
        (cmdstat, rparams) =support.cmd_exe.cmd_exe(cmd)
        if not cmdstat:
            return (False, rparams)
            # e.g change img
        if ("/mnt/" in vhd["baseimg"]) and (vhd["runningimg"] == vhd["baseimg"].split("/")[-1]):
            #cmd = "changeimg %s/%s/tmp_%s %s\n" % (bsstorage_path, vmuuid, vhd["baseimg"].split("/")[-1], vhd["baseimg"])
            cmd = operation.gluster.volume_db.changeimg_cmd("%s/%s/tmp_%s" % (bsstorage_path, vmuuid, vhd["baseimg"].split("/")[-1]),
                                                            "%s" % (vhd["baseimg"]))
            (ret, strs) = operation.vm.vm_running_script.monitor_exec(vmuuid, cmd, 30)
            if not ret:
                return (False, strs)
        else:
            # e.g change img
            #cmd = "changeimg %s/%s/tmp_%s %s/%s/%s\n" % (bsstorage_path, vmuuid, vhd["baseimg"].split("/")[-1], bsstorage_path, vmuuid, vhd["runningimg"])
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
                   
        return True,''
    
