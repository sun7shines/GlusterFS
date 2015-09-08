# -*- coding: utf-8 -*-

import os

import support.cmd_exe
import copy

import operation.gluster.volume_db
import operation.vm.vm_running_script
import operation.vstorage.disk_cmd_op
import execomd
import support.uuid_op

from operation.vobjects.vhd import VHD



class VHDDEV(VHD):
    def __init__(self,*args, **kwargs):
        VHD.__init__(self,*args, **kwargs)
        self.sync_flag = True
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
            
            # e.g first create snapshot
            (flag, state) = operation.vstorage.disk_cmd_op.active_hard_vhd_device(vhd)
            if not flag:
                return (False, state)

            vt_size = operation.vm.qemu_vhdinfo.get_vhd_vsize(vhd["baseimg"])
            if "unknown" == vt_size:
                return (False, "vm virtual device failed")
            lv_name = nowdesc + "_" + vhd["baseimg"].split("/")[-1]
            vg_name = vhd["baseimg"].split("/")[-2]
            snp_vhd_file = "/dev/%s/%s" % (vg_name, lv_name)
            (flag, state) = operation.vstorage.disk_cmd_op.create_lv(int(vt_size)/1024, lv_name, vg_name)
            if not flag:
                return (False, state)

            cmd = "fvmm-vdisk create -f qcow2 -b %s %s" % (vhd["baseimg"], snp_vhd_file)
            (cmdstat, rparams) = support.cmd_exe.cmd_exe(cmd)
            if not cmdstat:
                return (False, rparams)

            (flag, state) = operation.vstorage.disk_cmd_op.active_hard_vhd_device(vhd)
            if not flag:
                return (False, state)

            vt_size = operation.vm.qemu_vhdinfo.get_vhd_vsize(vhd["baseimg"])
            if "unknown" == vt_size:
                return (False, "vm virtual device failed")
            lv_base_name = nowdesc + "_" + vhd["baseimg"].split("/")[-1]
            lv_name = crnowdesc + "_" + vhd["baseimg"].split("/")[-1]
            vg_name = vhd["baseimg"].split("/")[-2]
            snp_vhd_file = "/dev/%s/%s" % (vg_name, lv_name)
            snp_vhd_bs_file = "/dev/%s/%s" % (vg_name, lv_base_name)
            (flag, state) = operation.vstorage.disk_cmd_op.create_lv(int(vt_size)/1024, lv_name, vg_name)
            if not flag:
                return (False, state)

            cmd = "fvmm-vdisk create -f qcow2 -b %s %s" % (snp_vhd_bs_file, snp_vhd_file)
            (cmdstat, rparams) = support.cmd_exe.cmd_exe(cmd)
            if not cmdstat:
                return (False, rparams)
        else:
            
            if (youarehere_id != vhd["runningimg"].split("/")[-1].split("_")[0]):
                # e.g 如果某个硬盘在子节点上有快照，在当前节点无快照。则不允许在此时创建快照。
                return True,''

            (flag, state) = operation.vstorage.disk_cmd_op.active_hard_vhd_device(vhd)
            if not flag:
                return (False, state)

            vt_size = operation.vm.qemu_vhdinfo.get_vhd_vsize(vhd["baseimg"])
            if "unknown" == vt_size:
                return (False, "vm virtual device failed")
            lv_name = crnowdesc + "_" + vhd["baseimg"].split("/")[-1]
            vg_name = vhd["baseimg"].split("/")[-2]
            snp_vhd_file = "/dev/%s/%s" % (vg_name, lv_name)
            (flag, state) = operation.vstorage.disk_cmd_op.create_lv(int(vt_size)/1024, lv_name, vg_name)
            if not flag:
                return (False, state)

            cmd = "fvmm-vdisk create -f qcow2 -b %s %s" % (vhd["runningimg"], snp_vhd_file)
            (cmdstat, rparams) = support.cmd_exe.cmd_exe(cmd)
            if not cmdstat:
                return (False, rparams)
                 
        if vm_is_running:
            
            lv_name = crnowdesc + "_" + vhd["baseimg"].split("/")[-1]
            vg_name = vhd["baseimg"].split("/")[-2]
            snp_vhd_file = "/dev/%s/%s" % (vg_name, lv_name)
            #cmd = "changeimg %s %s\n" % (vhd["runningimg"], snp_vhd_file)
            cmd = operation.gluster.volume_db.changeimg_cmd(vhd["runningimg"],snp_vhd_file)
            
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
            
            vhdinfo["youarehere_parent_id"] = vhd["runningimg"].split("/")[-1].split("_")[0]
            
        
        lv_name = crnowdesc + "_" + vhd["baseimg"].split("/")[-1]
        vg_name = vhd["baseimg"].split("/")[-2]
        snp_vhd_file = "/dev/%s/%s" % (vg_name, lv_name)
        vhdinfo["runningimg"] = snp_vhd_file

        vm_op = operation.vm.conf_xml.vm_operation(vmuuid, storage_path)
        (flag, state) = vm_op.set_vmhd_t(vhdinfo)
        if not flag:
            return (False, state)
        
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
            if vhd.get("use_pattern") and vhd.get("use_pattern") in ["lv","format_full_hd","format_free_hd","vg_free_space"]:
                vg_name = vhd["baseimg"].split("/")[-2]
                lv_name = vhd["baseimg"].split("/")[-1]

                vt_size = operation.vm.qemu_vhdinfo.get_vhd_vsize(vhd["baseimg"])
                if "unknown" == vt_size:
                    return (False, "vm virtual device failed")

                (flag, state) = operation.vstorage.disk_cmd_op.create_lv(int(vt_size)/1024, "tmp_%s" % lv_name, vg_name)
                if not flag:
                    return (False, state)

                cmd = "fvmm-vdisk convert -O qcow2 -b %s /dev/%s/tmp_%s" % (tdev, vg_name, lv_name)
                flag,stat = operation.vm.cmd_util.get_cmd_process(event,cmd,i*d+0.1,(i+1)*d+0.1)
                i = i + 1
                #(cmdstat, rparams) = support.cmd_exe.cmd_exe(cmd)
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
                if x == 0:
                    pass
                else:
                    vg_name = vhd["baseimg"].split("/")[-2]
                    lv_name = vhd["baseimg"].split("/")[-1]
                    cmd = "fvmm-vdisk convert -O qcow2 -b /dev/%s/tmp_%s %s" % (vg_name, lv_name, tdev)
                    flag,stat = operation.vm.cmd_util.get_cmd_process(event,cmd,i*d+0.1,(i+1)*d+0.1)
                    i = i + 1
                    #(cmdstat, rparams) = support.cmd_exe.cmd_exe(cmd)
                    if not flag:
                        return (False, str(stat))

                childsnap = childsnaplist[x]

                vg_name = vhd["baseimg"].split("/")[-2]
                lv_name = vhd["baseimg"].split("/")[-1]
                cmd = execomd.vmcomd["fvmm-vdisk"]+" commit /dev/%s/%s_%s" % (vg_name, childsnap["id"], lv_name)
                
                flag,stat = operation.vm.cmd_util.get_cmd_process(event,cmd,i*d+0.1,(i+1)*d+0.1)
                i = i + 1
                if not flag:
                    return (False, str(stat))

                vg_name = vhd["baseimg"].split("/")[-2]
                lv_name = vhd["baseimg"].split("/")[-1]
                
                #从tdev 复制为 childsnap
                cmd = "fvmm-vdisk convert -O qcow2 -b %s /dev/%s/%s_%s" % (tdev, vg_name, childsnap["id"], lv_name)
                #(cmdstat, rparams) = support.cmd_exe.cmd_exe(cmd)
                flag,stat = operation.vm.cmd_util.get_cmd_process(event,cmd,i*d+0.1,(i+1)*d+0.1)
                i = i + 1
                if not flag:
                    return (False, str(stat))

                (flag, base_dev) = operation.vm.qemu_vhdinfo.getvhdfile(tdev)
                if not flag:
                    return (False, base_dev)

                cmd = "fvmm-vdisk rebase -f qcow2 -u -b %s -F qcow2 /dev/%s/%s_%s" % (base_dev, vg_name, childsnap["id"], lv_name)
                (cmdstat, rparams) = support.cmd_exe.cmd_exe(cmd)
                if not cmdstat:
                    return (False, rparams)

            if len(childsnaplist) >= 1:
                vg_name = vhd["baseimg"].split("/")[-2]
                lv_name = vhd["baseimg"].split("/")[-1]
                cmd = "lvremove -f /dev/%s/tmp_%s" % (vg_name, lv_name)
                (cmdstat, rparams) = support.cmd_exe.cmd_exe(cmd)
                if not cmdstat:
                    return (False, rparams)
                
            cmd = "lvremove -f %s" % tdev
            (cmdstat, rparams) = support.cmd_exe.cmd_exe(cmd)
            if not cmdstat:
                return (False, rparams)
    
        else:
            cmd = "lvremove -f %s" % tdev
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
        
    
        vg_name = vhd["baseimg"].split("/")[-2]
        lv_name = support.uuid_op.get_uuid()
        new_dev = "/dev/%s/%s" % (vg_name, lv_name)
        nvhdsinfo.append({"baseimg":vhd["baseimg"], "newvhduuid":new_dev})

        (flag, state) = operation.vstorage.disk_cmd_op.active_hard_vhd_device(vhd)
        if not flag:
            return (False, state)
        vt_size = operation.vm.qemu_vhdinfo.get_vhd_vsize(vhd["baseimg"])
        if "unknown" == vt_size:
            return (False, "vm virtual device failed")
        (flag, state) = operation.vstorage.disk_cmd_op.create_lv(int(vt_size)/1024, lv_name, vg_name)
        if not flag:
            return (False, state)

        cmd = "fvmm-vdisk create -f qcow2 -b %s %s" % (vhd["runningimg"], new_dev)
        (cmdstat, rparams) = support.cmd_exe.cmd_exe(cmd)
        if not cmdstat:
            return (False, rparams)
            
        return True,''

    def revert_snapshot(self):

        vhd = self.vhd
        
        storage_path = self.storage_path
        vmuuid = self.vmuuid
        
        targetsnapshotid = self.targetsnapshotid
        crnowdesc= self.crnowdesc
        snxml_file = self.snxml_file
        snxml_flag = self.snxml_flag

        tdev = "/dev/%s/%s_%s" % (vhd["baseimg"].split("/")[-2], targetsnapshotid, vhd["baseimg"].split("/")[-1])
        if  (tdev and not os.access(tdev, os.F_OK)):
            # e.g 如果虚拟硬盘无目标快照，则不进行任何操作。等重启加载或不加载该硬盘。
            return True,''

        (flag, state) = operation.vstorage.disk_cmd_op.active_hard_vhd_device(vhd)
        if not flag:
            return (False, state)

        vg_name = vhd["baseimg"].split("/")[-2]
        lv_name = vhd["baseimg"].split("/")[-1]

        vt_size = operation.vm.qemu_vhdinfo.get_vhd_vsize(vhd["baseimg"])
        if "unknown" == vt_size:
            return (False, "vm virtual device failed")

        (flag, state) = operation.vstorage.disk_cmd_op.create_lv(int(vt_size)/1024, "%s_%s" % (crnowdesc, lv_name), vg_name)
        if not flag:
            return (False, state)

        cmd = "fvmm-vdisk create -f qcow2 -b /dev/%s/%s_%s /dev/%s/%s_%s" % (vg_name, targetsnapshotid, lv_name, vg_name, crnowdesc, lv_name)
        (cmdstat, rparams) = support.cmd_exe.cmd_exe(cmd)
        if not cmdstat:
            return (False, rparams)
        
        if os.access(snxml_file, os.F_OK) and snxml_flag:
            vhdinfo = {"vhd_type":vhd.get('vhd_type'),"baseimg":vhd["baseimg"], "runningimg":"/dev/"+ vhd["baseimg"].split("/")[-2] + "/"+ crnowdesc + "_" + vhd["baseimg"].split("/")[-1]}
            snxml_op = operation.vm.conf_xml.vm_operation(vmuuid, storage_path, targetsnapshotid + ".xml")
            (flag, state) = snxml_op.set_vmhd_t(vhdinfo)
            if not flag:
                return (False, "snxml config failed")
            return True,''

        # 旧版本vServer创建的快照，不保护对应快照节点的xml文件。
        # e.g update running vhd
        vhdinfo = copy.deepcopy(vhd)
        vhdinfo["runningimg"] = "/dev/"+ vhd["baseimg"].split("/")[-2] + "/"+ crnowdesc + "_" + vhd["baseimg"].split("/")[-1]
        vhdinfo["youarehere_parent_id"] = targetsnapshotid
        vm_op = operation.vm.conf_xml.vm_operation(vmuuid, storage_path)
        (flag, state) = vm_op.set_vmhd_t(vhdinfo)
        if not flag:
            return (False, state)
        
        return True,''
    
    def changeimg504_1(self):
                
        return True,''
    
    def changeimg504_2(self):
        
        return True,''
        

class VHDLV(VHDDEV):
    def __init__(self,*args, **kwargs):
        VHDDEV.__init__(self,*args, **kwargs)
        return
    
class VHDFORMAT_FULL_HD(VHDDEV):
    def __init__(self,*args, **kwargs):
        VHDDEV.__init__(self,*args, **kwargs)
        return

class VHDFORMAT_FREE_HD(VHDDEV):
    def __init__(self,*args, **kwargs):
        VHDDEV.__init__(self,*args, **kwargs)
        return
    
class VHDVG_FREE_SPACE(VHDDEV):
    def __init__(self,*args, **kwargs):
        VHDDEV.__init__(self,*args, **kwargs)
        return


