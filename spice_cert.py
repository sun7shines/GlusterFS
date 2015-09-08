# -*- coding: utf-8 -*-

import os
import sys
import syslog



pwd = "f2a0n0w8a0r6e"

cert_dir = "/etc/pki/libvirt-spice"
CA_CERT = cert_dir + "/" + "ca-cert.pem"
CA_KEY = cert_dir + "/" + "ca-key.pem"

SERVER_CERT = cert_dir + "/" + "server-cert.pem"
SERVER_KEY = cert_dir + "/" + "server-key.pem"
SERVER_CSR = cert_dir + "/" + "server-key.csr"
SERVER_SCU = cert_dir + "/" + "server-key.pem.secure"
SERVER_ISCU = cert_dir + '/' + "server-key.pem.insecure"

src_dir = "/usr/vmd"
SRC_CA_CERT = src_dir + '/' + "ca-cert.pem"
SRC_CA_KEY = src_dir + '/' + "ca-key.pem"

def create_spice_certificate():

    if os.path.exists(CA_CERT) and os.path.exists(CA_KEY):
        return True

    cmd = "rm -f %s %s" % (CA_CERT,CA_KEY)
    os.system(cmd)

    PASSWD = str(pwd) 
    if not os.path.exists(CA_KEY):
        cmd = "openssl genrsa -des3 -passout pass:%s -out %s 1024" % (PASSWD,CA_KEY)
        cmd = cmd + ' > /dev/null'
        print os.system(cmd)
    
    if not os.path.exists(CA_CERT):
        cmd = "openssl req -new -x509 -days 1095 -key %s -out %s -subj '/C=CN/L=Beijing/O=Fronware/CN=my CA' -passin pass:%s" % (CA_KEY,CA_CERT,PASSWD)

        cmd = cmd + ' > /dev/null'
        print os.system(cmd)

def create_srv_pem():
 
    PASSWD = str(pwd)

    if os.path.exists(SERVER_CERT) and os.path.exists(SERVER_KEY) \
and os.path.exists(SERVER_CSR) and os.path.exists(SERVER_SCU):
        return True
   
    if not os.path.exists(CA_CERT) or not os.path.exists(CA_KEY):
        return False 
     
    if not os.path.exists(SERVER_KEY):
        cmd = "openssl genrsa -out %s 1024" % (SERVER_KEY)
        cmd = cmd + ' > /dev/null'
        print os.system(cmd)

    if not os.path.exists(SERVER_CSR):
        cmd = "openssl req -new -key %s -out %s -subj '/C=CN/L=Beijing/O=Fronware/CN=my server'" % (SERVER_KEY,SERVER_CSR)       
        cmd = cmd + ' > /dev/null' 
        print os.system(cmd)

    if not os.path.exists(SERVER_CERT):
        cmd = "openssl x509 -req -days 1095 -in %s -CA %s -CAkey %s -set_serial 01 -out %s -passin pass:%s" % (SERVER_CSR,CA_CERT,CA_KEY,SERVER_CERT,PASSWD)
        cmd = cmd + ' > /dev/null'
        print os.system(cmd)

    cmd = "openssl rsa -in %s -out %s" % (SERVER_KEY,SERVER_ISCU) 
    cmd = cmd + ' > /dev/null'
    print os.system(cmd)

    cmd = "mv %s %s" % (SERVER_KEY,SERVER_SCU) 
    cmd = cmd + ' > /dev/null'
    print os.system(cmd)

    cmd = "mv %s %s" % (SERVER_ISCU,SERVER_KEY) 
    cmd = cmd + ' > /dev/null'
    print os.system(cmd)
 
    cmd = "openssl rsa -noout -text -in %s" % (SERVER_KEY)
    cmd = cmd + ' > /dev/null'
    print os.system(cmd)

    cmd = "openssl rsa -passin pass:%s -noout -text -in %s" % (PASSWD,CA_KEY)
    cmd = cmd + ' > /dev/null'
    print os.system(cmd)

    cmd = "openssl req -noout -text -in %s" % (SERVER_CSR)
    cmd = cmd + ' > /dev/null'
    print os.system(cmd)

    cmd = "openssl x509 -noout -text -in %s" % (SERVER_CERT)
    cmd = cmd + ' > /dev/null'
    print os.system(cmd)

    cmd = "openssl x509 -noout -text -in %s" % (CA_CERT)
    cmd = cmd + ' > /dev/null'
    print os.system(cmd)

    cmd = "openssl x509 -noout -text -in %s" % (SERVER_CERT)
    cmd = cmd + ' > /dev/null'
    print os.system(cmd)
    return True

def init_spice_certificate():

    if not os.path.exists(SRC_CA_CERT) or not os.path.exists(SRC_CA_KEY):
        syslog.syslog(syslog.LOG_ERR,'init spice certificate failed: server ca files not exists')        
        return False

    if not os.path.exists(CA_CERT) or not os.path.exists(CA_KEY):
        if not os.path.exists(cert_dir):
            cmd = 'mkdir -p %s' % (cert_dir)
            os.system(cmd)

        cmd = "rm -f %s %s" % (CA_CERT,CA_KEY)
        os.system(cmd)
  
        cmd = 'cp %s %s' % (SRC_CA_CERT,CA_CERT)
        os.system(cmd)

        cmd = 'cp %s %s' % (SRC_CA_KEY,CA_KEY)
        os.system(cmd)

    if not os.path.exists(SERVER_CERT) or not os.path.exists(SERVER_KEY) \
or not os.path.exists(SERVER_CSR) or not os.path.exists(SERVER_SCU):
        create_srv_pem() 
    
if __name__ == "__main__":
  
    arg = sys.argv[1]     
    if arg == 'ca':
        create_spice_certificate() 
    if arg == 'srv':
        create_srv_pem()




