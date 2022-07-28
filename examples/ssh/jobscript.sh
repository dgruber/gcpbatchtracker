export hostname=`hostname`
export main=`head -1 /etc/cloudbatch-taskgroup-hosts`

whoami
env
# generate public key (all nodes have the same)
ssh-keygen -y -f ~/.ssh/id_cloud_batch > ~/.ssh/id_cloud_batch.pub
cat ~/.ssh/id_cloud_batch.pub >> /root/.ssh/authorized_keys

export SSHD_PORT=2022
sed -i "s/#Port 22/Port ${SSHD_PORT}/" /etc/ssh/sshd_config

echo "sshd config"
cat /etc/ssh/sshd_config

# account is locked which leads to ssh problems
sed -i s/root:!/"root:*"/g /etc/shadow

echo "ssh config"
cat /etc/ssh/ssh_config

# unsupported option
sed -i "s/GSSAPIAuthentication yes/#GSSAPIAuthentication no/" /etc/ssh/ssh_config
sed -i "s/#UseDNS no/#UseDNS yes/" /etc/ssh/ssh_config
# unsupported option
sed -i "s/UsePAM yes/#UsePAM no/" /etc/ssh/sshd_config

if [[ "x${main}" == "x${hostname}" ]]; then 
   echo "Hello SSH launcher: ${hostname}"
   sleep 30
   for host in `tail -n+2 /etc/cloudbatch-taskgroup-hosts`; do
    while true
    do
      ssh -i ~/.ssh/id_cloud_batch -p ${SSHD_PORT} root@${host} "touch /tmp/shutdown"
      EXITCODE=$?
      if [[ ${EXITCODE} == 0 ]]; then
        echo "SSH worked"
        break
      else
        echo "SSH failed with exit code ${EXITCODE}"
        sleep 10
        echo "retry"
      fi
    done
  done
else
   echo "Hello SSH worker: ${hostname}"
   /usr/sbin/sshd -D -p 2022 -e &
   until [ -f /tmp/shutdown ]
   do
     sleep 10
   done
   echo "Shutdown worker ${hostname}"
fi
